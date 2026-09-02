"""
Etapa PySpark (documentação, não altera Silver) — Dicionário de correspondência
de colunas entre as 3 edições da pesquisa State of Data Brazil.

Nota sobre o motor de execução: a comparação em si (difflib.SequenceMatcher
sobre ~400 NOMES de coluna) é uma operação de metadado pequena e single-node
— não tem "dado de respondente" nenhum envolvido, então não há ganho real em
rodar isso em Spark. A única parte que usa Spark é a leitura da lista de
colunas de cada edição (`carrega_colunas`), já que o Silver agora é gravado
como diretório Spark (part-*.csv) pelos scripts 01/02/03.

Constatação ao comparar os headers já padronizados (Silver, ver scripts
01/02/03): 2024-2025 e 2025-2026 têm o texto das perguntas quase idêntico
(341 de ~360 colunas batem por nome exato) — o formulário mudou pouco
entre essas duas edições. Já 2023-2024 teve reformulação relevante no
texto de várias perguntas/opções (só 26 colunas batem por nome exato com
as outras duas), então a correspondência para essa edição precisa de
casamento aproximado (fuzzy match) + revisão manual.

Estratégia:
  1. Usa 2024-2025 como edição-espinha-dorsal (é a que mais se conecta
     com as outras duas).
  2. Para cada coluna de 2024-2025, procura correspondência em
     2025-2026: exata primeiro; se não achar, sugere a melhor
     aproximação (difflib) acima de um limiar de similaridade.
  3. Repete o mesmo processo para 2023-2024.
  4. Colunas de 2025-2026 ou 2023-2024 que não têm nenhuma coluna de
     2024-2025 correspondente (nem exata nem aproximada) entram como
     linhas próprias, para não perder nenhuma coluna do levantamento.

IMPORTANTE: as sugestões por fuzzy match são um RASCUNHO para revisão
manual da equipe, não uma verdade automática — o script marca claramente
o método usado em cada linha (`metodo_2023_2024` / `metodo_2025_2026`) e
uma faixa de confiança (`confianca_2023_2024` / `confianca_2025_2026`):

  - alta  (score >= 0.85): nomes praticamente idênticos, confiável
  - media (0.70 <= score < 0.85): revisar antes de usar
  - baixa (0.60 <= score < 0.70): ALTO risco de falso positivo — nomes
    longos e concatenados (pergunta-pai + opção) podem coincidir em
    pedaços de texto sem serem a mesma pergunta. Ex. encontrado nos
    dados: "banco_de_dados_dia_a_dia_sap_hana" (2024-2025) casou com
    "ferramenta_de_bi_utilizada_no_dia_a_dia_grafana" (2023-2024) só
    por compartilharem o trecho "_dia_a_dia_" — são perguntas
    completamente diferentes. NÃO usar sem revisão manual linha a linha.

Nota sobre revisão manual no S3: como esse CSV é editado à mão (coluna
`status_revisao_2023_2024`) e este script SOBRESCREVE o arquivo do zero
a cada execução, faça um backup do CSV do S3 antes de rodar de novo se
já tiver revisão feita — depois reaplique as aprovações/rejeições em
cima do arquivo novo (ver histórico do projeto para o procedimento).

Uso:
    python scripts/05_monta_dicionario_correspondencia.py
"""

from difflib import SequenceMatcher

import pandas as pd

from _config_aws import EDICOES, caminho_documentacao, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import cria_spark_session

DIRETORIOS_SILVER = {edicao: caminho_silver_staging_por_edicao(edicao) for edicao in EDICOES}
# Escrita via pandas (arquivo pequeno, editado à mão) — para gravar direto
# no S3 é preciso ter o pacote `s3fs` instalado (ver requirements.txt).
ARQUIVO_SAIDA = caminho_documentacao("dicionario_correspondencia_colunas.csv")

EDICAO_BASE = "2024-2025"
LIMIAR_SIMILARIDADE_FUZZY = 0.60  # abaixo disso, não sugere — fica "sem_correspondencia"


def carrega_colunas(spark, diretorio: str) -> list:
    return spark.read.option("header", True).csv(str(diretorio)).columns


def classifica_confianca(metodo: str, score: float) -> str:
    if metodo == "exato":
        return "alta"
    if metodo != "fuzzy":
        return None
    if score >= 0.85:
        return "alta"
    if score >= 0.70:
        return "media"
    return "baixa"


def casa_edicao(base_cols: list[str], outra_cols: list[str]):
    """Para cada coluna da base, acha correspondência (exata ou fuzzy) na outra edição.

    O passe fuzzy usa casamento guloso GLOBAL: calcula o score de todos os
    pares (coluna_pendente, candidato) e atribui em ordem decrescente de
    score, não na ordem em que as colunas aparecem no arquivo. Isso evita
    que um match fraco "roube" o candidato certo de um match mais forte
    que ainda não teve sua vez (bug encontrado na revisão manual: colunas
    de uma mesma lista longa de opções, ex. ferramentas de BI, formavam
    cadeias de matches errados por causa da ordem de iteração).

    Retorna:
      - mapa: {coluna_base: (coluna_outra_ou_None, metodo, score)}
      - usados: set de colunas de `outra_cols` já usadas em algum match
    """
    disponiveis = set(outra_cols)
    mapa = {}
    usados = set()

    # 1) passe exato
    pendentes = []
    for col in base_cols:
        if col in disponiveis:
            mapa[col] = (col, "exato", 1.0)
            usados.add(col)
        else:
            pendentes.append(col)

    # 2) calcula todos os pares candidatos (pendente, disponivel) acima do limiar
    candidatos_restantes = [c for c in outra_cols if c not in usados]
    pares = []
    for col in pendentes:
        for cand in candidatos_restantes:
            score = SequenceMatcher(None, col, cand).ratio()
            if score >= LIMIAR_SIMILARIDADE_FUZZY:
                pares.append((score, col, cand))

    # 3) atribui em ordem decrescente de score (guloso global) — cada coluna
    #    de cada lado só pode ser usada uma vez
    pares.sort(key=lambda p: p[0], reverse=True)
    pendentes_sem_match = set(pendentes)
    for score, col, cand in pares:
        if col not in pendentes_sem_match or cand in usados:
            continue
        mapa[col] = (cand, "fuzzy", round(score, 2))
        usados.add(cand)
        pendentes_sem_match.discard(col)

    # 4) quem sobrou sem nenhum candidato acima do limiar
    for col in pendentes_sem_match:
        mapa[col] = (None, "sem_correspondencia", 0.0)

    return mapa, usados


def main() -> None:
    spark = cria_spark_session("monta_dicionario_correspondencia")

    colunas = {ano: carrega_colunas(spark, diretorio) for ano, diretorio in DIRETORIOS_SILVER.items()}
    for ano, cols in colunas.items():
        print(f"{ano}: {len(cols)} colunas")

    outras_edicoes = [ano for ano in colunas if ano != EDICAO_BASE]
    base_cols = colunas[EDICAO_BASE]

    mapas = {}
    usados_por_edicao = {}
    for ano in outras_edicoes:
        mapa, usados = casa_edicao(base_cols, colunas[ano])
        mapas[ano] = mapa
        usados_por_edicao[ano] = usados
        n_exato = sum(1 for _, m, _ in mapa.values() if m == "exato")
        n_fuzzy = sum(1 for _, m, _ in mapa.values() if m == "fuzzy")
        n_sem = sum(1 for _, m, _ in mapa.values() if m == "sem_correspondencia")
        n_fuzzy_alta = sum(1 for _, m, s in mapa.values() if m == "fuzzy" and classifica_confianca(m, s) == "alta")
        n_fuzzy_media = sum(1 for _, m, s in mapa.values() if m == "fuzzy" and classifica_confianca(m, s) == "media")
        n_fuzzy_baixa = sum(1 for _, m, s in mapa.values() if m == "fuzzy" and classifica_confianca(m, s) == "baixa")
        print(f"\n{EDICAO_BASE} -> {ano}: exato={n_exato} fuzzy={n_fuzzy} sem_correspondencia={n_sem}")
        print(f"  fuzzy por confianca: alta={n_fuzzy_alta} media={n_fuzzy_media} baixa={n_fuzzy_baixa}")

    linhas = []
    for col_base in base_cols:
        linha = {"coluna_2024_2025": col_base}
        for ano in outras_edicoes:
            col_outra, metodo, score = mapas[ano][col_base]
            chave = f"coluna_{ano.replace('-', '_')}"
            linha[chave] = col_outra
            linha[f"metodo_{ano.replace('-', '_')}"] = metodo
            linha[f"score_{ano.replace('-', '_')}"] = score
            linha[f"confianca_{ano.replace('-', '_')}"] = classifica_confianca(metodo, score)
        linhas.append(linha)

    # Colunas das outras edições que não foram usadas em nenhum match (exclusivas daquela edição)
    for ano in outras_edicoes:
        nao_usadas = [c for c in colunas[ano] if c not in usados_por_edicao[ano]]
        for col in nao_usadas:
            linha = {"coluna_2024_2025": None}
            for outra in outras_edicoes:
                chave = f"coluna_{outra.replace('-', '_')}"
                linha[chave] = col if outra == ano else None
                linha[f"metodo_{outra.replace('-', '_')}"] = "exclusiva_da_edicao" if outra == ano else None
                linha[f"score_{outra.replace('-', '_')}"] = None
                linha[f"confianca_{outra.replace('-', '_')}"] = None
            linhas.append(linha)

    dicionario = pd.DataFrame(linhas)
    ARQUIVO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
    dicionario.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8")

    print(f"\nTotal de linhas no dicionário: {len(dicionario)}")
    print(f"Dicionário gravado em: {ARQUIVO_SAIDA}")

    spark.stop()


if __name__ == "__main__":
    main()
