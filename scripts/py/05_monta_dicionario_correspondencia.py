"""
Etapa (documentação, não altera Silver) — Dicionário de correspondência
de colunas entre as 3 edições da pesquisa State of Data Brazil.

2024-2025 é a espinha dorsal: casa nome exato com 2025-2026 e 2023-2024;
se não achar, sugere por similaridade de texto (difflib) acima de um
limiar, com casamento GULOSO GLOBAL (ordena todos os pares candidatos por
score antes de atribuir — evita que um match fraco "roube" o candidato
certo de um match mais forte que ainda não teve sua vez).

IMPORTANTE: fuzzy match é RASCUNHO pra revisão manual, não verdade
automática — confiança "baixa" (score 0.60-0.70) tem risco real de falso
positivo (nomes longos concatenados podem coincidir em pedaços de texto
sem ser a mesma pergunta). Este script SOBRESCREVE o CSV de saída do
zero a cada execução — faça backup antes se já tiver revisão manual feita.
"""

from difflib import SequenceMatcher

import pandas as pd

from _config_aws import EDICOES, caminho_documentacao, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import cria_spark_session

DIRETORIOS_SILVER = {edicao: caminho_silver_staging_por_edicao(edicao) for edicao in EDICOES}
ARQUIVO_SAIDA = caminho_documentacao("dicionario_correspondencia_colunas.csv")

EDICAO_BASE = "2024-2025"
LIMIAR_SIMILARIDADE_FUZZY = 0.60


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
    """Correspondência exata primeiro; depois fuzzy guloso GLOBAL (ordena
    todos os pares por score antes de atribuir, não pela ordem do arquivo).

    Retorna: mapa {coluna_base: (coluna_outra_ou_None, metodo, score)},
    usados (set de colunas de `outra_cols` já usadas).
    """
    disponiveis = set(outra_cols)
    mapa = {}
    usados = set()

    pendentes = []
    for col in base_cols:
        if col in disponiveis:
            mapa[col] = (col, "exato", 1.0)
            usados.add(col)
        else:
            pendentes.append(col)

    candidatos_restantes = [c for c in outra_cols if c not in usados]
    pares = []
    for col in pendentes:
        for cand in candidatos_restantes:
            score = SequenceMatcher(None, col, cand).ratio()
            if score >= LIMIAR_SIMILARIDADE_FUZZY:
                pares.append((score, col, cand))

    pares.sort(key=lambda p: p[0], reverse=True)
    pendentes_sem_match = set(pendentes)
    for score, col, cand in pares:
        if col not in pendentes_sem_match or cand in usados:
            continue
        mapa[col] = (cand, "fuzzy", round(score, 2))
        usados.add(cand)
        pendentes_sem_match.discard(col)

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

    # Exclusivas: colunas que não bateram em nenhum match
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
    dicionario.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8")

    print(f"\nTotal de linhas no dicionário: {len(dicionario)}")
    print(f"Dicionário gravado em: {ARQUIVO_SAIDA}")

    spark.stop()


if __name__ == "__main__":
    main()
