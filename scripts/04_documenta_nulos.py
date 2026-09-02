"""
Etapa PySpark (documentação, não altera Silver) — Dicionário de nulos.

Decisão do projeto: os valores nulos NÃO são preenchidos no Silver. Na
pesquisa State of Data Brazil, a maior parte dos nulos é estrutural
(lógica condicional do formulário — "skip logic"), não dado faltante:

  - Colunas binárias de multi-select (0/1): NaN significa que a
    pergunta-pai nem foi exibida para o respondente (não "não
    selecionou"). Ex: perguntas só respondidas por quem é gestor.
  - Colunas categóricas de escolha única: o % de nulo indica o quão
    condicional a pergunta é ao perfil do respondente. Perguntas
    universais (idade, gênero, nível de ensino) têm ~0% de nulo;
    perguntas condicionais (cargo como gestor, tempo buscando
    oportunidade) têm nulo alto porque só se aplicam a um subconjunto.

Este script não trata/preenche nulo nenhum — gera um DICIONÁRIO
documentando, para cada coluna de cada edição:
  - tipo: "binaria_multiselect" ou "categorica_escolha_unica"
  - pct_nulo: percentual de linhas nulas
  - classificacao: heurística de leitura do nulo, para orientar quem for
    escrever consultas na camada Gold (ex: calcular "% de gestores que
    fazem X" sobre a subpopulação elegível, não sobre a base toda)

O cálculo de nulo/tipo é feito em UMA agregação Spark por edição (uma
única leitura da base, não uma consulta por coluna) — como o resultado é
pequeno (uma linha por coluna, não por respondente), o dicionário final é
coletado para o driver e gravado como um único CSV "achatado" (mais fácil
de abrir e revisar numa planilha do que um diretório Spark particionado).

Classificação (heurística, baseada só no % de nulo — não substitui
revisão manual do questionário original quando houver dúvida):
  - "quase_universal"      (pct_nulo < 10%):  nulo = não-resposta genuína
  - "condicional_ao_perfil" (pct_nulo >= 10%): nulo = pergunta não aplicável
                                                 a esse respondente

Uso:
    python scripts/04_documenta_nulos.py
"""

import pandas as pd
from pyspark.sql import functions as F

from _config_aws import EDICOES, caminho_documentacao, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import col_seguro, cria_spark_session

DIRETORIOS_SILVER = {edicao: caminho_silver_staging_por_edicao(edicao) for edicao in EDICOES}
# Escrita via pandas (arquivo pequeno, editado à mão) — para gravar direto
# no S3 é preciso ter o pacote `s3fs` instalado (ver requirements.txt).
ARQUIVO_SAIDA = caminho_documentacao("dicionario_nulos.csv")

LIMIAR_QUASE_UNIVERSAL = 10.0  # % de nulo abaixo do qual consideramos "não-resposta genuína"
VALORES_BINARIOS_VALIDOS = {"0", "1"}


def classifica_semantica_nulo(tipo: str, pct_nulo: float) -> str:
    if tipo == "binaria_multiselect":
        return "grupo_nao_exibido_ao_respondente"
    return "quase_universal" if pct_nulo < LIMIAR_QUASE_UNIVERSAL else "condicional_ao_perfil"


def calcula_estatisticas_edicao(spark, edicao: str, diretorio: str) -> pd.DataFrame:
    print(f"Lendo Silver {edicao}: {diretorio}")
    df = spark.read.option("header", True).csv(str(diretorio))
    colunas = df.columns
    total_linhas = df.count()

    # Uma única passada: conta nulos por coluna E coleta até 200 valores
    # distintos por coluna (para decidir se é binária), tudo numa linha só.
    # Alias posicional (evita qualquer problema com caractere especial
    # remanescente no nome da coluna virando alias de agregação).
    apelidos = {f"col_{i}": c for i, c in enumerate(colunas)}
    agregacoes = []
    for apelido, c in apelidos.items():
        agregacoes.append(F.sum(col_seguro(c).isNull().cast("long")).alias(f"nulos__{apelido}"))
        agregacoes.append(F.slice(F.collect_set(col_seguro(c)), 1, 200).alias(f"valores__{apelido}"))
    linha = df.agg(*agregacoes).first()

    registros = []
    for apelido, c in apelidos.items():
        n_nulos = linha[f"nulos__{apelido}"]
        pct_nulo = round((n_nulos / total_linhas) * 100, 1) if total_linhas else 0.0
        valores = set(linha[f"valores__{apelido}"]) if linha[f"valores__{apelido}"] else set()
        tipo = "binaria_multiselect" if valores and valores.issubset(VALORES_BINARIOS_VALIDOS) else "categorica_escolha_unica"
        registros.append(
            {
                "edicao": edicao,
                "coluna": c,
                "tipo": tipo,
                "pct_nulo": pct_nulo,
                "classificacao_nulo": classifica_semantica_nulo(tipo, pct_nulo),
            }
        )

    return pd.DataFrame(registros)


def main() -> None:
    spark = cria_spark_session("documenta_nulos")

    blocos = [calcula_estatisticas_edicao(spark, edicao, diretorio) for edicao, diretorio in DIRETORIOS_SILVER.items()]
    dicionario = pd.concat(blocos, ignore_index=True).sort_values(["edicao", "pct_nulo"], ascending=[True, False])

    dicionario.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8")

    print(f"\nTotal de linhas no dicionário: {len(dicionario)}")
    print(dicionario["classificacao_nulo"].value_counts())
    print(f"\nDicionário gravado em: {ARQUIVO_SAIDA}")

    spark.stop()


if __name__ == "__main__":
    main()
