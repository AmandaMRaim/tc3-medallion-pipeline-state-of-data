"""
Etapa PySpark (Silver "por edição" -> Silver "state_of_data_silver") —
Harmoniza o schema das 3 edições (usando o dicionário do script 05) e
grava cada uma na sua partição da tabela catalogada
db_state_of_data.state_of_data_silver, depois cria/atualiza a tabela e
as 3 partições no Glue Data Catalog via boto3 (idempotente).

Política de confiança — só usa uma correspondência entre edições quando
método é "exato", ou "fuzzy" com confiança "alta" (score >= 0.85), ou foi
`aprovado_manual` na revisão. O resto fica de fora por enquanto (coluna
existe no schema final, mas NULA pra edição não validada) — mais seguro
que arriscar juntar dado errado.

Coluna de partição não é escrita dentro do arquivo (convenção Hive/Athena
— valor vem do caminho "particao=<valor>/").
"""

import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

from _config_aws import (
    DATABASE,
    EDICOES,
    TABELA_STATE_OF_DATA,
    caminho_base_silver_state_of_data,
    caminho_documentacao,
    caminho_silver_staging_por_edicao,
    caminho_silver_state_of_data,
    cataloga_tabela_particionada,
)
from _lib_padroniza_colunas import col_seguro, cria_spark_session

ARQUIVO_DICIONARIO = caminho_documentacao("dicionario_correspondencia_colunas.csv")
DIRETORIOS_SILVER_STAGING = {edicao: caminho_silver_staging_por_edicao(edicao) for edicao in EDICOES}


def confiavel(metodo, confianca, status_revisao) -> bool:
    if status_revisao == "rejeitado_manual":
        return False
    if status_revisao == "aprovado_manual":
        return True
    if metodo == "exato":
        return True
    if metodo == "fuzzy" and confianca == "alta":
        return True
    return False


def monta_mapa_colunas(dicionario: pd.DataFrame):
    """Retorna, para cada edição, um dict {nome_coluna_canonico: nome_coluna_na_edicao}."""
    mapa = {edicao: {} for edicao in EDICOES}

    for _, linha in dicionario.iterrows():
        col_2024_2025 = linha["coluna_2024_2025"]
        col_2023_2024 = linha["coluna_2023_2024"]
        col_2025_2026 = linha["coluna_2025_2026"]

        if pd.notna(col_2024_2025):
            canonico = col_2024_2025
            mapa["2024-2025"][canonico] = col_2024_2025

            if confiavel(linha["metodo_2023_2024"], linha["confianca_2023_2024"], linha.get("status_revisao_2023_2024")):
                if pd.notna(col_2023_2024):
                    mapa["2023-2024"][canonico] = col_2023_2024

            if confiavel(linha["metodo_2025_2026"], linha["confianca_2025_2026"], None):
                if pd.notna(col_2025_2026):
                    mapa["2025-2026"][canonico] = col_2025_2026

        elif pd.notna(col_2023_2024):
            canonico = col_2023_2024
            mapa["2023-2024"][canonico] = col_2023_2024
        elif pd.notna(col_2025_2026):
            canonico = col_2025_2026
            mapa["2025-2026"][canonico] = col_2025_2026

    return mapa


def main() -> None:
    spark = cria_spark_session("monta_silver_state_of_data")

    print(f"Lendo dicionário: {ARQUIVO_DICIONARIO}")
    dicionario = pd.read_csv(ARQUIVO_DICIONARIO)

    mapa = monta_mapa_colunas(dicionario)
    colunas_canonicas = sorted(set().union(*[set(mapa[e]) for e in EDICOES]))
    print(f"Total de colunas no schema harmonizado: {len(colunas_canonicas)}")

    for edicao in EDICOES:
        diretorio = DIRETORIOS_SILVER_STAGING[edicao]
        print(f"\nLendo Silver (staging) {edicao}: {diretorio}")
        df = spark.read.option("header", True).csv(diretorio)

        n_preenchidas = 0
        selecoes = [F.monotonically_increasing_id().alias("linha_origem_silver")]
        for canonico in colunas_canonicas:
            col_original = mapa[edicao].get(canonico)
            if col_original is not None and col_original in df.columns:
                selecoes.append(col_seguro(col_original).alias(canonico))
                n_preenchidas += 1
            else:
                selecoes.append(F.lit(None).cast(StringType()).alias(canonico))

        bloco = df.select(*selecoes)
        destino = caminho_silver_state_of_data(edicao)
        bloco.coalesce(1).write.mode("overwrite").option("header", True).option("encoding", "UTF-8").csv(destino)

        print(f"  {edicao}: {n_preenchidas}/{len(colunas_canonicas)} colunas preenchidas, {bloco.count()} linhas")
        print(f"  Partição gravada em: {destino}")

    # Ordem tem que bater com a do CSV (OpenCSVSerde casa por posição).
    colunas_arquivo = ["linha_origem_silver"] + colunas_canonicas
    print(f"\nCatalogando {DATABASE}.{TABELA_STATE_OF_DATA} no Glue Data Catalog...")
    cataloga_tabela_particionada(
        database=DATABASE,
        tabela=TABELA_STATE_OF_DATA,
        colunas=colunas_arquivo,
        particoes=EDICOES,
        localizacao_base=caminho_base_silver_state_of_data(),
    )

    spark.stop()


if __name__ == "__main__":
    main()
