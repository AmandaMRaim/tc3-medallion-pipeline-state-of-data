"""
Etapa PySpark (Silver "por edição" -> Silver "state_of_data") — Harmoniza
o schema das 3 edições e grava cada uma na sua partição da tabela
catalogada db_state_of_data.state_of_data.

Usa o dicionário de correspondência (Silver/_documentacao/
dicionario_correspondencia_colunas.csv, gerado pelo script 05 e revisado
manualmente — arquivo pequeno de metadado, lido com pandas mesmo nesta
versão PySpark, ver nota abaixo) para dar aos 3 datasets (schema próprio
de cada edição, gravado pelos scripts 01/02/03 em Silver/_por_edicao/)
o MESMO conjunto de colunas — requisito de uma tabela particionada no
Glue Data Catalog, já que todas as partições precisam compartilhar um
schema único.

Nota sobre o motor de execução: o dicionário de correspondência é uma
tabela de METADADO pequena (uma linha por coluna, não por respondente) e
é editada manualmente numa planilha durante a revisão — por isso continua
sendo lida/gravada com pandas como um único CSV "achatado", em vez de um
diretório Spark particionado (ruim pra abrir/editar à mão). Já a leitura
das 3 bases Silver e o `select`/alias por edição SÃO operações sobre dado
de respondente de verdade — isso sim roda em Spark.

Política de confiança — SÓ usa uma correspondência entre edições quando:
  - metodo == "exato" (nomes idênticos), ou
  - metodo == "fuzzy" e confianca == "alta" (score >= 0.85, texto quase
    idêntico), ou
  - status_revisao_2023_2024 == "aprovado_manual" (aprovado na revisão
    manual em chat, mesmo com confiança média/baixa)

Qualquer correspondência marcada "rejeitado_manual", ou fuzzy de
confiança média/baixa AINDA NÃO revisada, fica de fora por enquanto: a
coluna existe no schema final (para não perder o dado das edições que a
têm), mas fica NULA para a edição cuja correspondência não foi validada.

IMPORTANTE — catalogação: a tabela db_state_of_data.state_of_data e suas
3 partições JÁ EXISTEM no Glue Data Catalog. Este script só GRAVA os
arquivos no caminho S3 de cada partição — não cria/altera a tabela nem
registra partição nenhuma. A coluna de partição (NOME_COLUNA_PARTICAO em
_config_aws.py) NÃO é escrita como coluna de dado dentro do arquivo —
segue a convenção Hive/Athena, onde o valor da partição vem do caminho
(pasta), não do conteúdo do arquivo.

Uso (local, fora do Glue — exige credenciais AWS configuradas para o
Spark local enxergar o S3):
    python scripts/06_monta_silver_state_of_data.py
"""

import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

from _config_aws import EDICOES, caminho_documentacao, caminho_silver_staging_por_edicao, caminho_silver_state_of_data
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

        # Nome canônico: prioriza 2024-2025 (edição-espinha-dorsal); usa a
        # coluna exclusiva quando a linha não tem correspondente em 2024-2025.
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
        bloco.coalesce(1).write.mode("overwrite").option("header", True).csv(destino)

        print(f"  {edicao}: {n_preenchidas}/{len(colunas_canonicas)} colunas preenchidas, {bloco.count()} linhas")
        print(f"  Partição gravada em: {destino}")

    spark.stop()


if __name__ == "__main__":
    main()
