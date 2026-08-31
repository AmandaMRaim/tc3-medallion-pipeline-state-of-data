"""
Etapa PySpark (Silver -> Gold) — Dataset unificado das 3 edições.

Usa o dicionário de correspondência (Silver/_documentacao/
dicionario_correspondencia_colunas.csv, gerado pelo script 05 e revisado
manualmente — arquivo pequeno de metadado, lido com pandas mesmo nesta
versão PySpark, ver nota abaixo) para unir 2023-2024 + 2024-2025 +
2025-2026 em uma única tabela longa (uma linha por respondente, coluna
"edicao" identificando a origem).

Nota sobre o motor de execução: o dicionário de correspondência é uma
tabela de METADADO pequena (uma linha por coluna, não por respondente) e
é editada manualmente numa planilha durante a revisão — por isso continua
sendo lida/gravada com pandas como um único CSV "achatado", em vez de um
diretório Spark particionado (ruim pra abrir/editar à mão). Já a leitura
das 3 bases Silver, o `select`/alias por edição e a união final SÃO
operações sobre dado de respondente de verdade — isso sim roda em Spark.

Política de confiança — SÓ usa uma correspondência entre edições quando:
  - metodo == "exato" (nomes idênticos), ou
  - metodo == "fuzzy" e confianca == "alta" (score >= 0.85, texto quase
    idêntico), ou
  - status_revisao_2023_2024 == "aprovado_manual" (aprovado na revisão
    manual em chat, mesmo com confiança média/baixa)

Qualquer correspondência marcada "rejeitado_manual", ou fuzzy de
confiança média/baixa AINDA NÃO revisada, fica de fora por enquanto: a
coluna existe na Gold (para não perder o dado das edições que a têm),
mas fica NULA para a edição cuja correspondência não foi validada.

Uso (local, fora do Glue):
    python scripts/06_monta_gold_unificado.py
"""

from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from _lib_padroniza_colunas import col_seguro, cria_spark_session

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_DICIONARIO = BASE_DIR / "Silver" / "_documentacao" / "dicionario_correspondencia_colunas.csv"
DIRETORIOS_SILVER = {
    "2023-2024": BASE_DIR / "Silver" / "2023-2024" / "state-of-data-brazil-2023-2024_colunas_limpas",
    "2024-2025": BASE_DIR / "Silver" / "2024-2025" / "state-of-data-brazil-2024-2025_colunas_limpas",
    "2025-2026": BASE_DIR / "Silver" / "2025-2026" / "state-of-data-brazil-2025-2026_colunas_limpas",
}
DIRETORIO_SAIDA = BASE_DIR / "Gold" / "state_of_data_unificado"


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
    mapa = {"2023-2024": {}, "2024-2025": {}, "2025-2026": {}}

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
    spark = cria_spark_session("monta_gold_unificado")

    print(f"Lendo dicionário: {ARQUIVO_DICIONARIO}")
    dicionario = pd.read_csv(ARQUIVO_DICIONARIO)

    mapa = monta_mapa_colunas(dicionario)
    colunas_canonicas = sorted(set(mapa["2023-2024"]) | set(mapa["2024-2025"]) | set(mapa["2025-2026"]))
    print(f"Total de colunas canônicas na Gold: {len(colunas_canonicas)}")

    partes = []
    for edicao, diretorio in DIRETORIOS_SILVER.items():
        print(f"Lendo Silver {edicao}: {diretorio}")
        df = spark.read.option("header", True).csv(str(diretorio))

        n_preenchidas = 0
        selecoes = [
            F.lit(edicao).alias("edicao"),
            F.monotonically_increasing_id().alias("linha_origem_silver"),
        ]
        for canonico in colunas_canonicas:
            col_original = mapa[edicao].get(canonico)
            if col_original is not None and col_original in df.columns:
                selecoes.append(col_seguro(col_original).alias(canonico))
                n_preenchidas += 1
            else:
                selecoes.append(F.lit(None).cast(StringType()).alias(canonico))

        bloco = df.select(*selecoes)
        print(f"  {edicao}: {n_preenchidas}/{len(colunas_canonicas)} colunas canônicas preenchidas")
        partes.append(bloco)

    gold = partes[0]
    for bloco in partes[1:]:
        gold = gold.unionByName(bloco)

    DIRETORIO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
    gold.write.mode("overwrite").option("header", True).csv(str(DIRETORIO_SAIDA))

    print(f"\nGold final: {gold.count()} linhas, {len(gold.columns)} colunas")
    print(f"Diretório gravado em: {DIRETORIO_SAIDA}")

    spark.stop()


if __name__ == "__main__":
    main()
