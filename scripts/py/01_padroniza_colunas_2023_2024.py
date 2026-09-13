"""
Etapa PySpark (Bronze -> Silver) — Padronização do header da edição 2023-2024.

Header vem como tupla Python (ex: "('P1_a ', 'Idade')"); código usa "_"
como separador (convertido para "." pra reaproveitar a lógica de grupos
multi-select de _lib_padroniza_colunas.py). Alias P3_f/P4_l: mesma
pergunta sobre uso de IA generativa, gestor vs. não-gestor — 7 das 8
opções têm texto idêntico e são unificadas; a 8ª (P3_f_4/P4_l_4) tem
texto diferente e fica como duas colunas separadas.

Lê o Bronze do S3, escreve em Silver "por edição" (staging, schema
próprio — harmonização entre as 3 edições acontece no script 06).
"""

import re

from _config_aws import caminho_bronze, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import (
    aplica_coalesce_alias,
    constroi_dataframe_final,
    cria_spark_session,
    identifica_grupos_base,
    le_csv_bruto,
)

EDICAO = "2023-2024"
ARQUIVO_ORIGINAL = caminho_bronze(EDICAO)
DIRETORIO_SAIDA = caminho_silver_staging_por_edicao(EDICAO)

# "('P1_a ', 'Idade')" -> ("P1_a", "Idade")
PADRAO_TUPLA = re.compile(r"^\(\s*'(.*?)'\s*,\s*'(.*?)'\s*\)$")

# Tupla malformada no CSV de origem (falta fechar aspa/parêntese).
CORRECOES_TUPLA_MALFORMADA = {
    "('P6_b_16 ', 'SQL Server Integration Services (SSIS))": ("P6_b_16", "SQL Server Integration Services (SSIS)"),
}

GRUPOS_ALIAS = [
    ("P3.f", "P4.l"),  # tipo de uso de IA generativa: gestor vs. não-gestor
]


def parseia_coluna_tupla(nome_original: str):
    """Extrai (codigo, descricao) de uma coluna no formato tupla Python."""
    if nome_original in CORRECOES_TUPLA_MALFORMADA:
        return CORRECOES_TUPLA_MALFORMADA[nome_original]

    match = PADRAO_TUPLA.match(nome_original.strip())
    if not match:
        return None, nome_original  # mantém original se não bater (não deveria ocorrer)
    return match.group(1).strip(), match.group(2).strip()


def main() -> None:
    spark = cria_spark_session("padroniza_colunas_2023_2024")

    print(f"Lendo: {ARQUIVO_ORIGINAL}")
    df = le_csv_bruto(spark, ARQUIVO_ORIGINAL)

    colunas_originais = df.columns

    parsed = {}
    for col in colunas_originais:
        codigo, descricao = parseia_coluna_tupla(col)
        prefixo = codigo.replace("_", ".") if codigo else None
        parsed[col] = (prefixo, descricao)

    grupos_base = identifica_grupos_base(df, parsed)
    grupos = aplica_coalesce_alias(grupos_base, GRUPOS_ALIAS)

    n_alias = sum(1 for g in grupos if len(g.raizes) > 1)
    print(f"Grupos multi-select detectados: {len(grupos)} (dos quais {n_alias} unificados por alias)")
    print(f"Colunas-pai removidas: {sum(len(g.colunas_pai) for g in grupos)}")

    df_final = constroi_dataframe_final(df, grupos, parsed, correcoes_manuais={})

    df_final.coalesce(1).write.mode("overwrite").option("header", True).option("encoding", "UTF-8").csv(DIRETORIO_SAIDA)

    print(f"\nColunas originais: {len(colunas_originais)} | Colunas finais: {len(df_final.columns)}")
    print(f"Diretório gravado em Silver: {DIRETORIO_SAIDA}")

    spark.stop()


if __name__ == "__main__":
    main()
