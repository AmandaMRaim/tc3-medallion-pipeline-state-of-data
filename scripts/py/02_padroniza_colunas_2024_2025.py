"""
Etapa PySpark (Bronze -> Silver) — Padronização do header da edição 2024-2025.

Colunas prefixadas por código de pergunta (ex: "2.l.1_Remuneração/Salário").
Multi-select vem em par pai (texto concatenado) + filhas binárias — lógica
de detecção e o alias 3.f/4.l (mesma pergunta, gestor vs. não-gestor)
estão em _lib_padroniza_colunas.py.

Lê o Bronze do S3, escreve em Silver "por edição" (staging, schema
próprio — harmonização entre as 3 edições acontece no script 06).
"""

from _config_aws import caminho_bronze, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import cria_spark_session, executa

EDICAO = "2024-2025"
ARQUIVO_ORIGINAL = caminho_bronze(EDICAO)
DIRETORIO_SAIDA = caminho_silver_staging_por_edicao(EDICAO)

CORRECOES_MANUAIS: dict = {}

GRUPOS_ALIAS = [
    ("3.f", "4.l"),  # tipo de uso de IA generativa: gestor vs. não-gestor
]


if __name__ == "__main__":
    spark = cria_spark_session("padroniza_colunas_2024_2025")
    executa(spark, ARQUIVO_ORIGINAL, DIRETORIO_SAIDA, GRUPOS_ALIAS, CORRECOES_MANUAIS)
    spark.stop()
