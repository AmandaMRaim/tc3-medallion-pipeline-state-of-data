"""
Etapa PySpark (Bronze -> Silver) — Padronização do header da edição 2025-2026.

Mesma lógica do script de 2024-2025. Diferença: os CÓDIGOS de pergunta
não são estáveis entre edições (ex: "4.d" é "linguagem_de_programacao"
em 2024-2025 mas "banco_de_dados" aqui). Alias de IA generativa mudou
de código pra "3.f"/"4.i" (mesma pergunta, gestor vs. não-gestor).

Lê o Bronze do S3, escreve em Silver "por edição" (staging, schema
próprio — harmonização entre as 3 edições acontece no script 06).
"""

from _config_aws import caminho_bronze, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import cria_spark_session, executa

EDICAO = "2025-2026"
ARQUIVO_ORIGINAL = caminho_bronze(EDICAO)
DIRETORIO_SAIDA = caminho_silver_staging_por_edicao(EDICAO)

CORRECOES_MANUAIS: dict = {}

GRUPOS_ALIAS = [
    ("3.f", "4.i"),  # tipo de uso de IA generativa: gestor vs. não-gestor
]


if __name__ == "__main__":
    spark = cria_spark_session("padroniza_colunas_2025_2026")
    executa(spark, ARQUIVO_ORIGINAL, DIRETORIO_SAIDA, GRUPOS_ALIAS, CORRECOES_MANUAIS)
    spark.stop()
