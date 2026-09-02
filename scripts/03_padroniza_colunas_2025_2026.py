"""
Etapa PySpark (Bronze -> Silver) — Padronização do header da edição 2025-2026.

Mesma lógica do script de 2024-2025 (ver 02_padroniza_colunas_2024_2025.py
para a explicação completa; lógica compartilhada em _lib_padroniza_colunas.py).

Diferença confirmada nos dados desta edição: os CÓDIGOS de pergunta não são
estáveis entre edições (o mesmo código numérico muda de significado ano a
ano — ex: "4.d" é "linguagem_de_programacao" em 2024-2025 mas
"banco_de_dados" em 2025-2026). O grupo alias de IA generativa também mudou
de código: em 2024-2025 era "3.f"/"4.l", nesta edição é "3.f"/"4.i" — mesma
pergunta (gestor vs. não-gestor), mesmas 8 opções com texto idêntico,
preenchimento mutuamente exclusivo (confirmado antes de codar).

LÊ o Bronze do S3 (sem alterá-lo) e ESCREVE o resultado em Silver
"por edição" (staging) no S3, como diretório Spark (part-*.csv dentro).
Esse resultado ainda está no schema PRÓPRIO desta edição — a
harmonização entre as 3 edições (schema único, viram partições da
tabela catalogada db_state_of_data.state_of_data_silver) acontece no script 06.

Uso (local, fora do Glue — exige credenciais AWS configuradas para o
Spark local enxergar o S3):
    python scripts/03_padroniza_colunas_2025_2026.py
"""

from _config_aws import caminho_bronze, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import cria_spark_session, executa

EDICAO = "2025-2026"
ARQUIVO_ORIGINAL = caminho_bronze(EDICAO)
DIRETORIO_SAIDA = caminho_silver_staging_por_edicao(EDICAO)

# Correções pontuais para nomes com erro de digitação no CSV de origem.
# Chave: nome original completo da coluna, como vem no header.
CORRECOES_MANUAIS: dict = {}

# Grupos que descrevem a mesma pergunta de negócio mas são roteados a
# públicos mutuamente exclusivos do formulário (confirmado nos dados:
# preenchimento nunca se sobrepõe, mesmas 8 opções). Notação com "."
GRUPOS_ALIAS = [
    ("3.f", "4.i"),  # tipo de uso de IA generativa na empresa: gestor vs. não-gestor
]


if __name__ == "__main__":
    spark = cria_spark_session("padroniza_colunas_2025_2026")
    executa(spark, ARQUIVO_ORIGINAL, DIRETORIO_SAIDA, GRUPOS_ALIAS, CORRECOES_MANUAIS)
    spark.stop()
