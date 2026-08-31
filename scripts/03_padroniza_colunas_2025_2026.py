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

LÊ o Bronze (sem alterá-lo) e ESCREVE o resultado em Silver como um
diretório Spark (part-*.csv dentro).

Uso (local, fora do Glue):
    python scripts/03_padroniza_colunas_2025_2026.py
"""

from pathlib import Path

from _lib_padroniza_colunas import cria_spark_session, executa

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_ORIGINAL = BASE_DIR / "Bronze" / "2025-2026" / "state-of-data-brazil-2025-2026.csv"
DIRETORIO_SAIDA = BASE_DIR / "Silver" / "2025-2026" / "state-of-data-brazil-2025-2026_colunas_limpas"

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
