"""
Etapa PySpark (Bronze -> Silver) — Padronização do header da edição 2024-2025.

O CSV bruto dessa edição prefixa cada coluna com um código de pergunta,
ex: "1.a_idade", "2.l.1_Remuneração/Salário". Além disso, várias perguntas
de múltipla escolha (checkbox) vêm em dois formatos redundantes:

  - uma coluna "pai" com o texto concatenado das opções marcadas
    (ex: "2.l_motivo_insatisfacao" = "Benefícios, Falta de crescimento")
  - várias colunas "filhas" binárias (0/1), uma por opção
    (ex: "2.l.1_Remuneração/Salário" = 1, "2.l.2_Benefícios" = 0, ...)

Este script (lógica compartilhada em _lib_padroniza_colunas.py):

  1. Detecta automaticamente grupos pai/filhas: uma coluna X.y é "pai" de
     X.y.N quando X.y existe como coluna E as colunas X.y.N têm valores
     estritamente binários (0/1) — checado via collect_set no Spark.

  2. Detecta grupos "alias" — dois grupos pai/filhas diferentes que
     descrevem exatamente a mesma pergunta de negócio, mas são
     preenchidos por públicos mutuamente exclusivos do questionário
     (skip logic). Confirmado nos dados: "3.f_tipo_de_uso_de_ai_..."
     (respondida só por quem é gestor) e "4.l_tipo_de_uso_de_ai_..."
     (respondida só por quem não é gestor) nunca são preenchidas juntas
     e têm exatamente as mesmas 8 opções, com texto idêntico. Grupos
     alias são unificados (coalesce) em um único conjunto de colunas,
     evitando colunas espelhadas majoritariamente nulas.

  3. Renomeia cada coluna final para "{descrição da pergunta-pai}_
     {descrição da opção}", preservando o contexto da pergunta.

  4. Descarta as colunas-pai (texto concatenado).

  5. Para colunas fora de qualquer grupo multi-select, apenas remove o
     prefixo numérico do código, mantendo a descrição.

  6. Padroniza a sintaxe de todo nome final de coluna: minúsculo, sem
     acento, "/" e espaços viram "_", sem pontuação (, ? ( )).

LÊ o Bronze (sem alterá-lo) e ESCREVE o resultado em Silver como um
diretório Spark (part-*.csv dentro).

Uso (local, fora do Glue):
    python scripts/02_padroniza_colunas_2024_2025.py
"""

from pathlib import Path

from _lib_padroniza_colunas import cria_spark_session, executa

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_ORIGINAL = BASE_DIR / "Bronze" / "2024-2025" / "state-of-data-brazil-2024-2025.csv"
DIRETORIO_SAIDA = BASE_DIR / "Silver" / "2024-2025" / "state-of-data-brazil-2024-2025_colunas_limpas"

# Correções pontuais para nomes com erro de digitação no CSV de origem.
# Chave: nome original completo da coluna, como vem no header.
CORRECOES_MANUAIS: dict = {}

# Grupos que descrevem a mesma pergunta de negócio mas são roteados a
# públicos mutuamente exclusivos do formulário (confirmado nos dados
# antes de codar isso: preenchimento nunca se sobrepõe e as opções são
# idênticas). Cada tupla é o prefixo-raiz do grupo (ex: "3.f", "4.l").
GRUPOS_ALIAS = [
    ("3.f", "4.l"),  # tipo de uso de IA generativa na empresa: gestor vs. não-gestor
]


if __name__ == "__main__":
    spark = cria_spark_session("padroniza_colunas_2024_2025")
    executa(spark, ARQUIVO_ORIGINAL, DIRETORIO_SAIDA, GRUPOS_ALIAS, CORRECOES_MANUAIS)
    spark.stop()
