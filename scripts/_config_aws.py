"""
Configuração do ambiente AWS (S3 + Glue Data Catalog) para o pipeline.

Centraliza os nomes/caminhos que mudam entre rodar local e rodar num Glue
Job de verdade. Valores confirmados pelo grupo: BUCKET, DATABASE, TABELA,
caminho do Bronze e da tabela Silver particionada. O nome da coluna de
partição no Glue Catalog (NOME_COLUNA_PARTICAO) ainda é uma suposição —
ajuste se o catálogo usar outro nome.

Camadas:
  - Bronze: dados brutos por edição, em
    s3://{BUCKET}/Bronze/state_of_data/{edicao}/ (lê a pasta inteira —
    não precisa saber o nome exato do arquivo .csv lá dentro).
  - Silver "por edição" (staging): saída dos scripts 01/02/03 — cada
    edição ainda com o SEU PRÓPRIO schema (antes da harmonização),
    usada só como entrada do script 06. Não é a tabela catalogada.
  - Silver "state_of_data" (tabela final catalogada): saída do script 06
    — schema ÚNICO (harmonizado via dicionário de correspondência),
    uma partição por edição, gravada em
    s3://{BUCKET}/Silver/state_of_data/{particao}/
    Essa tabela e as partições JÁ EXISTEM no Glue Data Catalog — os
    scripts só escrevem os arquivos no caminho certo, sem recatalogar.
  - Gold: tabelas de negócio (script 07), lidas a partir da tabela Silver
    catalogada via Spark SQL/Glue Catalog, gravadas em
    s3://{BUCKET}/Gold/perguntas_negocio/{nome_tabela}/
"""

BUCKET = "tc-fase3-grupo80-state-of-data-brazil"

DATABASE = "db_state_of_data"
TABELA_STATE_OF_DATA = "state_of_data"

# Nome da coluna de partição tal como registrada no Glue Data Catalog.
# É "partition_0" (não "particao") porque as pastas no S3 não seguem o
# padrão Hive "chave=valor" (ex: "particao=2023-2024/") — são só o valor
# ("2023-2024/"), então o crawler nomeou a partição genericamente.
NOME_COLUNA_PARTICAO = "partition_0"

EDICOES = ["2023-2024", "2024-2025", "2025-2026"]


def caminho_bronze(edicao: str) -> str:
    """Pasta do Bronze para a edição — o Spark lê todos os .csv dentro dela,
    não precisa do nome exato do arquivo."""
    return f"s3://{BUCKET}/Bronze/state_of_data/{edicao}/"


def caminho_silver_staging_por_edicao(edicao: str) -> str:
    """Saída de 01/02/03 — schema próprio da edição, ainda não harmonizado."""
    return f"s3://{BUCKET}/Silver/_por_edicao/{edicao}/"


def caminho_silver_state_of_data(particao: str) -> str:
    """Partição da tabela Silver final catalogada (schema harmonizado)."""
    return f"s3://{BUCKET}/Silver/state_of_data/{particao}/"


def caminho_documentacao(nome_arquivo: str) -> str:
    """Dicionários de nulos/correspondência — CSV único, pequeno, para revisão manual."""
    return f"s3://{BUCKET}/Silver/_documentacao/{nome_arquivo}"


def caminho_gold_pergunta_negocio(nome_tabela: str) -> str:
    return f"s3://{BUCKET}/Gold/perguntas_negocio/{nome_tabela}/"


def tabela_qualificada(nome_tabela: str = TABELA_STATE_OF_DATA) -> str:
    return f"{DATABASE}.{nome_tabela}"
