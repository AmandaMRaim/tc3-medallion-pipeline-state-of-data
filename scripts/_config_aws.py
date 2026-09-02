"""
Configuração do ambiente AWS (S3 + Glue Data Catalog) para o pipeline.

Centraliza os nomes/caminhos que mudam entre rodar local e rodar num Glue
Job de verdade.

Camadas:
  - Bronze: dados brutos por edição, em
    s3://{BUCKET}/Bronze/state_of_data/{edicao}/ (lê a pasta inteira —
    não precisa saber o nome exato do arquivo .csv lá dentro).
  - Silver "por edição" (staging): saída dos scripts 01/02/03 — cada
    edição ainda com o SEU PRÓPRIO schema (antes da harmonização),
    usada só como entrada do script 06. Não é a tabela catalogada.
  - Silver "state_of_data_silver" (tabela final catalogada): saída do
    script 06 — schema ÚNICO (harmonizado via dicionário de
    correspondência), uma partição por edição, gravada em
    s3://{BUCKET}/Silver/state_of_data_silver/particao={particao}/
    (padrão Hive "chave=valor", pra facilitar descoberta por
    Crawler/MSCK REPAIR além do registro explícito que o próprio script
    06 faz via boto3 — ver `cataloga_tabela_particionada`).
  - Gold: 7 tabelas de negócio (script 07), lidas a partir da tabela
    Silver catalogada via Spark SQL/Glue Catalog, gravadas em
    s3://{BUCKET}/Gold/perguntas_negocio/{nome_tabela}/ e catalogadas
    (sem partição — "edicao" já é coluna normal) no mesmo banco
    db_state_of_data, uma tabela por pergunta de negócio — ver
    `cataloga_tabela_simples`.
"""

import hashlib

import boto3

BUCKET = "tc-fase3-grupo80-state-of-data-brazil"

# A API do Glue rejeita nome de coluna com mais de 255 caracteres
# (ValidationException). Algumas colunas do schema harmonizado (ex:
# perguntas do 2023-2024 sem correspondência confiável nas outras
# edições, que viram o nome canônico = a frase inteira da pergunta)
# passam disso.
LIMITE_NOME_COLUNA_GLUE = 255

DATABASE = "db_state_of_data"
TABELA_STATE_OF_DATA = "state_of_data_silver"

# Nome da coluna de partição — como esta tabela é criada pelo próprio
# script 06 (via boto3), o caminho no S3 já segue o padrão Hive
# "particao=<valor>/", então o nome pode ser o natural "particao".
NOME_COLUNA_PARTICAO = "particao"

EDICOES = ["2023-2024", "2024-2025", "2025-2026"]


def caminho_bronze(edicao: str) -> str:
    """Pasta do Bronze para a edição — o Spark lê todos os .csv dentro dela,
    não precisa do nome exato do arquivo."""
    return f"s3://{BUCKET}/Bronze/state_of_data/{edicao}/"


def caminho_silver_staging_por_edicao(edicao: str) -> str:
    """Saída de 01/02/03 — schema próprio da edição, ainda não harmonizado."""
    return f"s3://{BUCKET}/Silver/_por_edicao/{edicao}/"


def caminho_base_silver_state_of_data() -> str:
    """Prefixo raiz da tabela Silver final catalogada (sem a partição)."""
    return f"s3://{BUCKET}/Silver/{TABELA_STATE_OF_DATA}/"


def caminho_silver_state_of_data(particao: str) -> str:
    """Partição da tabela Silver final catalogada (schema harmonizado)."""
    return f"{caminho_base_silver_state_of_data()}{NOME_COLUNA_PARTICAO}={particao}/"


def caminho_documentacao(nome_arquivo: str) -> str:
    """Dicionários de nulos/correspondência — CSV único, pequeno, para revisão manual."""
    return f"s3://{BUCKET}/Silver/_documentacao/{nome_arquivo}"


def caminho_gold_pergunta_negocio(nome_tabela: str) -> str:
    return f"s3://{BUCKET}/Gold/perguntas_negocio/{nome_tabela}/"


def tabela_qualificada(nome_tabela: str = TABELA_STATE_OF_DATA) -> str:
    return f"{DATABASE}.{nome_tabela}"


def _trunca_nome_coluna_glue(nome: str) -> str:
    """Encurta um nome de coluna para caber no limite de 255 caracteres do
    Glue, preservando um sufixo de hash pra continuar único mesmo se dois
    nomes longos diferentes só se distinguirem depois do ponto de corte."""
    if len(nome) <= LIMITE_NOME_COLUNA_GLUE:
        return nome
    sufixo = "_" + hashlib.sha1(nome.encode("utf-8")).hexdigest()[:8]
    return nome[: LIMITE_NOME_COLUNA_GLUE - len(sufixo)] + sufixo


def _colunas_para_catalogo(colunas: list) -> list:
    """Aplica `_trunca_nome_coluna_glue` em todas as colunas e garante que
    não colidiram entre si depois do truncamento (o sufixo de hash torna
    isso praticamente impossível, mas falha alto e claro se acontecer, em
    vez de silenciosamente perder uma coluna)."""
    truncadas = [_trunca_nome_coluna_glue(c) for c in colunas]
    vistos: dict = {}
    for original, truncado in zip(colunas, truncadas):
        if truncado in vistos and vistos[truncado] != original:
            raise ValueError(
                f"Colisão ao truncar nomes de coluna para o Glue: "
                f"'{vistos[truncado]}' e '{original}' viraram '{truncado}'"
            )
        vistos[truncado] = original
    return truncadas


def _storage_descriptor(colunas: list, localizacao: str) -> dict:
    """Descriptor de armazenamento CSV compatível com o que o Spark escreve
    por padrão (quoteChar='"', escapeChar='\\', separador ','). Usa
    OpenCSVSerde, que respeita aspas/escapes — diferente do
    LazySimpleSerDe, que trataria vírgula dentro de campo como separador.

    Os nomes de coluna são truncados para o limite do Glue (255
    caracteres) — casamento com o CSV real continua correto porque o
    OpenCSVSerde lê por POSIÇÃO da coluna, não pelo nome."""
    colunas_catalogo = _colunas_para_catalogo(colunas)
    return {
        "Columns": [{"Name": c, "Type": "string"} for c in colunas_catalogo],
        "Location": localizacao,
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            "Parameters": {
                "separatorChar": ",",
                "quoteChar": '"',
                "escapeChar": "\\",
            },
        },
    }


def cataloga_tabela_particionada(database: str, tabela: str, colunas: list, particoes: list, localizacao_base: str) -> None:
    """Cria (ou atualiza, se já existir) uma tabela particionada no Glue
    Data Catalog via boto3 — chamado depois de já ter GRAVADO os arquivos
    de cada partição no S3 (ver script 06). Idempotente: pode rodar de
    novo sem erro se a tabela/partição já existir.

    `colunas`: nomes das colunas do schema harmonizado (todas STRING).
    `particoes`: valores de partição (ex: EDICOES) — o caminho de cada
    uma é montado como `{localizacao_base}{NOME_COLUNA_PARTICAO}={valor}/`.
    """
    glue = boto3.client("glue")

    table_input = {
        "Name": tabela,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "PartitionKeys": [{"Name": NOME_COLUNA_PARTICAO, "Type": "string"}],
        "StorageDescriptor": _storage_descriptor(colunas, localizacao_base),
    }

    try:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        print(f"Tabela criada no Glue Data Catalog: {database}.{tabela}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(DatabaseName=database, TableInput=table_input)
        print(f"Tabela já existia, schema atualizado: {database}.{tabela}")

    for particao in particoes:
        localizacao_particao = f"{localizacao_base}{NOME_COLUNA_PARTICAO}={particao}/"
        partition_input = {
            "Values": [particao],
            "StorageDescriptor": _storage_descriptor(colunas, localizacao_particao),
        }
        try:
            glue.create_partition(DatabaseName=database, TableName=tabela, PartitionInput=partition_input)
            print(f"  Partição registrada: {particao} -> {localizacao_particao}")
        except glue.exceptions.AlreadyExistsException:
            glue.update_partition(
                DatabaseName=database,
                TableName=tabela,
                PartitionValueList=[particao],
                PartitionInput=partition_input,
            )
            print(f"  Partição já existia, atualizada: {particao} -> {localizacao_particao}")


def cataloga_tabela_simples(database: str, tabela: str, colunas: list, localizacao: str) -> None:
    """Cria (ou atualiza, se já existir) uma tabela SEM partição no Glue
    Data Catalog via boto3 — usado nas tabelas Gold (script 07): cada uma
    já tem "edicao" como coluna normal (a agregação cruza as 3 edições
    dentro do mesmo arquivo), não faz sentido particionar por edição
    de novo. Idempotente, mesma lógica de `cataloga_tabela_particionada`.
    """
    glue = boto3.client("glue")

    table_input = {
        "Name": tabela,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "StorageDescriptor": _storage_descriptor(colunas, localizacao),
    }

    try:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        print(f"Tabela criada no Glue Data Catalog: {database}.{tabela}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(DatabaseName=database, TableInput=table_input)
        print(f"Tabela já existia, schema atualizado: {database}.{tabela}")
