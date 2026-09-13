"""
Configuração do ambiente AWS (S3 + Glue Data Catalog) para o pipeline.

Camadas: Bronze (s3://{BUCKET}/Bronze/state_of_data/{edicao}/) -> Silver
"por edição" (staging, schema próprio, script 06 harmoniza) -> Silver
"state_of_data_silver" (tabela catalogada, 1 partição por edição) ->
Gold (7 tabelas de negócio, também catalogadas, sem partição).
"""

import hashlib

import boto3

BUCKET = "tc-fase3-grupo80-state-of-data-brazil"

# API do Glue rejeita nome de coluna com mais de 255 caracteres.
LIMITE_NOME_COLUNA_GLUE = 255

DATABASE = "db_state_of_data"
TABELA_STATE_OF_DATA = "state_of_data_silver"

# Caminho no S3 já segue o padrão Hive "particao=<valor>/".
NOME_COLUNA_PARTICAO = "particao"

EDICOES = ["2023-2024", "2024-2025", "2025-2026"]


def caminho_bronze(edicao: str) -> str:
    return f"s3://{BUCKET}/Bronze/state_of_data/{edicao}/"


def caminho_silver_staging_por_edicao(edicao: str) -> str:
    return f"s3://{BUCKET}/Silver/_por_edicao/{edicao}/"


def caminho_base_silver_state_of_data() -> str:
    return f"s3://{BUCKET}/Silver/{TABELA_STATE_OF_DATA}/"


def caminho_silver_state_of_data(particao: str) -> str:
    return f"{caminho_base_silver_state_of_data()}{NOME_COLUNA_PARTICAO}={particao}/"


def caminho_documentacao(nome_arquivo: str) -> str:
    return f"s3://{BUCKET}/Silver/_documentacao/{nome_arquivo}"


def caminho_gold_pergunta_negocio(nome_tabela: str) -> str:
    return f"s3://{BUCKET}/Gold/perguntas_negocio/{nome_tabela}/"


def tabela_qualificada(nome_tabela: str = TABELA_STATE_OF_DATA) -> str:
    return f"{DATABASE}.{nome_tabela}"


def _trunca_nome_coluna_glue(nome: str) -> str:
    """Corta pro limite do Glue com sufixo de hash, pra continuar único."""
    if len(nome) <= LIMITE_NOME_COLUNA_GLUE:
        return nome
    sufixo = "_" + hashlib.sha1(nome.encode("utf-8")).hexdigest()[:8]
    return nome[: LIMITE_NOME_COLUNA_GLUE - len(sufixo)] + sufixo


def _colunas_para_catalogo(colunas: list) -> list:
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
    """OpenCSVSerde (não LazySimpleSerDe) porque respeita aspas/vírgula
    dentro de campo. Casamento por POSIÇÃO da coluna, não pelo nome —
    por isso dá pra truncar o nome sem afetar o dado."""
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
    """Cria/atualiza (idempotente) a tabela particionada no Glue Data
    Catalog — chamado DEPOIS de já ter gravado os arquivos no S3."""
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
    """Igual `cataloga_tabela_particionada`, mas sem partição (tabelas
    Gold já têm "edicao" como coluna normal)."""
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
