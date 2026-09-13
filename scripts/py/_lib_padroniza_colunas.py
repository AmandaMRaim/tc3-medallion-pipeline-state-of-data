"""
Lógica compartilhada (PySpark) de padronização de header (Bronze -> Silver)
para as edições da pesquisa State of Data Brazil. Cada script de edição
(01/02/03) só declara suas constantes e chama `executa(...)` daqui.
"""

from dataclasses import dataclass, field
import re
import unicodedata
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# "2.l.1_Remuneração/Salário" -> prefixo="2.l.1", descricao="Remuneração/Salário"
PADRAO_PREFIXO = re.compile(r"^(\d+(?:\.[A-Za-z0-9]+)*)[_ ]+(.*)$")

VALORES_BINARIOS_VALIDOS = {"0", "1"}


def repara_mojibake(texto):
    """Reverte UTF-8 lido como Latin-1 (ex: "não" -> "nÃ£o"). Texto já
    correto falha no round-trip e fica inalterado — seguro aplicar sempre."""
    if texto is None:
        return None
    try:
        return texto.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return texto


_repara_mojibake_udf = F.udf(repara_mojibake, StringType())


def cria_spark_session(nome_app: str) -> SparkSession:
    """Só para rodar local. Dentro de um Glue Job, use a sessão do GlueContext."""
    return (
        SparkSession.builder.appName(nome_app)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def le_csv_bruto(spark: SparkSession, caminho: Path) -> DataFrame:
    """Lê CSV como string (igual dtype=str do pandas), com multiline/aspas
    escapadas. Repara mojibake em nomes de coluna e valores — o
    option("encoding") sozinho não corrige arquivo já salvo com bytes errados."""
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(str(caminho))
    )

    df = df.toDF(*[repara_mojibake(c) for c in df.columns])
    df = df.select(*[_repara_mojibake_udf(col_seguro(c)).alias(c) for c in df.columns])
    return df


@dataclass
class GrupoMultiselect:
    raizes: list
    descricao_pai: str
    colunas_pai: list
    opcoes: dict = field(default_factory=dict)


def parseia_coluna(nome_original: str) -> tuple:
    """Retorna (prefixo, descricao); prefixo é None se não bater com o padrão."""
    nome = nome_original.strip()
    match = PADRAO_PREFIXO.match(nome)
    if not match:
        return None, nome
    return match.group(1), match.group(2).strip()


def col_seguro(nome: str):
    """Referencia a coluna pelo nome literal (backtick) — F.col() comum trata
    "." como separador de campo aninhado, o que quebra nomes com ponto de
    verdade no texto (ex: "h2o.ai")."""
    escapado = nome.replace("`", "``")
    return F.col(f"`{escapado}`")


def coluna_e_binaria(df: DataFrame, coluna: str) -> bool:
    """Checa se os valores não nulos são só "0"/"1" (collect_set limitado a
    200, para não trazer coluna de alta cardinalidade pro driver)."""
    linha = df.select(F.slice(F.collect_set(col_seguro(coluna)), 1, 200).alias("valores")).first()
    valores = set(linha["valores"]) if linha and linha["valores"] else set()
    return bool(valores) and valores.issubset(VALORES_BINARIOS_VALIDOS)


def normaliza_nome_coluna(nome: str) -> str:
    """minúsculo, sem acento, "/" e espaço viram "_", remove pontuação
    (inclui "." — nome de coluna com ponto é problema conhecido no Hive/Athena)."""
    nome = nome.replace("/", "_")
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(ch for ch in nome if not unicodedata.combining(ch))
    nome = nome.lower()
    nome = re.sub(r"[,?().]", "", nome)
    nome = re.sub(r"\s+", "_", nome.strip())
    nome = re.sub(r"_+", "_", nome)
    return nome.strip("_")


def identifica_grupos_base(df: DataFrame, parsed: dict):
    """Uma coluna X.y é "pai" de X.y.N quando X.y existe como coluna E as
    X.y.N são binárias (0/1). Checagem de binariedade em UMA agregação só
    (evita uma ação por coluna candidata)."""
    prefixo_para_col = {p: c for c, (p, _) in parsed.items() if p}

    candidatos = {}
    for col, (prefixo, desc) in parsed.items():
        if not prefixo:
            continue
        partes = prefixo.split(".")
        if len(partes) < 3 or not partes[-1].isdigit():
            continue
        root = ".".join(partes[:-1])
        pai_col = prefixo_para_col.get(root)
        if pai_col is None:
            continue
        candidatos[col] = (root, pai_col, desc)

    if not candidatos:
        return {}

    # Alias posicional: nome bruto pode ter ponto/parênteses/vírgula, ainda
    # sem normalização nesse ponto.
    colunas_candidatas = list(candidatos.keys())
    apelidos = {f"bin_check_{i}": c for i, c in enumerate(colunas_candidatas)}
    agregacoes = [F.slice(F.collect_set(col_seguro(c)), 1, 200).alias(apelido) for apelido, c in apelidos.items()]
    linha = df.agg(*agregacoes).first()

    grupos: dict = {}
    for apelido, col in apelidos.items():
        root, pai_col, desc = candidatos[col]
        valores = set(linha[apelido]) if linha[apelido] else set()
        if not (valores and valores.issubset(VALORES_BINARIOS_VALIDOS)):
            continue

        _, desc_pai = parsed[pai_col]
        grupo = grupos.setdefault(
            root, GrupoMultiselect(raizes=[root], descricao_pai=desc_pai, colunas_pai=[pai_col])
        )
        grupo.opcoes.setdefault(desc, []).append(col)

    return grupos


def aplica_coalesce_alias(grupos: dict, grupos_alias: list) -> list:
    """Funde grupos de `grupos_alias` (mesma pergunta, públicos mutuamente
    exclusivos) num só GrupoMultiselect, opção a opção por texto exato."""
    raizes_em_alias = {r for combo in grupos_alias for r in combo}
    resultado = [g for raiz, g in grupos.items() if raiz not in raizes_em_alias]

    for combo in grupos_alias:
        membros = [grupos[r] for r in combo if r in grupos]
        if len(membros) < 2:
            resultado.extend(membros)
            continue

        fundido = GrupoMultiselect(
            raizes=[r for m in membros for r in m.raizes],
            descricao_pai=membros[0].descricao_pai,
            colunas_pai=[c for m in membros for c in m.colunas_pai],
        )
        chaves_opcao = {desc for m in membros for desc in m.opcoes}
        for desc in chaves_opcao:
            cols = [c for m in membros for c in m.opcoes.get(desc, [])]
            fundido.opcoes[desc] = cols
        resultado.append(fundido)

    return resultado


def constroi_dataframe_final(
    df: DataFrame,
    grupos: list,
    parsed: dict,
    correcoes_manuais: dict,
) -> DataFrame:
    """Coalesce nos grupos multi-select (quando a opção tem mais de uma
    coluna de origem, ex. alias); alias simples nas colunas fora de grupo."""
    colunas_pai_para_remover = {c for g in grupos for c in g.colunas_pai}
    colunas_em_grupo = {c for g in grupos for cols in g.opcoes.values() for c in cols}

    selecoes = []
    nomes_gerados = []

    for g in grupos:
        for desc_opcao, cols_originais in g.opcoes.items():
            nome_final = normaliza_nome_coluna(f"{g.descricao_pai}_{desc_opcao}")
            expressao = F.coalesce(*[col_seguro(c) for c in cols_originais]).alias(nome_final)
            selecoes.append(expressao)
            nomes_gerados.append(nome_final)

    for col in df.columns:
        if col in colunas_pai_para_remover or col in colunas_em_grupo:
            continue
        if col in correcoes_manuais:
            nome_final = normaliza_nome_coluna(correcoes_manuais[col])
        else:
            _, desc = parsed[col]
            nome_final = normaliza_nome_coluna(desc)
        selecoes.append(col_seguro(col).alias(nome_final))
        nomes_gerados.append(nome_final)

    vistos = set()
    colisoes = [n for n in nomes_gerados if (n in vistos or vistos.add(n))]
    if colisoes:
        raise ValueError(f"Colisão de nomes de coluna não resolvida: {sorted(set(colisoes))}")

    return df.select(*selecoes)


def executa(
    spark: SparkSession,
    arquivo_original,
    diretorio_saida,
    grupos_alias: list,
    correcoes_manuais: dict,
) -> None:
    """`arquivo_original`/`diretorio_saida` aceitam Path local ou URI S3."""
    print(f"Lendo: {arquivo_original}")
    df = le_csv_bruto(spark, arquivo_original)

    colunas_originais = df.columns
    parsed = {c: parseia_coluna(c) for c in colunas_originais}

    grupos_base = identifica_grupos_base(df, parsed)
    grupos = aplica_coalesce_alias(grupos_base, grupos_alias)

    n_alias = sum(1 for g in grupos if len(g.raizes) > 1)
    print(f"Grupos multi-select detectados: {len(grupos)} (dos quais {n_alias} unificados por alias)")
    print(f"Colunas-pai removidas: {sum(len(g.colunas_pai) for g in grupos)}")

    df_final = constroi_dataframe_final(df, grupos, parsed, correcoes_manuais)

    if isinstance(diretorio_saida, Path):
        diretorio_saida.parent.mkdir(parents=True, exist_ok=True)
    (
        df_final.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("encoding", "UTF-8")
        .csv(str(diretorio_saida))
    )
    print(f"\nColunas originais: {len(colunas_originais)} | Colunas finais: {len(df_final.columns)}")
    print(f"Diretório gravado em Silver: {diretorio_saida}")
