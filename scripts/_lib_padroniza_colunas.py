"""
Lógica compartilhada (PySpark) de padronização de header (Bronze -> Silver)
para as edições da pesquisa State of Data Brazil que usam o padrão de
coluna "<código_numérico>_<descrição>" (2024-2025 e 2025-2026).

Cada edição tem seu próprio script fino (02_..., 03_..., ...) que só
declara ARQUIVO_ORIGINAL, DIRETORIO_SAIDA, GRUPOS_ALIAS e CORRECOES_MANUAIS
e chama `executa(...)` daqui. Ver docstring de qualquer um desses scripts
para a explicação completa da lógica de detecção de grupos multi-select
e do coalesce de aliases.

A detecção de grupos e a montagem do nome final das colunas trabalham só
com METADADOS (nomes de coluna, um Python list — igual em pandas ou
Spark), então essa parte não muda com o motor de execução. As partes que
precisam tocar os DADOS de fato usam a API do Spark: `identifica_grupos_base`
checa em uma única agregação (collect_set em lote) se cada coluna candidata
a "filha" de multi-select só tem valores "0"/"1", e `constroi_dataframe_final`
monta o DataFrame final com `select`/`coalesce`. `coluna_e_binaria` fica
disponível como utilitário avulso (checagem pontual de UMA coluna).
"""

from dataclasses import dataclass, field
import re
import unicodedata
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Separa o prefixo de código da pergunta do texto legível.
# "2.l.1_Remuneração/Salário" -> prefixo="2.l.1", descricao="Remuneração/Salário"
PADRAO_PREFIXO = re.compile(r"^(\d+(?:\.[A-Za-z0-9]+)*)[_ ]+(.*)$")

VALORES_BINARIOS_VALIDOS = {"0", "1"}


def cria_spark_session(nome_app: str) -> SparkSession:
    """Cria a SparkSession para rodar local (fora do Glue).

    Dentro do AWS Glue Job, não chame isso — use a sessão já fornecida
    pelo GlueContext:
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext
        glueContext = GlueContext(SparkContext.getOrCreate())
        spark = glueContext.spark_session
    """
    return (
        SparkSession.builder.appName(nome_app)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def le_csv_bruto(spark: SparkSession, caminho: Path) -> DataFrame:
    """Lê um CSV da pesquisa tratando tudo como string (igual ao dtype=str do
    pandas), com suporte a campos multiline e aspas escapadas."""
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(str(caminho))
    )


@dataclass
class GrupoMultiselect:
    raizes: list  # prefixos-raiz que compõem o grupo (>1 só se for alias)
    descricao_pai: str
    colunas_pai: list
    # descricao_opcao -> lista de colunas originais (uma por raiz) que representam essa opção
    opcoes: dict = field(default_factory=dict)


def parseia_coluna(nome_original: str) -> tuple:
    """Retorna (prefixo, descricao) de uma coluna. Prefixo é None se não bater com o padrão."""
    nome = nome_original.strip()
    match = PADRAO_PREFIXO.match(nome)
    if not match:
        return None, nome
    return match.group(1), match.group(2).strip()


def col_seguro(nome: str):
    """Referencia uma coluna pelo nome LITERAL, escapado com backtick.

    O F.col()/df[...] comum interpreta "." como separador de campo
    aninhado (df["a.b"] tentaria acessar o campo "b" dentro de uma
    struct "a"). Isso quebra em cima dos nomes de coluna brutos desta
    pesquisa, que têm ponto de verdade no meio do texto — ex. a tupla do
    2023-2024 termina a frase com "." (ex: "...da companhia.") e algumas
    opções já normalizadas têm ponto no meio (ex: "h2o.ai"). Envolver em
    backtick faz o Spark tratar o nome inteiro como um identificador
    literal, ponto incluso.
    """
    escapado = nome.replace("`", "``")
    return F.col(f"`{escapado}`")


def coluna_e_binaria(df: DataFrame, coluna: str) -> bool:
    """Checa se os valores não nulos de uma coluna são só "0"/"1".

    Usa collect_set (agregação distribuída, uma única ação) limitado a
    200 valores distintos — suficiente pra decidir "é binária?" sem
    trazer pro driver o conteúdo de colunas de texto livre/alta
    cardinalidade por engano.
    """
    linha = df.select(F.slice(F.collect_set(col_seguro(coluna)), 1, 200).alias("valores")).first()
    valores = set(linha["valores"]) if linha and linha["valores"] else set()
    return bool(valores) and valores.issubset(VALORES_BINARIOS_VALIDOS)


def normaliza_nome_coluna(nome: str) -> str:
    """Padroniza a sintaxe final do nome de coluna: minúsculo, sem acento,
    "/" e espaços viram "_", remove pontuação (, ? ( ) .), sem underscores
    duplicados nas pontas.

    Ponto final também é removido (não só ,?()): nome de coluna com "."
    é um problema conhecido no Hive/Athena (o "." separa banco.tabela.coluna
    lá também), então é melhor nunca deixar sobrar no nome definitivo.
    """
    nome = nome.replace("/", "_")
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(ch for ch in nome if not unicodedata.combining(ch))
    nome = nome.lower()
    nome = re.sub(r"[,?().]", "", nome)
    nome = re.sub(r"\s+", "_", nome.strip())
    nome = re.sub(r"_+", "_", nome)
    return nome.strip("_")


def identifica_grupos_base(df: DataFrame, parsed: dict):
    """Identifica, por prefixo-raiz, os grupos pai/filhas de multi-select.

    Uma coluna X.y é "pai" de X.y.N quando X.y existe como coluna E as
    colunas X.y.N têm valores estritamente binários (0/1).

    A checagem de binariedade é feita para TODAS as colunas candidatas de
    uma vez, numa única agregação Spark (uma ação só) — em vez de uma
    ação por coluna, o que em produção (Glue) viraria centenas de jobs
    pequenos desnecessários para uma pesquisa com ~400 colunas.

    Retorna dict: raiz -> GrupoMultiselect (uma única raiz cada, antes do coalesce de alias).
    """
    prefixo_para_col = {p: c for c, (p, _) in parsed.items() if p}

    # 1) Identifica candidatos a "filha" só pelo metadado (nome da coluna),
    #    sem tocar nos dados ainda.
    candidatos = {}
    for col, (prefixo, desc) in parsed.items():
        if not prefixo:
            continue
        partes = prefixo.split(".")
        if len(partes) < 3 or not partes[-1].isdigit():
            continue  # só nos interessa prefixo tipo "2.l.1" (termina em número)
        root = ".".join(partes[:-1])
        pai_col = prefixo_para_col.get(root)
        if pai_col is None:
            continue
        candidatos[col] = (root, pai_col, desc)

    if not candidatos:
        return {}

    # 2) Uma única agregação para checar quais candidatos são binários.
    colunas_candidatas = list(candidatos.keys())
    # Alias posicional (col_0, col_1, ...) em vez do nome original: nomes de
    # coluna com ponto/parênteses/vírgula não podem virar alias de agregação
    # sem passar antes por normalização, e aqui ainda estamos em cima do
    # nome bruto (a normalização só acontece depois, em constroi_dataframe_final).
    apelidos = {f"bin_check_{i}": c for i, c in enumerate(colunas_candidatas)}
    agregacoes = [F.slice(F.collect_set(col_seguro(c)), 1, 200).alias(apelido) for apelido, c in apelidos.items()]
    linha = df.agg(*agregacoes).first()

    # 3) Monta os grupos só com os candidatos confirmados binários.
    grupos: dict = {}
    for apelido, col in apelidos.items():
        root, pai_col, desc = candidatos[col]
        valores = set(linha[apelido]) if linha[apelido] else set()
        if not (valores and valores.issubset(VALORES_BINARIOS_VALIDOS)):
            continue  # filha não é binária -> não é dummy de multi-select (ex: 1.a.1_faixa_idade)

        _, desc_pai = parsed[pai_col]
        grupo = grupos.setdefault(
            root, GrupoMultiselect(raizes=[root], descricao_pai=desc_pai, colunas_pai=[pai_col])
        )
        grupo.opcoes.setdefault(desc, []).append(col)

    return grupos


def aplica_coalesce_alias(grupos: dict, grupos_alias: list) -> list:
    """Funde grupos declarados em `grupos_alias` (mesma pergunta, públicos mutuamente
    exclusivos do formulário) em um único GrupoMultiselect."""
    raizes_em_alias = {r for combo in grupos_alias for r in combo}
    resultado = [g for raiz, g in grupos.items() if raiz not in raizes_em_alias]

    for combo in grupos_alias:
        membros = [grupos[r] for r in combo if r in grupos]
        if len(membros) < 2:
            resultado.extend(membros)  # alias configurado mas não encontrado nos dados; não quebra
            continue

        fundido = GrupoMultiselect(
            raizes=[r for m in membros for r in m.raizes],
            descricao_pai=membros[0].descricao_pai,
            colunas_pai=[c for m in membros for c in m.colunas_pai],
        )
        # Funde opção a opção pelo texto EXATO da descrição da opção.
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
    """Monta o DataFrame final com um `select()` — coalesce nos grupos
    multi-select (quando a opção tiver mais de uma coluna de origem, ex.
    grupo alias) e alias simples nas colunas fora de grupo."""
    colunas_pai_para_remover = {c for g in grupos for c in g.colunas_pai}
    colunas_em_grupo = {c for g in grupos for cols in g.opcoes.values() for c in cols}

    selecoes = []
    nomes_gerados = []

    # 1) Colunas de grupos multi-select (com coalesce quando houver mais de uma coluna por opção)
    for g in grupos:
        for desc_opcao, cols_originais in g.opcoes.items():
            nome_final = normaliza_nome_coluna(f"{g.descricao_pai}_{desc_opcao}")
            expressao = F.coalesce(*[col_seguro(c) for c in cols_originais]).alias(nome_final)
            selecoes.append(expressao)
            nomes_gerados.append(nome_final)

    # 2) Colunas fora de qualquer grupo (perguntas de escolha única)
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

    # Checagem de colisão residual (não deveria ocorrer; se ocorrer, avisa em vez de sobrescrever)
    vistos = set()
    colisoes = [n for n in nomes_gerados if (n in vistos or vistos.add(n))]
    if colisoes:
        raise ValueError(f"Colisão de nomes de coluna não resolvida: {sorted(set(colisoes))}")

    return df.select(*selecoes)


def executa(
    spark: SparkSession,
    arquivo_original: Path,
    diretorio_saida: Path,
    grupos_alias: list,
    correcoes_manuais: dict,
) -> None:
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

    diretorio_saida.parent.mkdir(parents=True, exist_ok=True)
    (
        df_final.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(diretorio_saida))
    )
    print(f"\nColunas originais: {len(colunas_originais)} | Colunas finais: {len(df_final.columns)}")
    print(f"Diretório gravado em Silver: {diretorio_saida}")
