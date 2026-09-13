"""
Etapa PySpark (Silver "state_of_data_silver" -> Gold por pergunta de
negócio) — State of Data Brazil.

Lê a tabela Silver catalogada via `spark.table(...)` e gera 7 tabelas
Gold pré-agregadas, uma por pergunta de negócio do desafio (estrutura do
mercado, perfis valorizados, diversidade, adoção de tecnologia/IA,
diferenças regionais, oportunidades e desafios). % de adoção nos grupos
multi-select é calculado sobre a população ELEGÍVEL (quem respondeu
aquele grupo), não a base toda. Cada tabela é gravada no S3 e catalogada
(sem partição — "edicao" é coluna normal) no mesmo banco da Silver.
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from _config_aws import (
    DATABASE,
    NOME_COLUNA_PARTICAO,
    caminho_gold_pergunta_negocio,
    cataloga_tabela_simples,
    tabela_qualificada,
)
from _lib_padroniza_colunas import col_seguro, cria_spark_session

TABELA_ORIGEM = tabela_qualificada()


def uniao(dfs: list) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def distribuicao_categorica(df: DataFrame, colunas: list, dimensoes: list = ("edicao",)) -> DataFrame:
    """Contagem e % por `dimensoes`, calculado separadamente por coluna.
    Retorna: dimensoes..., variavel, valor, contagem, total_respondentes, pct_na_dimensao."""
    dimensoes = list(dimensoes)
    partes = []
    for coluna in colunas:
        sub = (
            df.select(*dimensoes, col_seguro(coluna).alias("valor"))
            .filter(F.col("valor").isNotNull())
            .withColumn("variavel", F.lit(coluna))
        )
        contagem = sub.groupBy(*dimensoes, "variavel", "valor").agg(F.count(F.lit(1)).alias("contagem"))
        total = sub.groupBy(*dimensoes).agg(F.count(F.lit(1)).alias("total_respondentes"))
        resultado = contagem.join(total, on=dimensoes, how="inner").withColumn(
            "pct_na_dimensao", F.round(F.col("contagem") / F.col("total_respondentes") * 100, 1)
        )
        partes.append(resultado)

    return uniao(partes)


def desmancha_grupo_multiselect(df: DataFrame, prefixo: str, dimensoes: list = ("edicao",)) -> DataFrame:
    """Desmancha um grupo de colunas binárias (0/1) em formato longo. %
    de adoção sobre a população ELEGÍVEL (tem ao menos 1 coluna não nula
    no grupo), não a base toda. Retorna: dimensoes..., opcao, elegiveis,
    selecionaram, pct_adocao."""
    dimensoes = list(dimensoes)
    colunas_grupo = [c for c in df.columns if c.startswith(prefixo)]
    if not colunas_grupo:
        raise ValueError(f"Nenhuma coluna encontrada com o prefixo '{prefixo}'")

    elegivel = F.coalesce(*[col_seguro(c) for c in colunas_grupo]).isNotNull()
    base_elegivel = df.filter(elegivel)

    elegiveis_por_dimensao = base_elegivel.groupBy(*dimensoes).agg(F.count(F.lit(1)).alias("elegiveis"))

    linhas = []
    for coluna in colunas_grupo:
        opcao = coluna[len(prefixo):]
        numerica = F.coalesce(col_seguro(coluna).cast("double"), F.lit(0.0))
        agrupado = base_elegivel.groupBy(*dimensoes).agg(F.sum(numerica).alias("selecionaram")).withColumn(
            "opcao", F.lit(opcao)
        )
        linhas.append(agrupado)

    resultado = uniao(linhas).join(elegiveis_por_dimensao, on=dimensoes, how="inner")
    resultado = resultado.withColumn("pct_adocao", F.round(F.col("selecionaram") / F.col("elegiveis") * 100, 1))
    colunas_ordem = [*dimensoes, "opcao", "elegiveis", "selecionaram", "pct_adocao"]
    ordenacao = [F.col(d).asc() for d in dimensoes] + [F.col("pct_adocao").desc()]
    return resultado.select(*colunas_ordem).orderBy(*ordenacao)


def desmancha_grupos_seguro(df: DataFrame, grupos: dict, dimensoes: list = ("edicao",)) -> list:
    """Igual `desmancha_grupo_multiselect` pra cada grupo, mas PULA (com
    aviso) prefixo sem nenhuma coluna — acontece quando o grupo depende de
    correspondência manual ainda não curada no dicionário."""
    resultado = []
    for categoria, prefixo in grupos.items():
        if not any(c.startswith(prefixo) for c in df.columns):
            print(f"  aviso: nenhuma coluna com prefixo '{prefixo}' — categoria '{categoria}' pulada")
            continue
        sub = desmancha_grupo_multiselect(df, prefixo, dimensoes=dimensoes)
        sub = sub.withColumn("categoria", F.lit(categoria))
        resultado.append(sub)
    return resultado


def gold_01_estrutura_mercado(df: DataFrame) -> DataFrame:
    colunas = [
        "cargo_atual", "nivel", "situacao_de_trabalho", "modelo_de_trabalho_atual",
        "regiao_onde_mora", "setor", "numero_de_funcionarios",
    ]
    colunas = [c for c in colunas if c in df.columns]
    return distribuicao_categorica(df, colunas, dimensoes=["edicao"])


def gold_02_perfis_valorizados(df: DataFrame) -> DataFrame:
    dimensoes = ["edicao", "cargo_atual", "nivel"]
    dimensoes = [c for c in dimensoes if c in df.columns]
    return distribuicao_categorica(df, ["faixa_salarial"], dimensoes=dimensoes)


def gold_03_diversidade_genero(df: DataFrame) -> DataFrame:
    dimensoes = ["edicao", "genero"]
    dimensoes = [c for c in dimensoes if c in df.columns]
    colunas = [c for c in ["cargo_atual", "nivel", "faixa_salarial", "cor_raca_etnia"] if c in df.columns]
    return distribuicao_categorica(df, colunas, dimensoes=dimensoes)


def gold_04_adocao_tecnologias(df: DataFrame) -> DataFrame:
    grupos = {
        "linguagem_de_programacao": "linguagem_de_programacao_dia_a_dia_",
        "banco_de_dados": "banco_de_dados_dia_a_dia_",
        "cloud": "cloud_dia_a_dia_",
        "ferramenta_de_bi": "ferramenta_de_bi_dia_a_dia_",
        "ferramenta_etl_data_engineer": "ferramentas_etl_de_",
        "ferramenta_etl_data_analyst": "ferramentas_etl_da_",
        # Só existe em 2025-2026 ("linguagem usada no dia a dia" virou
        # "linguagem preferida" — conceito diferente do grupo acima).
        "linguagem_preferida_2025_2026": "linguagem_preferida_",
    }
    partes = desmancha_grupos_seguro(df, grupos, dimensoes=["edicao"])
    partes = [p.filter(F.col("elegiveis") > 0) for p in partes]
    return uniao(partes)


def gold_05_adocao_ia(df: DataFrame) -> DataFrame:
    partes = []

    if "ai_generativa_e_llm_e_uma_prioridade" in df.columns:
        sub = distribuicao_categorica(df, ["ai_generativa_e_llm_e_uma_prioridade"], dimensoes=["edicao"])
        sub = sub.withColumnRenamed("variavel", "variavel_original").withColumn(
            "categoria", F.lit("ia_generativa_prioridade_na_empresa")
        )
        partes.append(sub)

    grupos = {
        "uso_pessoal_chatgpt_copilot": "usa_chatgpt_ou_copilot_no_trabalho_",
        "tipo_de_uso_de_ia_na_empresa": "tipo_de_uso_de_ai_generativa_e_llm_na_empresa_",
        "motivos_para_nao_usar_ia": "motivos_para_nao_usar_ai_generativa_e_llm_",
    }
    partes.extend(desmancha_grupos_seguro(df, grupos, dimensoes=["edicao"]))
    return uniao(partes)


def gold_06_diferencas_regiao_senioridade_modelo(df: DataFrame) -> DataFrame:
    dimensoes = ["edicao", "regiao_onde_mora", "nivel", "modelo_de_trabalho_atual"]
    dimensoes = [c for c in dimensoes if c in df.columns]
    return distribuicao_categorica(df, ["faixa_salarial"], dimensoes=dimensoes)


def gold_07_oportunidades_desafios(df: DataFrame) -> DataFrame:
    grupos = {
        "desafios_como_gestor": "desafios_como_gestor_",
        "motivo_insatisfacao_profissional": "motivo_insatisfacao_",
        "criterios_para_escolher_emprego": "criterios_para_escolha_de_emprego_",
        "motivos_para_nao_usar_ia": "motivos_para_nao_usar_ai_generativa_e_llm_",
    }
    partes = desmancha_grupos_seguro(df, grupos, dimensoes=["edicao"])
    return uniao(partes)


TABELAS = {
    "gold_01_estrutura_mercado": gold_01_estrutura_mercado,
    "gold_02_perfis_valorizados": gold_02_perfis_valorizados,
    "gold_03_diversidade_genero": gold_03_diversidade_genero,
    "gold_04_adocao_tecnologias": gold_04_adocao_tecnologias,
    "gold_05_adocao_ia": gold_05_adocao_ia,
    "gold_06_diferencas_regiao_senioridade_modelo": gold_06_diferencas_regiao_senioridade_modelo,
    "gold_07_oportunidades_desafios": gold_07_oportunidades_desafios,
}


def main() -> None:
    spark = cria_spark_session("monta_gold_perguntas_negocio")

    print(f"Lendo tabela do Glue Data Catalog: {TABELA_ORIGEM}")
    df = spark.table(TABELA_ORIGEM)
    if NOME_COLUNA_PARTICAO in df.columns and NOME_COLUNA_PARTICAO != "edicao":
        # gold_XX abaixo usam "edicao" como nome da dimensão.
        df = df.withColumnRenamed(NOME_COLUNA_PARTICAO, "edicao")

    for nome_tabela, funcao in TABELAS.items():
        print(f"\nGerando {nome_tabela} ...")
        tabela = funcao(df)
        destino = caminho_gold_pergunta_negocio(nome_tabela)
        tabela.coalesce(1).write.mode("overwrite").option("header", True).option("encoding", "UTF-8").csv(destino)
        print(f"  {tabela.count()} linhas -> {destino}")

        print(f"  Catalogando {DATABASE}.{nome_tabela} no Glue Data Catalog...")
        cataloga_tabela_simples(
            database=DATABASE,
            tabela=nome_tabela,
            colunas=tabela.columns,
            localizacao=destino,
        )

    spark.stop()


if __name__ == "__main__":
    main()
