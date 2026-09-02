"""
Etapa PySpark (Silver "state_of_data_silver" -> Gold por pergunta de negócio) — State of Data Brazil.

Lê a tabela Silver catalogada db_state_of_data.state_of_data_silver
(schema harmonizado, gravada E catalogada pelo script 06, um respondente
por linha, as 3 edições como partições) diretamente do Glue Data Catalog
— dentro de um Glue Job a sessão Spark já enxerga o catálogo como Hive
metastore, então `spark.table("db_state_of_data.state_of_data_silver")`
funciona sem nenhuma configuração extra. Gera uma tabela Gold pequena e
pré-agregada para cada pergunta de negócio do desafio:

  1. gold_01_estrutura_mercado
     Como está estruturado o mercado brasileiro de Dados?

  2. gold_02_perfis_valorizados
     Quais perfis profissionais são mais valorizados pelo mercado?

  3. gold_03_diversidade_genero
     Qual é o cenário de diversidade de gênero nas carreiras de dados?

  4. gold_04_adocao_tecnologias
     Quais tecnologias apresentam maior adoção entre os profissionais?

  5. gold_05_adocao_ia
     Qual é o índice de adoção de IA e seu impacto?

  6. gold_06_diferencas_regiao_senioridade_modelo
     Existem diferenças relevantes entre regiões, senioridades ou
     modelos de trabalho?

  7. gold_07_oportunidades_desafios
     Quais oportunidades e desafios podem ser identificados para
     empresas que desejam investir em Dados e IA?

Todas as tabelas de multi-select calculam "% de adoção" sobre a
população ELEGÍVEL (quem respondeu aquele grupo de pergunta), não sobre
a base toda — ver Silver/_documentacao/dicionario_nulos.csv sobre por que
isso importa (nulo em grupo multi-select = pergunta não exibida, não
"não selecionou").

Cada tabela final é pequena (dezenas a poucas centenas de linhas — já é
uma AGREGAÇÃO), então é gravada com `coalesce(1)` para sair como um único
arquivo `part-*.csv` dentro do diretório, mais fácil de abrir/conferir.

Uso (dentro de um Glue Job — fora do Glue, rodar isso exige uma
SparkSession local configurada para enxergar o Glue Data Catalog como
Hive metastore, o que não foi testado neste ambiente):
    python scripts/07_monta_gold_perguntas_negocio.py
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from _config_aws import NOME_COLUNA_PARTICAO, caminho_gold_pergunta_negocio, tabela_qualificada
from _lib_padroniza_colunas import col_seguro, cria_spark_session

TABELA_ORIGEM = tabela_qualificada()


def uniao(dfs: list) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def distribuicao_categorica(df: DataFrame, colunas: list, dimensoes: list = ("edicao",)) -> DataFrame:
    """Contagem e % (dentro de cada combinação de `dimensoes`, calculado
    separadamente por coluna categórica) para uma ou mais colunas.

    Retorna formato longo: dimensoes..., variavel, valor, contagem, total_respondentes, pct_na_dimensao.
    Linhas nulas na coluna categórica são ignoradas (não fazem parte da distribuição).
    """
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
    """"Desmancha" um grupo de colunas binárias (0/1) de multi-select em formato longo.

    % de adoção calculado sobre a população ELEGÍVEL (quem tem pelo menos
    uma coluna não nula no grupo, dentro de cada combinação de `dimensoes`),
    não sobre a base toda.

    Retorna: dimensoes..., opcao, elegiveis, selecionaram, pct_adocao.
    """
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
        # Só existe em 2025-2026 (a pergunta "linguagem usada no dia a dia" virou
        # "linguagem preferida" nessa edição — conceito diferente, não é o mesmo
        # grupo de "linguagem_de_programacao_dia_a_dia" acima; ver dicionário de
        # correspondência para o porquê da separação).
        "linguagem_preferida_2025_2026": "linguagem_preferida_",
    }
    partes = []
    for categoria, prefixo in grupos.items():
        sub = desmancha_grupo_multiselect(df, prefixo, dimensoes=["edicao"])
        sub = sub.filter(F.col("elegiveis") > 0)  # descarta edições onde o grupo nem existe
        sub = sub.withColumn("categoria", F.lit(categoria))
        partes.append(sub)
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
    for categoria, prefixo in grupos.items():
        sub = desmancha_grupo_multiselect(df, prefixo, dimensoes=["edicao"])
        sub = sub.withColumn("categoria", F.lit(categoria))
        partes.append(sub)

    # Mistura de esquemas de propósito (distribuicao_categorica x desmancha
    # multiselect) — colunas que só existem num dos dois ficam nulas no
    # outro (allowMissingColumns=True em uniao()), igual fazia o pd.concat.
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
    partes = []
    for categoria, prefixo in grupos.items():
        sub = desmancha_grupo_multiselect(df, prefixo, dimensoes=["edicao"])
        sub = sub.withColumn("categoria", F.lit(categoria))
        partes.append(sub)
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
        # As funções gold_XX abaixo usam "edicao" como nome da dimensão de
        # edição — renomeia a coluna de partição (vem do catálogo com o
        # nome configurado em _config_aws.NOME_COLUNA_PARTICAO) para não
        # precisar mexer em cada função.
        df = df.withColumnRenamed(NOME_COLUNA_PARTICAO, "edicao")

    for nome_tabela, funcao in TABELAS.items():
        print(f"\nGerando {nome_tabela} ...")
        tabela = funcao(df)
        destino = caminho_gold_pergunta_negocio(nome_tabela)
        tabela.coalesce(1).write.mode("overwrite").option("header", True).option("encoding", "UTF-8").csv(destino)
        print(f"  {tabela.count()} linhas -> {destino}")

    spark.stop()


if __name__ == "__main__":
    main()
