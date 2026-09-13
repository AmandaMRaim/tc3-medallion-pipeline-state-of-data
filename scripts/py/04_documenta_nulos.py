"""
Etapa PySpark (documentação, não altera Silver) — Dicionário de nulos.

Não preenche nenhum nulo — só documenta, por coluna/edição, se o nulo é
estrutural (grupo multi-select não exibido, "skip logic" do formulário)
ou não-resposta genuína (pct_nulo < 10%, heurística). Importa pra quem
for escrever consultas na Gold: ex. "% de gestores que fazem X" precisa
ser calculado sobre a subpopulação elegível, não a base toda.
"""

import pandas as pd
from pyspark.sql import functions as F

from _config_aws import EDICOES, caminho_documentacao, caminho_silver_staging_por_edicao
from _lib_padroniza_colunas import col_seguro, cria_spark_session

DIRETORIOS_SILVER = {edicao: caminho_silver_staging_por_edicao(edicao) for edicao in EDICOES}
ARQUIVO_SAIDA = caminho_documentacao("dicionario_nulos.csv")

LIMIAR_QUASE_UNIVERSAL = 10.0
VALORES_BINARIOS_VALIDOS = {"0", "1"}


def classifica_semantica_nulo(tipo: str, pct_nulo: float) -> str:
    if tipo == "binaria_multiselect":
        return "grupo_nao_exibido_ao_respondente"
    return "quase_universal" if pct_nulo < LIMIAR_QUASE_UNIVERSAL else "condicional_ao_perfil"


def calcula_estatisticas_edicao(spark, edicao: str, diretorio: str) -> pd.DataFrame:
    print(f"Lendo Silver {edicao}: {diretorio}")
    df = spark.read.option("header", True).csv(str(diretorio))
    colunas = df.columns
    total_linhas = df.count()

    # Uma agregação só: conta nulos + coleta valores distintos por coluna.
    # Alias posicional evita problema com caractere especial no nome.
    apelidos = {f"col_{i}": c for i, c in enumerate(colunas)}
    agregacoes = []
    for apelido, c in apelidos.items():
        agregacoes.append(F.sum(col_seguro(c).isNull().cast("long")).alias(f"nulos__{apelido}"))
        agregacoes.append(F.slice(F.collect_set(col_seguro(c)), 1, 200).alias(f"valores__{apelido}"))
    linha = df.agg(*agregacoes).first()

    registros = []
    for apelido, c in apelidos.items():
        n_nulos = linha[f"nulos__{apelido}"]
        pct_nulo = round((n_nulos / total_linhas) * 100, 1) if total_linhas else 0.0
        valores = set(linha[f"valores__{apelido}"]) if linha[f"valores__{apelido}"] else set()
        tipo = "binaria_multiselect" if valores and valores.issubset(VALORES_BINARIOS_VALIDOS) else "categorica_escolha_unica"
        registros.append(
            {
                "edicao": edicao,
                "coluna": c,
                "tipo": tipo,
                "pct_nulo": pct_nulo,
                "classificacao_nulo": classifica_semantica_nulo(tipo, pct_nulo),
            }
        )

    return pd.DataFrame(registros)


def main() -> None:
    spark = cria_spark_session("documenta_nulos")

    blocos = [calcula_estatisticas_edicao(spark, edicao, diretorio) for edicao, diretorio in DIRETORIOS_SILVER.items()]
    dicionario = pd.concat(blocos, ignore_index=True).sort_values(["edicao", "pct_nulo"], ascending=[True, False])

    dicionario.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8")

    print(f"\nTotal de linhas no dicionário: {len(dicionario)}")
    print(dicionario["classificacao_nulo"].value_counts())
    print(f"\nDicionário gravado em: {ARQUIVO_SAIDA}")

    spark.stop()


if __name__ == "__main__":
    main()
