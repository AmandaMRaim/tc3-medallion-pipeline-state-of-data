"""
Etapa local (Gold, base -> Gold por pergunta de negócio) — State of Data Brazil.

Parte de Gold/state_of_data_unificado.csv (base longa, um respondente por
linha, as 3 edições unificadas — ver script 06) e gera uma tabela Gold
pequena e pré-agregada para cada pergunta de negócio do desafio:

  1. gold_01_estrutura_mercado.csv
     Como está estruturado o mercado brasileiro de Dados?
     (cargo, senioridade, situação de trabalho, modelo de trabalho,
     região, setor, porte de empresa — contagem e % por edição)

  2. gold_02_perfis_valorizados.csv
     Quais perfis profissionais são mais valorizados pelo mercado?
     (cargo x senioridade x faixa salarial — contagem e % por edição)

  3. gold_03_diversidade_genero.csv
     Qual é o cenário de diversidade de gênero nas carreiras de dados?
     (gênero x cargo x senioridade x faixa salarial x cor/raça/etnia)

  4. gold_04_adocao_tecnologias.csv
     Quais tecnologias apresentam maior adoção entre os profissionais?
     (linguagens, bancos de dados, cloud, ferramentas de BI e ETL —
     % de adoção sobre a população elegível, por edição)

  5. gold_05_adocao_ia.csv
     Qual é o índice de adoção de IA e seu impacto?
     (prioridade de IA generativa na empresa, uso de ChatGPT/Copilot,
     tipo de uso de IA na empresa, motivos para não usar)

  6. gold_06_diferencas_regiao_senioridade_modelo.csv
     Existem diferenças relevantes entre regiões, senioridades ou
     modelos de trabalho? (faixa salarial cruzada com região x
     senioridade x modelo de trabalho)

  7. gold_07_oportunidades_desafios.csv
     Quais oportunidades e desafios podem ser identificados para
     empresas que desejam investir em Dados e IA? (desafios como
     gestor, motivos de insatisfação, critérios de escolha de emprego,
     motivos para não usar IA — sinais do lado de quem contrata/retém)

Todas as tabelas de multi-select calculam "% de adoção" sobre a
população ELEGÍVEL (quem respondeu aquele grupo de pergunta), não sobre
a base toda — ver Silver/_documentacao/dicionario_nulos.csv sobre por que
isso importa (nulo em grupo multi-select = pergunta não exibida, não
"não selecionou").

Uso:
    python scripts/07_monta_gold_perguntas_negocio.py
"""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_BASE = BASE_DIR / "Gold" / "state_of_data_unificado.csv"
DIR_SAIDA = BASE_DIR / "Gold" / "perguntas_negocio"


def carrega_base() -> pd.DataFrame:
    print(f"Lendo base unificada: {ARQUIVO_BASE}")
    return pd.read_csv(ARQUIVO_BASE, dtype=str, low_memory=False)


def distribuicao_categorica(df: pd.DataFrame, colunas: list[str], dimensoes: list[str] = ("edicao",)) -> pd.DataFrame:
    """Contagem e % (dentro de cada combinação de `dimensoes`) para uma ou mais colunas categóricas.

    Retorna formato longo: dimensoes..., variavel, valor, contagem, pct_na_dimensao.
    Linhas nulas na coluna categórica são ignoradas (não fazem parte da distribuição).
    """
    partes = []
    for coluna in colunas:
        sub = df[[*dimensoes, coluna]].dropna(subset=[coluna]).copy()
        sub = sub.rename(columns={coluna: "valor"})
        sub["variavel"] = coluna

        contagem = sub.groupby([*dimensoes, "variavel", "valor"]).size().reset_index(name="contagem")
        total_dimensao = sub.groupby(list(dimensoes)).size().reset_index(name="total_respondentes")
        contagem = contagem.merge(total_dimensao, on=list(dimensoes))
        contagem["pct_na_dimensao"] = (contagem["contagem"] / contagem["total_respondentes"] * 100).round(1)
        partes.append(contagem)

    return pd.concat(partes, ignore_index=True)


def desmancha_grupo_multiselect(df: pd.DataFrame, prefixo: str, dimensoes: list[str] = ("edicao",)) -> pd.DataFrame:
    """"Desmancha" um grupo de colunas binárias (0/1) de multi-select em formato longo.

    % de adoção calculado sobre a população ELEGÍVEL (quem tem pelo menos
    uma coluna não nula no grupo, dentro de cada combinação de `dimensoes`),
    não sobre a base toda.

    Retorna: dimensoes..., opcao, elegiveis, selecionaram, pct_adocao.
    """
    colunas_grupo = [c for c in df.columns if c.startswith(prefixo)]
    if not colunas_grupo:
        raise ValueError(f"Nenhuma coluna encontrada com o prefixo '{prefixo}'")

    elegivel = df[colunas_grupo].notna().any(axis=1)
    base_elegivel = df.loc[elegivel, [*dimensoes, *colunas_grupo]].copy()

    elegiveis_por_dimensao = base_elegivel.groupby(list(dimensoes)).size().reset_index(name="elegiveis")

    linhas = []
    for coluna in colunas_grupo:
        opcao = coluna[len(prefixo):]
        numerica = pd.to_numeric(base_elegivel[coluna], errors="coerce").fillna(0)
        tmp = base_elegivel[list(dimensoes)].copy()
        tmp["selecionou"] = numerica
        agrupado = tmp.groupby(list(dimensoes))["selecionou"].sum().reset_index(name="selecionaram")
        agrupado["opcao"] = opcao
        linhas.append(agrupado)

    resultado = pd.concat(linhas, ignore_index=True)
    resultado = resultado.merge(elegiveis_por_dimensao, on=list(dimensoes))
    resultado["pct_adocao"] = (resultado["selecionaram"] / resultado["elegiveis"] * 100).round(1)
    colunas_ordem = [*dimensoes, "opcao", "elegiveis", "selecionaram", "pct_adocao"]
    return resultado[colunas_ordem].sort_values([*dimensoes, "pct_adocao"], ascending=[True] * len(dimensoes) + [False])


def gold_01_estrutura_mercado(df: pd.DataFrame) -> pd.DataFrame:
    colunas = [
        "cargo_atual", "nivel", "situacao_de_trabalho", "modelo_de_trabalho_atual",
        "regiao_onde_mora", "setor", "numero_de_funcionarios",
    ]
    colunas = [c for c in colunas if c in df.columns]
    return distribuicao_categorica(df, colunas, dimensoes=["edicao"])


def gold_02_perfis_valorizados(df: pd.DataFrame) -> pd.DataFrame:
    dimensoes = ["edicao", "cargo_atual", "nivel"]
    dimensoes = [c for c in dimensoes if c in df.columns]
    return distribuicao_categorica(df, ["faixa_salarial"], dimensoes=dimensoes)


def gold_03_diversidade_genero(df: pd.DataFrame) -> pd.DataFrame:
    dimensoes = ["edicao", "genero"]
    dimensoes = [c for c in dimensoes if c in df.columns]
    colunas = [c for c in ["cargo_atual", "nivel", "faixa_salarial", "cor_raca_etnia"] if c in df.columns]
    return distribuicao_categorica(df, colunas, dimensoes=dimensoes)


def gold_04_adocao_tecnologias(df: pd.DataFrame) -> pd.DataFrame:
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
        sub = sub[sub["elegiveis"] > 0]  # descarta edições onde o grupo nem existe
        sub.insert(0, "categoria", categoria)
        partes.append(sub)
    return pd.concat(partes, ignore_index=True)


def gold_05_adocao_ia(df: pd.DataFrame) -> pd.DataFrame:
    partes = []

    if "ai_generativa_e_llm_e_uma_prioridade" in df.columns:
        sub = distribuicao_categorica(df, ["ai_generativa_e_llm_e_uma_prioridade"], dimensoes=["edicao"])
        sub.insert(0, "categoria", "ia_generativa_prioridade_na_empresa")
        partes.append(sub.rename(columns={"variavel": "variavel_original"}))

    grupos = {
        "uso_pessoal_chatgpt_copilot": "usa_chatgpt_ou_copilot_no_trabalho_",
        "tipo_de_uso_de_ia_na_empresa": "tipo_de_uso_de_ai_generativa_e_llm_na_empresa_",
        "motivos_para_nao_usar_ia": "motivos_para_nao_usar_ai_generativa_e_llm_",
    }
    for categoria, prefixo in grupos.items():
        sub = desmancha_grupo_multiselect(df, prefixo, dimensoes=["edicao"])
        sub.insert(0, "categoria", categoria)
        partes.append(sub)

    return pd.concat(partes, ignore_index=True)


def gold_06_diferencas_regiao_senioridade_modelo(df: pd.DataFrame) -> pd.DataFrame:
    dimensoes = ["edicao", "regiao_onde_mora", "nivel", "modelo_de_trabalho_atual"]
    dimensoes = [c for c in dimensoes if c in df.columns]
    return distribuicao_categorica(df, ["faixa_salarial"], dimensoes=dimensoes)


def gold_07_oportunidades_desafios(df: pd.DataFrame) -> pd.DataFrame:
    grupos = {
        "desafios_como_gestor": "desafios_como_gestor_",
        "motivo_insatisfacao_profissional": "motivo_insatisfacao_",
        "criterios_para_escolher_emprego": "criterios_para_escolha_de_emprego_",
        "motivos_para_nao_usar_ia": "motivos_para_nao_usar_ai_generativa_e_llm_",
    }
    partes = []
    for categoria, prefixo in grupos.items():
        sub = desmancha_grupo_multiselect(df, prefixo, dimensoes=["edicao"])
        sub.insert(0, "categoria", categoria)
        partes.append(sub)
    return pd.concat(partes, ignore_index=True)


TABELAS = {
    "gold_01_estrutura_mercado.csv": gold_01_estrutura_mercado,
    "gold_02_perfis_valorizados.csv": gold_02_perfis_valorizados,
    "gold_03_diversidade_genero.csv": gold_03_diversidade_genero,
    "gold_04_adocao_tecnologias.csv": gold_04_adocao_tecnologias,
    "gold_05_adocao_ia.csv": gold_05_adocao_ia,
    "gold_06_diferencas_regiao_senioridade_modelo.csv": gold_06_diferencas_regiao_senioridade_modelo,
    "gold_07_oportunidades_desafios.csv": gold_07_oportunidades_desafios,
}


def main() -> None:
    df = carrega_base()
    DIR_SAIDA.mkdir(parents=True, exist_ok=True)

    for nome_arquivo, funcao in TABELAS.items():
        print(f"\nGerando {nome_arquivo} ...")
        tabela = funcao(df)
        caminho = DIR_SAIDA / nome_arquivo
        tabela.to_csv(caminho, index=False)
        print(f"  {len(tabela)} linhas -> {caminho}")


if __name__ == "__main__":
    main()
