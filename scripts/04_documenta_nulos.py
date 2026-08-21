"""
Etapa local (documentação, não altera Silver) — Dicionário de nulos.

Decisão do projeto: os valores nulos NÃO são preenchidos no Silver. Na
pesquisa State of Data Brazil, a maior parte dos nulos é estrutural
(lógica condicional do formulário — "skip logic"), não dado faltante:

  - Colunas binárias de multi-select (0/1): NaN significa que a
    pergunta-pai nem foi exibida para o respondente (não "não
    selecionou"). Ex: perguntas só respondidas por quem é gestor.
  - Colunas categóricas de escolha única: o % de nulo indica o quão
    condicional a pergunta é ao perfil do respondente. Perguntas
    universais (idade, gênero, nível de ensino) têm ~0% de nulo;
    perguntas condicionais (cargo como gestor, tempo buscando
    oportunidade) têm nulo alto porque só se aplicam a um subconjunto.

Este script não trata/preenche nulo nenhum — gera um DICIONÁRIO
(CSV) documentando, para cada coluna de cada edição:
  - tipo: "binaria_multiselect" ou "categorica_escolha_unica"
  - pct_nulo: percentual de linhas nulas
  - classificacao: heurística de leitura do nulo, para orientar quem for
    escrever consultas na camada Gold (ex: calcular "% de gestores que
    fazem X" sobre a subpopulação elegível, não sobre a base toda)

Classificação (heurística, baseada só no % de nulo — não substitui
revisão manual do questionário original quando houver dúvida):
  - "quase_universal"      (pct_nulo < 10%):  nulo = não-resposta genuína
  - "condicional_ao_perfil" (pct_nulo >= 10%): nulo = pergunta não aplicável
                                                 a esse respondente

Uso:
    python scripts/04_documenta_nulos.py
"""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVOS_SILVER = {
    "2023-2024": BASE_DIR / "Silver" / "2023-2024" / "state-of-data-brazil-2023-2024_colunas_limpas.csv",
    "2024-2025": BASE_DIR / "Silver" / "2024-2025" / "state-of-data-brazil-2024-2025_colunas_limpas.csv",
    "2025-2026": BASE_DIR / "Silver" / "2025-2026" / "state-of-data-brazil-2025-2026_colunas_limpas.csv",
}
ARQUIVO_SAIDA = BASE_DIR / "Silver" / "_documentacao" / "dicionario_nulos.csv"

LIMIAR_QUASE_UNIVERSAL = 10.0  # % de nulo abaixo do qual consideramos "não-resposta genuína"

VALORES_BINARIOS_VALIDOS = {"0", "1"}


def classifica_tipo(df: pd.DataFrame, coluna: str) -> str:
    valores = set(df[coluna].dropna().unique())
    if valores and valores.issubset(VALORES_BINARIOS_VALIDOS):
        return "binaria_multiselect"
    return "categorica_escolha_unica"


def classifica_semantica_nulo(tipo: str, pct_nulo: float) -> str:
    if tipo == "binaria_multiselect":
        return "grupo_nao_exibido_ao_respondente"
    return "quase_universal" if pct_nulo < LIMIAR_QUASE_UNIVERSAL else "condicional_ao_perfil"


def main() -> None:
    linhas = []

    for edicao, arquivo in ARQUIVOS_SILVER.items():
        print(f"Lendo: {arquivo}")
        df = pd.read_csv(arquivo, dtype=str, low_memory=False)
        pct_nulo = df.isna().mean() * 100

        for coluna in df.columns:
            tipo = classifica_tipo(df, coluna)
            pct = round(float(pct_nulo[coluna]), 1)
            linhas.append(
                {
                    "edicao": edicao,
                    "coluna": coluna,
                    "tipo": tipo,
                    "pct_nulo": pct,
                    "classificacao_nulo": classifica_semantica_nulo(tipo, pct),
                }
            )

    dicionario = pd.DataFrame(linhas).sort_values(["edicao", "pct_nulo"], ascending=[True, False])

    ARQUIVO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
    dicionario.to_csv(ARQUIVO_SAIDA, index=False)

    print(f"\nTotal de linhas no dicionário: {len(dicionario)}")
    print(dicionario["classificacao_nulo"].value_counts())
    print(f"\nDicionário gravado em: {ARQUIVO_SAIDA}")


if __name__ == "__main__":
    main()
