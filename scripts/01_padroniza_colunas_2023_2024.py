"""
Etapa local (Bronze -> Silver) — Padronização do header da edição 2023-2024.

O CSV bruto dessa edição traz os nomes de coluna como string de tupla
Python, ex: "('P1_a ', 'Idade')" (código, descrição). Diferente das
edições seguintes (2024-2025 e 2025-2026), o código usa "_" como
separador de segmento (ex: "P1_e_1") em vez de ".". Fora essa diferença
de formato, o mesmo padrão de dados se repete aqui:

  - uma coluna "pai" com o texto concatenado das opções marcadas
    (ex: código "P1_e", descrição "experiencia_profissional_prejudicada")
  - várias colunas "filhas" binárias (0/1), uma por opção
    (ex: "P1_e_1", "P1_e_2", ...)

Este script (lógica de agrupamento compartilhada em _lib_padroniza_colunas.py):

  1. Extrai (código, descrição) de cada coluna a partir da tupla,
     aplicando a correção manual conhecida (P6_b_16, tupla malformada
     no CSV de origem).

  2. Converte o código para o formato com "." (ex: "P1_e_1" -> "P1.e.1")
     só para reaproveitar a mesma lógica de detecção de grupos usada nas
     outras edições.

  3. Detecta grupos pai/filhas multi-select (mesma regra: pai existe como
     coluna E filhas têm valores estritamente binários).

  4. Detecta o grupo "alias" P3_f / P4_l — mesma pergunta sobre tipo de
     uso de IA generativa, respondida por públicos mutuamente exclusivos
     (gestor vs. não-gestor), confirmado nos dados (0 sobreposição). O
     coalesce funde opção a opção por texto EXATO: das 8 opções, 7 têm
     texto idêntico e são unificadas; a opção "AI Generativa e LLMs para
     melhorar produtos externos" (P3_f_4) tem texto diferente de
     "...para os clientes finais" (P4_l_4) e é mantida como duas colunas
     separadas (decisão manual — não presumir que é a mesma opção só
     pela posição no índice).

  5. Renomeia cada coluna final para "{descrição da pergunta-pai}_
     {descrição da opção}" nos grupos multi-select; remove as colunas-pai
     (texto concatenado); para colunas fora de grupo, mantém a descrição
     limpa da tupla.

LÊ o Bronze (sem alterá-lo) e ESCREVE o resultado em Silver.

Uso:
    python scripts/01_padroniza_colunas_2023_2024.py
"""

import re
from pathlib import Path

import pandas as pd

from _lib_padroniza_colunas import (
    aplica_coalesce_alias,
    constroi_dataframe_final,
    identifica_grupos_base,
)

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_ORIGINAL = BASE_DIR / "Bronze" / "2023-2024" / "state-of-data-brazil-2023-2024.csv"
ARQUIVO_LIMPO = BASE_DIR / "Silver" / "2023-2024" / "state-of-data-brazil-2023-2024_colunas_limpas.csv"

# Captura (codigo, descricao) da tupla: "('P1_a ', 'Idade')" -> ("P1_a", "Idade")
PADRAO_TUPLA = re.compile(r"^\(\s*'(.*?)'\s*,\s*'(.*?)'\s*\)$")

# Correção pontual: tupla malformada no CSV de origem (falta fechar aspa/parêntese).
CORRECOES_TUPLA_MALFORMADA = {
    "('P6_b_16 ', 'SQL Server Integration Services (SSIS))": ("P6_b_16", "SQL Server Integration Services (SSIS)"),
}

# Grupos que descrevem a mesma pergunta de negócio mas são roteados a
# públicos mutuamente exclusivos do formulário (confirmado nos dados:
# preenchimento nunca se sobrepõe). Notação com "." para reaproveitar a
# lógica compartilhada. O coalesce só funde as opções com texto EXATO
# igual entre os dois grupos — ver nota no docstring sobre P3_f_4/P4_l_4.
GRUPOS_ALIAS: list[tuple[str, ...]] = [
    ("P3.f", "P4.l"),  # tipo de uso de IA generativa na empresa: gestor vs. não-gestor
]


def parseia_coluna_tupla(nome_original: str) -> tuple:
    """Extrai (codigo, descricao) de uma coluna no formato tupla Python."""
    if nome_original in CORRECOES_TUPLA_MALFORMADA:
        return CORRECOES_TUPLA_MALFORMADA[nome_original]

    match = PADRAO_TUPLA.match(nome_original.strip())
    if not match:
        return None, nome_original  # mantém original se não bater (não deveria ocorrer)
    return match.group(1).strip(), match.group(2).strip()


def main() -> None:
    print(f"Lendo: {ARQUIVO_ORIGINAL}")
    df = pd.read_csv(ARQUIVO_ORIGINAL, dtype=str, low_memory=False)

    colunas_originais = df.columns.tolist()

    # parsed no formato esperado pela lib: coluna_original -> (prefixo_com_pontos, descricao)
    parsed: dict[str, tuple] = {}
    for col in colunas_originais:
        codigo, descricao = parseia_coluna_tupla(col)
        prefixo = codigo.replace("_", ".") if codigo else None
        parsed[col] = (prefixo, descricao)

    grupos_base = identifica_grupos_base(df, parsed)
    grupos = aplica_coalesce_alias(grupos_base, GRUPOS_ALIAS)

    n_alias = sum(1 for g in grupos if len(g.raizes) > 1)
    print(f"Grupos multi-select detectados: {len(grupos)} (dos quais {n_alias} unificados por alias)")
    print(f"Colunas-pai removidas: {sum(len(g.colunas_pai) for g in grupos)}")

    df_final = constroi_dataframe_final(df, grupos, parsed, correcoes_manuais={})

    ARQUIVO_LIMPO.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(ARQUIVO_LIMPO, index=False)
    print(f"\nColunas originais: {len(colunas_originais)} | Colunas finais: {len(df_final.columns)}")
    print(f"Arquivo gravado em Silver: {ARQUIVO_LIMPO}")


if __name__ == "__main__":
    main()
