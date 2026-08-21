"""
Etapa local (Bronze -> Silver) — Padronização do header da edição 2024-2025.

O CSV bruto dessa edição prefixa cada coluna com um código de pergunta,
ex: "1.a_idade", "2.l.1_Remuneração/Salário". Além disso, várias perguntas
de múltipla escolha (checkbox) vêm em dois formatos redundantes:

  - uma coluna "pai" com o texto concatenado das opções marcadas
    (ex: "2.l_motivo_insatisfacao" = "Benefícios, Falta de crescimento")
  - várias colunas "filhas" binárias (0/1), uma por opção
    (ex: "2.l.1_Remuneração/Salário" = 1, "2.l.2_Benefícios" = 0, ...)

Este script:

  1. Detecta automaticamente grupos pai/filhas: uma coluna X.y é "pai" de
     X.y.N quando X.y existe como coluna E as colunas X.y.N têm valores
     estritamente binários (0/1).

  2. Detecta grupos "alias" — dois grupos pai/filhas diferentes que
     descrevem exatamente a mesma pergunta de negócio, mas são
     preenchidos por públicos mutuamente exclusivos do questionário
     (skip logic). Confirmado nos dados: "3.f_tipo_de_uso_de_ai_..."
     (respondida só por quem é gestor) e "4.l_tipo_de_uso_de_ai_..."
     (respondida só por quem não é gestor) nunca são preenchidas juntas
     e têm exatamente as mesmas 8 opções, com texto idêntico. Grupos
     alias são unificados (coalesce) em um único conjunto de colunas,
     evitando colunas espelhadas majoritariamente nulas.

  3. Renomeia cada coluna final para "{descrição da pergunta-pai}_
     {descrição da opção}", preservando o contexto da pergunta (evita
     colisão entre opções repetidas em perguntas diferentes, ex:
     "Remuneração/Salário" usada tanto em motivo de insatisfação quanto
     em critérios de escolha de emprego).

  4. Descarta as colunas-pai (texto concatenado) — a informação já está
     nas colunas binárias.

  5. Para colunas fora de qualquer grupo multi-select (perguntas de
     escolha única, ex: "1.a_idade"), apenas remove o prefixo numérico
     do código, mantendo a descrição.

  6. Aplica um dicionário de correções manuais para nomes com erro de
     digitação no CSV de origem, se necessário.

  7. Padroniza a sintaxe de todo nome final de coluna: minúsculo, sem
     acento, "/" e espaços viram "_".

LÊ o Bronze (sem alterá-lo) e ESCREVE o resultado em Silver.

Uso:
    python scripts/02_padroniza_colunas_2024_2025.py
"""

from dataclasses import dataclass, field
import re
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_ORIGINAL = BASE_DIR / "Bronze" / "2024-2025" / "state-of-data-brazil-2024-2025.csv"
ARQUIVO_LIMPO = BASE_DIR / "Silver" / "2024-2025" / "state-of-data-brazil-2024-2025_colunas_limpas.csv"

# Separa o prefixo de código da pergunta do texto legível.
# "2.l.1_Remuneração/Salário" -> prefixo="2.l.1", descricao="Remuneração/Salário"
PADRAO_PREFIXO = re.compile(r"^(\d+(?:\.[A-Za-z0-9]+)*)[_ ]+(.*)$")

VALORES_BINARIOS_VALIDOS = {"0", "1"}

# Correções pontuais para nomes com erro de digitação no CSV de origem.
# Chave: nome original completo da coluna, como vem no header.
CORRECOES_MANUAIS: dict[str, str] = {}

# Grupos que descrevem a mesma pergunta de negócio mas são roteados a
# públicos mutuamente exclusivos do formulário (confirmado nos dados
# antes de codar isso: preenchimento nunca se sobrepõe e as opções são
# idênticas). Cada tupla é o prefixo-raiz do grupo (ex: "3.f", "4.l");
# o primeiro da lista tem prioridade no coalesce em caso de conflito
# (não deveria haver conflito, já que são mutuamente exclusivos).
GRUPOS_ALIAS: list[tuple[str, ...]] = [
    ("3.f", "4.l"),  # tipo de uso de IA generativa na empresa: gestor vs. não-gestor
]


@dataclass
class GrupoMultiselect:
    raizes: list[str]  # prefixos-raiz que compõem o grupo (>1 só se for alias)
    descricao_pai: str
    colunas_pai: list[str]
    # descricao_opcao -> lista de colunas originais (uma por raiz) que representam essa opção
    opcoes: dict[str, list[str]] = field(default_factory=dict)


def parseia_coluna(nome_original: str) -> tuple[Optional[str], str]:
    """Retorna (prefixo, descricao) de uma coluna. Prefixo é None se não bater com o padrão."""
    nome = nome_original.strip()
    match = PADRAO_PREFIXO.match(nome)
    if not match:
        return None, nome
    return match.group(1), match.group(2).strip()


def coluna_e_binaria(df: pd.DataFrame, coluna: str) -> bool:
    valores = set(df[coluna].dropna().unique())
    return valores.issubset(VALORES_BINARIOS_VALIDOS)


def normaliza_nome_coluna(nome: str) -> str:
    """Padroniza a sintaxe final do nome de coluna: minúsculo, sem acento,
    "/" e espaços viram "_", remove pontuação (, ? ( )), sem underscores
    duplicados nas pontas."""
    nome = nome.replace("/", "_")
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(ch for ch in nome if not unicodedata.combining(ch))
    nome = nome.lower()
    nome = re.sub(r"[,?()]", "", nome)
    nome = re.sub(r"\s+", "_", nome.strip())
    nome = re.sub(r"_+", "_", nome)
    return nome.strip("_")


def identifica_grupos_base(df: pd.DataFrame, parsed: dict[str, tuple[Optional[str], str]]):
    """Identifica, por prefixo-raiz, os grupos pai/filhas de multi-select.

    Retorna dict: raiz -> GrupoMultiselect (com uma única raiz cada, antes do coalesce de alias).
    """
    prefixo_para_col = {p: c for c, (p, _) in parsed.items() if p}
    grupos: dict[str, GrupoMultiselect] = {}

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
        if not coluna_e_binaria(df, col):
            continue  # filha não é binária -> não é dummy de multi-select (ex: 1.a.1_faixa_idade)

        _, desc_pai = parsed[pai_col]
        grupo = grupos.setdefault(
            root, GrupoMultiselect(raizes=[root], descricao_pai=desc_pai, colunas_pai=[pai_col])
        )
        grupo.opcoes.setdefault(desc, []).append(col)

    return grupos


def aplica_coalesce_alias(grupos: dict[str, GrupoMultiselect]) -> list[GrupoMultiselect]:
    """Funde grupos declarados em GRUPOS_ALIAS em um único GrupoMultiselect."""
    raizes_em_alias = {r for combo in GRUPOS_ALIAS for r in combo}
    resultado = [g for raiz, g in grupos.items() if raiz not in raizes_em_alias]

    for combo in GRUPOS_ALIAS:
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


def constroi_dataframe_final(df: pd.DataFrame, grupos: list[GrupoMultiselect], parsed):
    colunas_pai_para_remover = {c for g in grupos for c in g.colunas_pai}
    colunas_em_grupo = {c for g in grupos for cols in g.opcoes.values() for c in cols}

    novas_colunas: dict[str, pd.Series] = {}
    nomes_gerados: list[str] = []

    # 1) Colunas de grupos multi-select (com coalesce quando houver mais de uma coluna por opção)
    for g in grupos:
        for desc_opcao, cols_originais in g.opcoes.items():
            nome_final = normaliza_nome_coluna(f"{g.descricao_pai}_{desc_opcao}")
            serie = df[cols_originais[0]]
            for outra in cols_originais[1:]:
                serie = serie.combine_first(df[outra])
            novas_colunas[nome_final] = serie
            nomes_gerados.append(nome_final)

    # 2) Colunas fora de qualquer grupo (perguntas de escolha única)
    for col in df.columns:
        if col in colunas_pai_para_remover or col in colunas_em_grupo:
            continue
        if col in CORRECOES_MANUAIS:
            nome_final = normaliza_nome_coluna(CORRECOES_MANUAIS[col])
        else:
            _, desc = parsed[col]
            nome_final = normaliza_nome_coluna(desc)
        novas_colunas[nome_final] = df[col]
        nomes_gerados.append(nome_final)

    # Checagem de colisão residual (não deveria ocorrer; se ocorrer, avisa em vez de sobrescrever)
    vistos = set()
    colisoes = [n for n in nomes_gerados if (n in vistos or vistos.add(n))]
    if colisoes:
        raise ValueError(f"Colisão de nomes de coluna não resolvida: {sorted(set(colisoes))}")

    return pd.DataFrame(novas_colunas)


def main() -> None:
    print(f"Lendo: {ARQUIVO_ORIGINAL}")
    df = pd.read_csv(ARQUIVO_ORIGINAL, dtype=str, low_memory=False)

    colunas_originais = df.columns.tolist()
    parsed = {c: parseia_coluna(c) for c in colunas_originais}

    grupos_base = identifica_grupos_base(df, parsed)
    grupos = aplica_coalesce_alias(grupos_base)

    n_alias = sum(1 for g in grupos if len(g.raizes) > 1)
    print(f"Grupos multi-select detectados: {len(grupos)} (dos quais {n_alias} unificados por alias)")
    print(f"Colunas-pai removidas: {sum(len(g.colunas_pai) for g in grupos)}")

    df_final = constroi_dataframe_final(df, grupos, parsed)

    ARQUIVO_LIMPO.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(ARQUIVO_LIMPO, index=False)
    print(f"\nColunas originais: {len(colunas_originais)} | Colunas finais: {len(df_final.columns)}")
    print(f"Arquivo gravado em Silver: {ARQUIVO_LIMPO}")


if __name__ == "__main__":
    main()
