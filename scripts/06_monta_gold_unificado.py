"""
Etapa local (Silver -> Gold) — Dataset unificado das 3 edições.

Usa o dicionário de correspondência (Silver/_documentacao/
dicionario_correspondencia_colunas.csv, gerado pelo script 05 e revisado
manualmente) para unir 2023-2024 + 2024-2025 + 2025-2026 em uma única
tabela longa (uma linha por respondente, coluna "edicao" identificando a
origem).

Política de confiança — SÓ usa uma correspondência entre edições quando:
  - metodo == "exato" (nomes idênticos), ou
  - metodo == "fuzzy" e confianca == "alta" (score >= 0.85, texto quase
    idêntico), ou
  - status_revisao_2023_2024 == "aprovado_manual" (aprovado na revisão
    manual em chat, mesmo com confiança média/baixa)

Qualquer correspondência marcada "rejeitado_manual", ou fuzzy de
confiança média/baixa AINDA NÃO revisada, fica de fora por enquanto: a
coluna existe na Gold (para não perder o dado das edições que a têm),
mas fica NULA para a edição cuja correspondência não foi validada. Isso
é proposital — mais seguro deixar nulo do que juntar dado errado. A
revisão manual pode ser continuada depois; rodar o script de novo após
atualizar o dicionário incorpora mais colunas automaticamente.

Uso:
    python scripts/06_monta_gold_unificado.py
"""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_DICIONARIO = BASE_DIR / "Silver" / "_documentacao" / "dicionario_correspondencia_colunas.csv"
ARQUIVOS_SILVER = {
    "2023-2024": BASE_DIR / "Silver" / "2023-2024" / "state-of-data-brazil-2023-2024_colunas_limpas.csv",
    "2024-2025": BASE_DIR / "Silver" / "2024-2025" / "state-of-data-brazil-2024-2025_colunas_limpas.csv",
    "2025-2026": BASE_DIR / "Silver" / "2025-2026" / "state-of-data-brazil-2025-2026_colunas_limpas.csv",
}
ARQUIVO_SAIDA = BASE_DIR / "Gold" / "state_of_data_unificado.csv"


def confiavel(metodo: str, confianca: str, status_revisao) -> bool:
    if status_revisao == "rejeitado_manual":
        return False
    if status_revisao == "aprovado_manual":
        return True
    if metodo == "exato":
        return True
    if metodo == "fuzzy" and confianca == "alta":
        return True
    return False


def monta_mapa_colunas(dicionario: pd.DataFrame):
    """Retorna, para cada edição, um dict {nome_coluna_canonico: nome_coluna_na_edicao}."""
    mapa = {"2023-2024": {}, "2024-2025": {}, "2025-2026": {}}

    for _, linha in dicionario.iterrows():
        col_2024_2025 = linha["coluna_2024_2025"]
        col_2023_2024 = linha["coluna_2023_2024"]
        col_2025_2026 = linha["coluna_2025_2026"]

        # Nome canônico: prioriza 2024-2025 (edição-espinha-dorsal); usa a
        # coluna exclusiva quando a linha não tem correspondente em 2024-2025.
        if pd.notna(col_2024_2025):
            canonico = col_2024_2025
            mapa["2024-2025"][canonico] = col_2024_2025

            if confiavel(linha["metodo_2023_2024"], linha["confianca_2023_2024"], linha.get("status_revisao_2023_2024")):
                if pd.notna(col_2023_2024):
                    mapa["2023-2024"][canonico] = col_2023_2024

            if confiavel(linha["metodo_2025_2026"], linha["confianca_2025_2026"], None):
                if pd.notna(col_2025_2026):
                    mapa["2025-2026"][canonico] = col_2025_2026

        elif pd.notna(col_2023_2024):
            canonico = col_2023_2024
            mapa["2023-2024"][canonico] = col_2023_2024
        elif pd.notna(col_2025_2026):
            canonico = col_2025_2026
            mapa["2025-2026"][canonico] = col_2025_2026

    return mapa


def main() -> None:
    print(f"Lendo dicionário: {ARQUIVO_DICIONARIO}")
    dicionario = pd.read_csv(ARQUIVO_DICIONARIO)

    mapa = monta_mapa_colunas(dicionario)
    colunas_canonicas = sorted(set(mapa["2023-2024"]) | set(mapa["2024-2025"]) | set(mapa["2025-2026"]))
    print(f"Total de colunas canônicas na Gold: {len(colunas_canonicas)}")

    partes = []
    for edicao, arquivo in ARQUIVOS_SILVER.items():
        print(f"Lendo Silver {edicao}: {arquivo}")
        df = pd.read_csv(arquivo, dtype=str, low_memory=False)

        dados_colunas = {"edicao": edicao, "linha_origem_silver": df.index}
        n_preenchidas = 0
        for canonico in colunas_canonicas:
            col_original = mapa[edicao].get(canonico)
            if col_original is not None and col_original in df.columns:
                dados_colunas[canonico] = df[col_original]
                n_preenchidas += 1
            else:
                dados_colunas[canonico] = pd.NA
        bloco = pd.DataFrame(dados_colunas, index=df.index)
        print(f"  {edicao}: {n_preenchidas}/{len(colunas_canonicas)} colunas canônicas preenchidas, {len(df)} linhas")
        partes.append(bloco)

    gold = pd.concat(partes, ignore_index=True)

    ARQUIVO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
    gold.to_csv(ARQUIVO_SAIDA, index=False)

    print(f"\nGold final: {len(gold)} linhas, {len(gold.columns)} colunas")
    print(f"Arquivo gravado em: {ARQUIVO_SAIDA}")


if __name__ == "__main__":
    main()
