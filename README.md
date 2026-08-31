# tc3-medallion-pipeline-state-of-data

Pipeline de Engenharia de Dados (arquitetura Medallion: Bronze → Silver →
Gold) sobre a pesquisa **State of Data Brasil** (Data Hackers + Bain),
unificando as 3 últimas edições disponíveis (2023-2024, 2024-2025,
2025-2026). Feito para o Tech Challenge da Fase 03 (Big Data & Analytics)
da FIAP.

## Estrutura de pastas

```
Bronze/                     dados brutos, como vieram do Kaggle (não alterar)
  2023-2024/
  2024-2025/
  2025-2026/
Silver/                     dados tratados: header padronizado, grupos
  2023-2024/                multi-select desmanchados, sem preenchimento
  2024-2025/                de nulo (ver Silver/_documentacao)
  2025-2026/
  _documentacao/
    dicionario_nulos.csv                  semântica do nulo por coluna
    dicionario_correspondencia_colunas.csv  de/para de colunas entre as 3 edições
Gold/
  state_of_data_unificado/  as 3 edições unidas, 1 linha por respondente
  perguntas_negocio/        7 tabelas pré-agregadas, uma por pergunta do desafio
scripts/                    pipeline PySpark (ver abaixo)
```

## Requisitos

- Python 3.9+
- Java 17 (exigido pelo PySpark; sem JVM instalada o Spark não sobe)
- `pip install -r requirements.txt` (pandas + pyspark)

Instalação do Java no macOS (Homebrew):
```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```
Adicione essas duas linhas de `export` no seu `~/.zshrc` para não precisar repetir a cada sessão de terminal.

## Como rodar o pipeline (local)

Rodar cada script em ordem, a partir da raiz do repositório:

```bash
python3 scripts/01_padroniza_colunas_2023_2024.py
python3 scripts/02_padroniza_colunas_2024_2025.py
python3 scripts/03_padroniza_colunas_2025_2026.py
python3 scripts/04_documenta_nulos.py
python3 scripts/05_monta_dicionario_correspondencia.py
python3 scripts/06_monta_gold_unificado.py
python3 scripts/07_monta_gold_perguntas_negocio.py
```

| # | Script | Camada | O que faz |
|---|---|---|---|
| 01/02/03 | `padroniza_colunas_*` | Bronze → Silver | Limpa o header de cada edição: detecta grupos de pergunta multi-select (pai com texto concatenado + filhas binárias 0/1), funde grupos "alias" (mesma pergunta, público mutuamente exclusivo por skip logic), normaliza a sintaxe do nome final (minúsculo, sem acento, sem pontuação) |
| 04 | `documenta_nulos` | Silver (doc) | Documenta, para cada coluna, se o nulo é "grupo não exibido" (multi-select), "quase universal" (não-resposta genuína) ou "condicional ao perfil" (pergunta não se aplica a todos) — **não preenche nenhum nulo** |
| 05 | `monta_dicionario_correspondencia` | Silver (doc) | Casa colunas entre as 3 edições (nome exato, depois aproximado/fuzzy) e classifica a confiança de cada match — rascunho para revisão manual, não verdade automática |
| 06 | `monta_gold_unificado` | Silver → Gold | Une as 3 edições numa base só, usando apenas correspondências confiáveis do dicionário (exatas, fuzzy de alta confiança, ou aprovadas na revisão manual) |
| 07 | `monta_gold_perguntas_negocio` | Gold → Gold | Gera 7 tabelas pré-agregadas, uma por pergunta de negócio do desafio (estrutura do mercado, perfis valorizados, diversidade, adoção de tecnologia/IA, diferenças regionais, oportunidades e desafios) |

**Importante — revisão manual:** `dicionario_correspondencia_colunas.csv`
é editado à mão (coluna `status_revisao_2023_2024`: `aprovado_manual` /
`rejeitado_manual`) depois de rodar o script 05. Rodar o script 05 de
novo **sobrescreve o arquivo do zero** — se já tiver revisão manual
feita, faça backup antes ou aplique as mudanças com cuidado em cima do
arquivo existente em vez de rodar o script direto.

## Saída dos scripts

Scripts que escrevem tabela de **dado de respondente** (01, 02, 03, 06, 07)
gravam um **diretório Spark** (`part-*.csv` + `_SUCCESS` dentro), não um
arquivo único — é assim que sai de um Glue Job de verdade e como o
Athena/Glue Catalog leem uma tabela (apontando pra um prefixo do S3, não
pra um arquivo). Os dois dicionários de documentação (04 e 05) continuam
sendo um único CSV "achatado" via pandas, de propósito — são pequenos
(uma linha por coluna, não por respondente) e feitos para abrir/editar
numa planilha.

## No AWS Glue

Os scripts usam `cria_spark_session(...)` de `scripts/_lib_padroniza_colunas.py`
para rodar localmente. Dentro de um Glue Job, troque essa chamada pela
sessão já fornecida pelo GlueContext:

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
```

O resto do código (leitura, transformação, escrita) funciona sem
alteração — trocando os caminhos locais (`Bronze/...`, `Silver/...`,
`Gold/...`) por `s3://<bucket>/...`.
