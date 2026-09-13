# tc3-medallion-pipeline-state-of-data

Pipeline de Engenharia de Dados (arquitetura Medallion: Bronze → Silver →
Gold) sobre a pesquisa **State of Data Brasil** (Data Hackers + Bain),
unificando as 3 últimas edições disponíveis (2023-2024, 2024-2025,
2025-2026). Feito para o Tech Challenge da Fase 03 (Big Data & Analytics)
da FIAP.

## Arquitetura

O pipeline roda direto sobre **S3 + AWS Glue Data Catalog** (bucket e
nomes de tabela em `scripts/py/_config_aws.py`):

```
S3 Bronze (dados brutos, por edição)
        │
        ▼  scripts 01/02/03 — padroniza o header de cada edição
S3 Silver "por edição" (staging — schema PRÓPRIO de cada edição)
        │
        ▼  script 05 — dicionário de correspondência entre edições
S3 Silver/_documentacao (dicionários de nulo e correspondência)
        │
        ▼  script 06 — harmoniza schema, grava cada edição na sua partição e cataloga
S3 Silver "state_of_data_silver"  ──►  Glue Data Catalog: db_state_of_data.state_of_data_silver
   (1 tabela, 3 partições — 2023-2024 / 2024-2025 / 2025-2026)
        │
        ▼  script 07 — lê a tabela via spark.table(), agrega por pergunta de negócio
S3 Gold/perguntas_negocio/  (7 tabelas pré-agregadas)
```

A tabela `db_state_of_data.state_of_data_silver` e suas 3 partições são
**criadas pelo próprio script 06** (via boto3/API do Glue), depois de
gravar os arquivos no S3 — ver `_config_aws.cataloga_tabela_particionada`.
É idempotente: pode rodar de novo sem erro (atualiza em vez de falhar se
a tabela/partição já existir).

## Estrutura de pastas

```
scripts/
  py/                                  scripts .py — os que de fato rodam como Glue Job
    _config_aws.py                     bucket, database, nomes de tabela e caminhos S3
    _lib_padroniza_colunas.py          lógica compartilhada (grupos multi-select, etc.)
    01/02/03_padroniza_colunas_*.py    Bronze -> Silver "por edição" (staging)
    04_documenta_nulos.py              documenta semântica do nulo por coluna
    05_monta_dicionario_correspondencia.py   de/para de colunas entre as 3 edições
    06_monta_silver_state_of_data.py   harmoniza schema -> tabela Silver catalogada
    07_monta_gold_perguntas_negocio.py Silver catalogada -> 7 tabelas Gold
  notebooks/                           mesmos 7 scripts em .ipynb (cópia, não substitui
                                        os .py — ver "Versão em notebook" abaixo)
```

## Requisitos

- Python 3.9+
- Java 17 (exigido pelo PySpark; sem JVM instalada o Spark não sobe)
- `pip install -r requirements.txt` (pandas + pyspark + s3fs)
- Credenciais AWS configuradas (`aws configure` ou variáveis de ambiente)
  para o Spark local enxergar o S3 — dentro de um Glue Job isso já vem
  pronto, não precisa configurar nada

Instalação do Java no macOS (Homebrew):
```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```
Adicione essas duas linhas de `export` no seu `~/.zshrc` para não precisar repetir a cada sessão de terminal.

## Como rodar o pipeline

Rodar cada script em ordem, a partir da raiz do repositório:

```bash
python3 scripts/py/01_padroniza_colunas_2023_2024.py
python3 scripts/py/02_padroniza_colunas_2024_2025.py
python3 scripts/py/03_padroniza_colunas_2025_2026.py
python3 scripts/py/04_documenta_nulos.py
python3 scripts/py/05_monta_dicionario_correspondencia.py
python3 scripts/py/06_monta_silver_state_of_data.py
python3 scripts/py/07_monta_gold_perguntas_negocio.py
```

| # | Script | Camada | O que faz |
|---|---|---|---|
| 01/02/03 | `padroniza_colunas_*` | Bronze → Silver (staging, por edição) | Limpa o header de cada edição: detecta grupos de pergunta multi-select (pai com texto concatenado + filhas binárias 0/1), funde grupos "alias" (mesma pergunta, público mutuamente exclusivo por skip logic), normaliza a sintaxe do nome final (minúsculo, sem acento, sem pontuação) |
| 04 | `documenta_nulos` | Silver (doc) | Documenta, para cada coluna, se o nulo é "grupo não exibido" (multi-select), "quase universal" (não-resposta genuína) ou "condicional ao perfil" (pergunta não se aplica a todos) — **não preenche nenhum nulo** |
| 05 | `monta_dicionario_correspondencia` | Silver (doc) | Casa colunas entre as 3 edições (nome exato, depois aproximado/fuzzy) e classifica a confiança de cada match — rascunho para revisão manual, não verdade automática |
| 06 | `monta_silver_state_of_data` | Silver (staging) → Silver (tabela catalogada) | Harmoniza o schema das 3 edições (usando o dicionário), grava cada uma na sua partição de `db_state_of_data.state_of_data_silver` e **cria/atualiza a tabela e as 3 partições no Glue Data Catalog via boto3** |
| 07 | `monta_gold_perguntas_negocio` | Silver (catalogada) → Gold (catalogada) | Lê `db_state_of_data.state_of_data_silver` via `spark.table(...)` (Glue Data Catalog), gera 7 tabelas pré-agregadas, uma por pergunta de negócio do desafio, e **cria/atualiza cada uma no Glue Data Catalog** (mesmo banco, sem partição — "edicao" já é coluna normal) |

**Importante — revisão manual:** `dicionario_correspondencia_colunas.csv`
é editado à mão (coluna `status_revisao_2023_2024`: `aprovado_manual` /
`rejeitado_manual`) depois de rodar o script 05. Rodar o script 05 de
novo **sobrescreve o arquivo do zero** — se já tiver revisão manual
feita, faça backup do CSV no S3 antes, ou reaplique as aprovações em
cima do arquivo novo depois.

## Saída dos scripts

Scripts que escrevem tabela de **dado de respondente** (01, 02, 03, 06, 07)
gravam um **diretório Spark** (`part-*.csv` + `_SUCCESS` dentro) no S3,
não um arquivo único — é assim que o Athena/Glue Catalog leem uma tabela
(apontando pra um prefixo do S3, não pra um arquivo). Os dois dicionários
de documentação (04 e 05) continuam sendo um único CSV "achatado" via
pandas, de propósito — são pequenos (uma linha por coluna, não por
respondente) e feitos para abrir/editar numa planilha (exige o pacote
`s3fs` para o pandas conseguir escrever direto no S3).

A tabela Silver catalogada **não grava a coluna de partição dentro do
arquivo** — segue a convenção Hive/Athena, onde o valor da partição vem
do caminho (pasta), não do conteúdo do CSV.

## Versão em notebook

`scripts/notebooks/` tem os mesmos 7 scripts convertidos para `.ipynb`
(uma célula por função/bloco de configuração) — cópia gerada a partir
dos `.py`, mantida à parte pra não ser a fonte de verdade do pipeline
(edite o `.py` correspondente e gere de novo se precisar sincronizar).
Cada notebook começa com uma célula de bootstrap que adiciona
`scripts/py/` ao `sys.path`, já que é de lá que vêm `_config_aws` e
`_lib_padroniza_colunas`:
```python
import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), "..", "py"))
```
Isso assume que o notebook roda com o diretório de trabalho igual à
pasta onde ele está (`scripts/notebooks/`) — é o padrão do Jupyter e do
Glue Studio Notebook ao abrir um arquivo `.ipynb`.

## No AWS Glue

Os scripts usam `cria_spark_session(...)` de `scripts/py/_lib_padroniza_colunas.py`
para rodar localmente. Dentro de um Glue Job, troque essa chamada pela
sessão já fornecida pelo GlueContext (que já vem com o Glue Data Catalog
configurado como Hive metastore, então `spark.table("db.tabela")` no
script 07 funciona sem nenhuma configuração extra):

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
```

O resto do código (leitura, transformação, escrita) funciona sem
alteração.
