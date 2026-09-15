# tc3-medallion-pipeline-state-of-data

Pipeline de Engenharia de Dados (arquitetura Medallion: Bronze → Silver →
Gold) sobre a pesquisa **State of Data Brasil** (Data Hackers + Bain),
unificando as 3 últimas edições disponíveis (2023-2024, 2024-2025,
2025-2026). Feito para o Tech Challenge da Fase 03 (Big Data & Analytics)
da FIAP.
PDF da Apresentação Executiva: <a href="POSTECH_DATA_ANALYTICS.pdf" target="_blank">POSTECH_DATA_ANALYTICS.pdf</a>

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

## Analytics

Depois da construção das 7 tabelas Gold, foi desenvolvida uma camada de
**Analytics** para responder diretamente às perguntas de negócio do desafio.
Essa etapa parte dos arquivos em `Gold/perguntas_negocio/`, faz as validações
e tratamentos necessários para cada análise e gera tabelas auxiliares e gráficos
prontos para o material executivo.

O fluxo analítico ficou separado do pipeline de Engenharia de Dados:

```
Gold/perguntas_negocio/  (7 tabelas pré-agregadas)
        │
        ▼  scripts/Analytics/gold_XX_* — inspeção, análise e cruzamentos em PySpark
scripts/Analytics/outputs/gold_XX/      — CSVs analíticos para validação/visualização
        │
        ▼  scripts de gráficos — leitura dos outputs e preparação visual
scripts/Analytics/graficos/gold_XX/     — gráficos em PNG para o storytelling executivo
```

As transformações e agregações analíticas são feitas principalmente em
**PySpark**. `pandas` é utilizado nos outputs menores e na preparação dos dados
para visualização, e os gráficos são gerados em **Matplotlib**. Em algumas
visualizações também é utilizado `numpy`.

## Estrutura da camada de Analytics

```
scripts/
  Analytics/
    gold_01_estrutura_mercado/              estrutura atual e evolução do mercado
    gold_02_perfis_valorizados/             remuneração e perfis mais valorizados
    gold_03_diversidade_genero/             gênero, senioridade, cargos, salário e cor/raça/etnia
    gold_04_adocao_tecnologias/             adoção e evolução de tecnologias
    gold_05_adocao_ia/                      adoção, prioridade, uso e barreiras de IA
    gold_06_regiao_senioridade_modelo_trabalho/  diferenças salariais e cruzamentos
    gold_07_oportunidades_desafios/         oportunidades, desafios e síntese executiva
    outputs/                                CSVs derivados das análises
      gold_01/
      gold_02/
      gold_03/
      gold_04/
      gold_05/
      gold_06/
      gold_07/
    graficos/                               PNGs utilizados no material executivo
    utils/
      export_utils.py                       função compartilhada para exportação de CSVs
```

Cada pasta `gold_XX_*` segue a mesma lógica geral: primeiro é feita a inspeção
da Gold e a validação das variáveis disponíveis; depois são executadas as
análises e cruzamentos; por último, os resultados selecionados são exportados e
transformados em gráficos. Quando uma pergunta exige mais de um recorte, a
análise foi separada em scripts menores para manter cada etapa rastreável.

## Perguntas de negócio analisadas

| Gold | Pergunta | O que foi analisado |
|---|---|---|
| 01 | Como está estruturado o mercado brasileiro de Dados? | Estrutura e evolução de cargos, senioridade, setores, modelos de trabalho, região, situação de trabalho e porte das empresas. Os cargos foram harmonizados antes das comparações históricas para reduzir diferenças de nomenclatura entre edições. |
| 02 | Quais perfis profissionais são mais valorizados pelo mercado? | Perfis por cargo e senioridade, com distribuição das faixas salariais, P50 e P75, ranking da edição 2025-2026 e evolução histórica. A análise usa amostra mínima de 20 respondentes para os perfis comparáveis e trata a faixa salarial inconsistente identificada durante a inspeção. |
| 03 | Qual é o cenário de diversidade de gênero nas carreiras de dados? | Composição de gênero ao longo das edições, participação feminina por senioridade, cargo e faixa salarial, além da composição de cor/raça/etnia e do recorte de participação feminina dentro dessas categorias. |
| 04 | Quais tecnologias apresentam maior adoção entre os profissionais? | Rankings e evolução de linguagens de programação, ferramentas de BI, cloud, bancos de dados e ferramentas de ETL para Data Engineer e Data Analyst, além da linguagem preferida na edição mais recente e dos maiores crescimentos e quedas de adoção. |
| 05 | Qual é o índice de adoção de Inteligência Artificial e seu impacto? | Evolução da adoção pessoal de IA, prioridade atribuída à IA nas empresas, formas de uso, uso pessoal de soluções de IA e principais barreiras para adoção. |
| 06 | Existem diferenças relevantes entre regiões, senioridades ou modelos de trabalho? | Comparação das faixas salariais por região, senioridade e modelo de trabalho, utilizando P50 e P75, além dos cruzamentos senioridade × modelo de trabalho e senioridade × região. Grupos com menos de 30 respondentes são mantidos, mas sinalizados como amostra pequena. |
| 07 | Quais oportunidades e desafios podem ser identificados para empresas que desejam investir em Dados e Inteligência Artificial? | Critérios para escolha de emprego, motivos de insatisfação profissional, desafios dos gestores, barreiras para uso de IA, variações entre edições e uma síntese executiva dos principais indicadores. |

## Critérios adotados nas análises

As comparações históricas respeitam a disponibilidade real de cada variável nas
edições da pesquisa. Quando uma categoria não existe em determinado ano, ela não
é preenchida artificialmente apenas para completar a série.

Para os recortes de remuneração, as faixas salariais continuam sendo tratadas
como **intervalos categóricos ordenados**. O P50 corresponde à primeira faixa que
atinge 50% da distribuição acumulada e o P75 à primeira faixa que atinge 75%; os
resultados, portanto, representam faixas salariais e não salários pontuais.

Também foram aplicadas validações de denominador e tamanho de amostra antes das
comparações. Nos casos em que a amostra reduzida ainda é mantida no resultado,
essa condição é explicitamente sinalizada para evitar que grupos pequenos sejam
interpretados com o mesmo peso dos grupos mais representativos.

As diferenças de nomenclatura que impediam comparações diretas foram tratadas
somente quando necessário para a análise. Um exemplo é a harmonização das
categorias de **Engenharia de Dados** e **Arquitetura de Dados** utilizada nos
recortes históricos de cargos e remuneração.

## Outputs e gráficos

Os arquivos em `scripts/Analytics/outputs/` são derivados analíticos das Golds e
servem como etapa intermediária entre o processamento em PySpark e a construção
das visualizações. Eles não substituem as tabelas Gold do pipeline.

Os scripts de gráficos leem esses resultados já consolidados e geram arquivos
PNG voltados para a apresentação executiva. Entre as visualizações produzidas
estão estrutura e evolução de cargos, senioridade, setores e modelos de trabalho;
P50/P75 dos perfis valorizados; diversidade de gênero e cor/raça/etnia; adoção de
tecnologias; adoção e barreiras de IA; diferenças salariais por região,
senioridade e modelo de trabalho; e os principais desafios e oportunidades para
empresas.

## Como rodar as análises

Depois de gerar as 7 tabelas Gold, rode os scripts de cada pasta em **ordem
numérica**. Os scripts assumem a seguinte estrutura relativa a partir da raiz do
repositório:

```text
Gold/perguntas_negocio/gold_XX_*/
scripts/Analytics/gold_XX_*/
scripts/Analytics/outputs/gold_XX/
scripts/Analytics/graficos/gold_XX/
```

Exemplo para a Gold 01:

```bash
python3 scripts/Analytics/gold_01_estrutura_mercado/01_cargos.py
python3 scripts/Analytics/gold_01_estrutura_mercado/02_nivel.py
python3 scripts/Analytics/gold_01_estrutura_mercado/03_setor.py
python3 scripts/Analytics/gold_01_estrutura_mercado/04_modelo_de_trabalho.py
python3 scripts/Analytics/gold_01_estrutura_mercado/05_complementares.py
python3 scripts/Analytics/gold_01_estrutura_mercado/06_gráficos_gold_01.py
```

O mesmo padrão é seguido nas demais Golds: inspeção/validação → análises e
cruzamentos → exportação dos resultados → geração dos gráficos.
