# Documentação do Pipeline PNCP

## O que o projeto faz

Coleta contratos públicos da API do PNCP (Portal Nacional de Contratações Públicas), processa e armazena os dados em três camadas (Bronze, Silver, Gold) usando Apache Kafka como sistema de mensageria entre as etapas.

---

## Estrutura de pastas

```
orquestracao-pncp/
├── workers/
│   ├── extract_worker.py     ← producer Kafka (chamado pela task ingest_bronze)
│   ├── transform_worker.py   ← consumer + producer (chamado pela task transform_silver)
│   └── load_worker.py        ← consumer final (chamado pela task build_gold)
├── kafka/
│   └── docker-compose.yml    ← Kafka + Zookeeper, rede pncp-network
├── src/
│   ├── ingestion.py           (chamada à API PNCP — Extract)
│   ├── transform.py            (flatten, classificação, agregações — Transform)
│   ├── loading.py              (upserts no MongoDB — Load)
│   └── config.py
├── dags/
│   └── pncp_pipeline.py       ← DAG Airflow: liga os 3 workers
├── docker-compose.yaml        ← Postgres + Airflow, rede pncp-network
└── requirements.txt
```

### Como subir o ambiente

A rede `pncp-network` é compartilhada entre os dois `docker-compose`. **Suba o Kafka primeiro** (ele cria a rede), depois o Airflow:

```bash
docker compose -f kafka/docker-compose.yml up -d
docker compose up -d
```

### Coletando uma data específica

Por padrão o pipeline coleta o dia atual (UTC). Para coletar uma data específica, dispare o DAG passando `ano`, `mes` e `dia` em "Trigger DAG w/ config" (UI do Airflow) ou via CLI:

```bash
docker-compose exec scheduler airflow dags trigger pncp_pipeline \
  --conf '{"ano": 2026, "mes": 5, "dia": 10}'
```

Se algum desses parâmetros não for informado (ou ficar `null`), o pipeline usa a data atual. A função `janela_coleta` (`workers/extract_worker.py`) é a responsável por essa lógica e é reaproveitada pelo `load_worker` para manter o mesmo período em toda a execução.

---

## Arquitetura geral

```
API PNCP
   │
   ▼
EXTRACT WORKER (ingest_bronze)
   │  Producer
   ▼
Kafka: pncp.contratos.raw
   │
   ▼
TRANSFORM WORKER (transform_silver)
   │  Consumer + Producer
   ├──► MongoDB Silver (backup)
   ▼
Kafka: pncp.contratos.transformed
   │
   ▼
LOAD WORKER (build_gold)
   │  Consumer
   ▼
MongoDB Gold (agregações)
```

Em caso de falha, cada worker envia os dados problemáticos para um tópico de DLQ (Dead Letter Queue) antes de lançar o erro.

---

## Tópicos Kafka

| Tópico | Quem produz | Quem consome | Conteúdo |
|---|---|---|---|
| `pncp.contratos.raw` | `ingest_bronze` | `transform_silver` | Contratos crus da API (JSON original) |
| `pncp.contratos.transformed` | `transform_silver` | `build_gold` | Contratos normalizados e classificados |
| `pncp.contratos.raw.dlq` | `transform_silver` (falha) | — | Contratos que falharam na transformação |
| `pncp.contratos.transformed.dlq` | `build_gold` (falha) | — | Contratos que falharam na agregação |

---

## Componentes

### 1. Extract Worker — `ingest_bronze`

**Arquivo:** `workers/extract_worker.py` → função `run`
**Papel no Kafka:** Producer

**O que faz:**
- Chama a API do PNCP página por página (dia atual, ou data customizada via Params do Airflow)
- Para cada página recebida:
  - Salva os contratos no MongoDB Bronze (`contratacoes_raw`) como backup
  - Publica cada contrato como mensagem no tópico `pncp.contratos.raw`

**Por que salva no MongoDB também:**
A API é instável. O MongoDB Bronze garante que os dados coletados não se percam se o Kafka reiniciar ou se houver necessidade de reprocessamento futuro.

**Resiliência:** retry automático com backoff na chamada da API. Em caso de timeout, as mensagens já publicadas ficam preservadas no Kafka.

---

### 2. Transform Worker — `transform_silver`

**Arquivo:** `workers/transform_worker.py` → função `run`
**Papel no Kafka:** Consumer + Producer

**O que faz:**
1. **Consome** todas as mensagens do tópico `pncp.contratos.raw` (group_id: `pncp-silver`)
2. Processa o lote com PySpark:
   - `flatten_and_select`: achata o JSON e seleciona os campos relevantes
   - `classify_ramo_mei`: classifica o contrato por ramo (Obras, TI, Serviços, Compras, Outros)
   - `deduplicate`: remove duplicatas pelo `numeroControlePNCP`
   - `add_data_coleta`: adiciona timestamp de processamento
3. Salva os registros processados no MongoDB Silver (`contratacoes_processadas`)
4. **Publica** os registros processados no tópico `pncp.contratos.transformed`

**Em caso de erro:**
Publica os registros originais (não processados) no tópico `pncp.contratos.raw.dlq` para análise posterior, e relança o erro para o Airflow marcar a task como falha.

**Offset:** O Kafka registra até onde esse consumer group leu. Na próxima execução diária, começa exatamente de onde parou — não reprocessa o que já foi transformado.

---

### 3. Load Worker — `build_gold`

**Arquivo:** `workers/load_worker.py` → função `run`
**Papel no Kafka:** Consumer

**O que faz:**
1. **Consome** todas as mensagens do tópico `pncp.contratos.transformed` (group_id: `pncp-gold`)
2. Roda agregações com PySpark e salva 5 coleções no MongoDB Gold:
   - `gold_area_de_servico`: total e valor por UF e ramo MEI
   - `gold_estado`: total e valor por estado
   - `gold_faixa_de_valor`: distribuição por faixa de valor
   - `gold_situacao`: distribuição por situação do contrato
   - `gold_por_mes`: evolução mensal por UF

**Em caso de erro:**
Publica os registros no tópico `pncp.contratos.transformed.dlq` e relança o erro.

---

## Arquivos do projeto

| Arquivo | Responsabilidade |
|---|---|
| `src/ingestion.py` | Paginação e chamada da API PNCP com retry/backoff |
| `src/transform.py` | Transformações PySpark (flatten, classificação, agregações Gold) |
| `src/loading.py` | Operações MongoDB (upsert no Bronze, Silver e Gold) |
| `src/config.py` | Variáveis de ambiente (URLs, nomes de tópicos, credenciais) |
| `workers/extract_worker.py` | Extract Worker (`ingest_bronze`): Producer Kafka, chamado pela DAG |
| `workers/transform_worker.py` | Transform Worker (`transform_silver`): Consumer + Producer Kafka, chamado pela DAG |
| `workers/load_worker.py` | Load Worker (`build_gold`): Consumer Kafka final, chamado pela DAG |
| `dags/pncp_pipeline.py` | Orquestração Airflow: define as 3 tasks (uma por worker) e a dependência entre elas |
| `docker-compose.yaml` | Sobe Postgres e Airflow (webserver + scheduler), conectados à rede `pncp-network` |
| `kafka/docker-compose.yml` | Sobe Zookeeper e Kafka na rede `pncp-network` |

---

## Fluxo de dados completo

```
[Airflow agenda execução diária]
         │
         ▼
ingest_bronze
  ├── Chama API PNCP (dia atual, paginado)
  ├── Salva no MongoDB Bronze             → contratacoes_raw
  └── Publica em pncp.contratos.raw       → cada contrato = 1 mensagem Kafka
         │
         ▼ (Airflow aguarda sucesso)
transform_silver
  ├── Consome de pncp.contratos.raw
  ├── PySpark: flatten + classifica + dedup + timestamp
  ├── Salva no MongoDB Silver             → contratacoes_processadas
  └── Publica em pncp.contratos.transformed
         │
         ▼ (Airflow aguarda sucesso)
build_gold
  ├── Consome de pncp.contratos.transformed
  ├── PySpark: 5 agregações por UF, área, faixa, situação, mês
  └── Salva no MongoDB Gold               → 5 coleções gold_*
```

---

## Modelagem de dados no MongoDB Atlas

Todas as camadas (Bronze, Silver, Gold) são persistidas em um cluster **MongoDB Atlas** (`mongodb+srv://...`), configurado via `MONGO_URI` em `src/config.py`.

### Granularidade dos documentos

- **Bronze (`contratacoes_raw`) e Silver (`contratacoes_processadas`)**: 1 documento = 1 contratação publicada no PNCP. É a menor unidade de negócio retornada pela API, então não faz sentido quebrar em granularidade menor nem agrupar várias contratações em um único documento.
- **Gold (`gold_*`)**: 1 documento = 1 linha de agregação (uma combinação de dimensões + período), pré-calculada para alimentar dashboards sem precisar reprocessar os dados brutos a cada consulta.

### Chaves e índices por coleção

| Coleção | Chave única (índice) | Por que essa chave |
|---|---|---|
| `contratacoes_raw` | `numeroControlePNCP` | É o identificador oficial e único de cada contratação no PNCP. Usá-lo como chave garante que o mesmo contrato nunca seja duplicado, mesmo coletando o mesmo dia mais de uma vez. |
| `contratacoes_processadas` | `numero_controle_pncp` | Mesma identidade de negócio da Bronze, apenas em snake_case após o `flatten_and_select`. Mantém rastreabilidade 1:1 entre as camadas e permite que o Silver receba "atualizações" de um contrato (ex: mudança de situação) sem virar um novo documento. |
| `gold_area_de_servico` | `periodo_inicio + periodo_fim + uf + ramo_mei` | Cada documento é uma célula do "cubo" total/valor por estado e ramo de atuação, dentro de um período. A combinação dessas dimensões é o que identifica unicamente uma agregação. |
| `gold_estado` | `periodo_inicio + periodo_fim + uf` | Agregação total por estado e período — a chave reflete exatamente o nível de corte do relatório. |
| `gold_faixa_de_valor` | `periodo_inicio + periodo_fim + uf + faixa_valor` | Distribuição por faixa de valor dentro de cada UF/período. |
| `gold_situacao` | `periodo_inicio + periodo_fim + uf + situacao_nome` | Distribuição por situação da contratação dentro de cada UF/período. |
| `gold_por_mes` | `uf + ano + mes` | Série histórica mensal por UF — não usa `periodo_inicio/fim` porque o objetivo é acumular a evolução ao longo do tempo, não um snapshot do período coletado. |

### Estratégia de carga: upsert idempotente

Todas as gravações (`src/loading.py`) usam `update_one(filtro, {"$set": registro}, upsert=True)`, onde `filtro` é montado a partir da chave única da coleção:

- **Bronze/Silver**: `filtro = {"numeroControlePNCP": ...}` (ou `numero_controle_pncp`).
- **Gold**: `filtro` é construído dinamicamente a partir das colunas de `chave` (ex: `{"periodo_inicio": ..., "periodo_fim": ..., "uf": ..., "ramo_mei": ...}`).

Isso garante que:
- Reexecutar a DAG para o mesmo dia (ex: retry do Airflow) **atualiza** os documentos existentes em vez de criar duplicatas.
- Reprocessamentos manuais de períodos passados sobrescrevem as agregações Gold daquele período sem afetar os demais.
- O `deduplicate` (Spark, por `numero_controle_pncp`) reforça a idempotência antes mesmo de chegar ao Mongo, evitando upserts redundantes dentro do mesmo lote.

---

## Por que Kafka e não chamada direta entre tasks?

Sem Kafka, as tasks se chamariam diretamente via MongoDB:
- `transform_silver` leria do MongoDB Bronze
- `build_gold` leria do MongoDB Silver

Com Kafka:
- Se o Spark falha no meio, o consumer reinicia do offset salvo — sem re-ingerir da API
- Cada worker só precisa saber do tópico que consome, não de quem produziu
- Novos consumidores (ex: alertas, dashboards) podem ser adicionados sem alterar nenhum worker existente

---

## Tecnologias

| Tecnologia | Versão | Função |
|---|---|---|
| Apache Airflow | 2.9.2 | Orquestração e agendamento das tasks |
| Apache Kafka | 7.6.0 (Confluent) | Mensageria entre os workers |
| Zookeeper | 7.6.0 (Confluent) | Gerencia metadados do cluster Kafka |
| PySpark | 3.5.1 | Transformações distribuídas |
| MongoDB Atlas | — | Persistência nas camadas Bronze, Silver e Gold |
| Docker Compose | — | Sobe todo o ambiente local |
