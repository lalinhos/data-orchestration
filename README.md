# Pipeline de Orquestração PNCP

Pipeline de coleta, transformação e armazenamento de dados de contratações públicas da [API do PNCP](https://pncp.gov.br), orquestrado via **Apache Airflow** com processamento distribuído via **Apache Spark**.

## Tecnologias

- **Python 3.12**
- **Apache Airflow 2.9** — orquestração e agendamento do pipeline
- **Apache Kafka + Zookeeper (Confluent 7.6)** — mensageria entre os workers
- **Apache Spark (PySpark 3.5)** — transformação e agregação dos dados
- **MongoDB Atlas** — persistência nas camadas Bronze, Silver e Gold
- **Docker** — ambiente de execução isolado e reproduzível

## Arquitetura

```
API PNCP
   │
   ▼
extract_worker (Producer)
   │
   ▼
Kafka: pncp.contratos.raw
   │
   ▼
transform_worker (Consumer + Producer)
   │
   ▼
Kafka: pncp.contratos.transformed
   │
   ▼
load_worker (Consumer)
```

Em caso de falha, `transform_worker` e `load_worker` publicam os registros problemáticos em tópicos de DLQ (`pncp.contratos.raw.dlq` / `pncp.contratos.transformed.dlq`) antes de lançar o erro.

| Camada | Coleção MongoDB | Descrição |
|---|---|---|
| Bronze | `contratacoes_raw` | Dados brutos da API, sem transformação |
| Silver | `contratacoes_processadas` | Campos selecionados, padronizados e classificados |
| Gold | `gold_area_de_servico`, `gold_estado`, `gold_faixa_de_valor`, `gold_situacao`, `gold_por_mes` | Agregações analíticas prontas para consumo |

## Fluxo do pipeline

```
ingest_bronze → transform_silver → build_gold
```

1. **ingest_bronze** (`workers/extract_worker.py`) — coleta paginada da API do PNCP (dia atual) com retry automático, salva no MongoDB Bronze e publica em `pncp.contratos.raw`
2. **transform_silver** (`workers/transform_worker.py`) — consome `pncp.contratos.raw`, aplica flatten, classifica por ramo MEI, deduplica, salva na Silver e publica em `pncp.contratos.transformed`
3. **build_gold** (`workers/load_worker.py`) — consome `pncp.contratos.transformed`, executa 5 agregações por estado e salva nas coleções Gold

## Classificação por ramo MEI

Cada contratação é classificada automaticamente com base no `objeto_compra`:

| Ramo | Palavras-chave detectadas |
|---|---|
| Obras | obra, construção, reforma, pavimento... |
| TI | software, sistema, tecnologia, hardware... |
| Serviços | serviço, limpeza, vigilância, manutenção... |
| Compras | aquisição, fornecimento, material, equipamento... |
| Outros | demais casos |

## Estrutura do projeto

```
├── dags/
│   └── pncp_pipeline.py     # DAG do Airflow: liga os 3 workers
├── workers/
│   ├── extract_worker.py    # producer Kafka (task ingest_bronze)
│   ├── transform_worker.py  # consumer + producer (task transform_silver)
│   └── load_worker.py       # consumer final (task build_gold)
├── src/
│   ├── config.py            # URLs, parâmetros e variáveis de ambiente
│   ├── ingestion.py         # coleta paginada da API PNCP (Extract)
│   ├── transform.py         # transformações e agregações Spark (Transform)
│   └── loading.py           # acesso ao MongoDB Atlas (Load)
├── kafka/
│   └── docker-compose.yml   # Kafka + Zookeeper, rede pncp-network
├── Dockerfile               # imagem Airflow + Java + dependências
├── docker-compose.yaml      # webserver, scheduler e postgres, rede pncp-network
├── requirements.txt
└── .env                     # credenciais (não versionado)
```

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

## Configuração

Crie um arquivo `.env` na raiz do projeto:

```env
MONGO_URI=mongodb+srv://<usuario>:<senha>@<cluster>.mongodb.net/?appName=<app>
MONGO_DB=pncp
```

## Como rodar

```bash
# Sobe o Kafka + Zookeeper primeiro (cria a rede pncp-network)
docker compose -f kafka/docker-compose.yml up -d

# Primeira vez — inicializa o banco do Airflow e cria o usuário admin
docker-compose up airflow-init

# Sobe o ambiente completo (Postgres + Airflow)
docker-compose up
```

Acesse o dashboard em **http://localhost:8080** com `admin` / `admin`.

O DAG `pncp_pipeline` roda automaticamente todos os dias. Para disparar manualmente:

```bash
docker-compose exec scheduler airflow dags trigger pncp_pipeline
```

## Resiliência

- Erros **5xx** da API: retry com backoff (10s, 20s)
- Erros de **rede/timeout**: retry com backoff (5s, 10s)
- Cada task do Airflow tem **2 retries** automáticos com intervalo de 5 minutos
- Upsert por `numero_controle_pncp` — executar o pipeline duas vezes não duplica registros
- Falhas em `transform_silver`/`build_gold` enviam os registros para tópicos de DLQ antes de relançar o erro

## Mais detalhes

Veja [docs/DOCUMENTACAO.md](docs/DOCUMENTACAO.md) para a explicação completa de cada worker, tópicos Kafka, modelagem do MongoDB e estratégia de upsert.
