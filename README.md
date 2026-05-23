# Pipeline de Orquestração PNCP

Pipeline de coleta, transformação e armazenamento de dados de contratações públicas da [API do PNCP](https://pncp.gov.br), orquestrado via **Apache Airflow** com processamento distribuído via **Apache Spark**.

## Tecnologias

- **Python 3.12**
- **Apache Airflow 2.9** — orquestração e agendamento do pipeline
- **Apache Spark (PySpark 3.5)** — transformação e agregação dos dados
- **MongoDB Atlas** — persistência nas camadas Bronze, Silver e Gold
- **Docker** — ambiente de execução isolado e reproduzível

## Arquitetura

```
API PNCP → Bronze (raw) → Silver (processado) → Gold (agregado)
```

| Camada | Coleção MongoDB | Descrição |
|---|---|---|
| Bronze | `contratacoes_raw` | Dados brutos da API, sem transformação |
| Silver | `contratacoes_processadas` | Campos selecionados, padronizados e classificados |
| Gold | `gold_area_de_servico`, `gold_estado`, `gold_faixa_de_valor`, `gold_situacao`, `gold_por_mes` | Agregações analíticas prontas para consumo |

## Fluxo do pipeline

```
ingest_bronze → transform_silver → build_gold
```

1. **ingest_bronze** — coleta paginada da API do PNCP com retry automático, salva no MongoDB raw
2. **transform_silver** — lê do Bronze, aplica flatten, classifica por ramo MEI, deduplica e salva na Silver
3. **build_gold** — lê da Silver, executa 5 agregações por estado e salva nas coleções Gold

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
│   └── pncp_pipeline.py     # DAG do Airflow (3 tasks)
├── src/
│   ├── config.py            # URLs, parâmetros e variáveis de ambiente
│   ├── ingestion.py         # coleta paginada da API PNCP
│   ├── processing.py        # transformações e agregações Spark
│   └── loading.py           # acesso ao MongoDB Atlas
├── Dockerfile               # imagem Airflow + Java + dependências
├── docker-compose.yaml      # webserver, scheduler e postgres
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
# Primeira vez — inicializa o banco do Airflow e cria o usuário admin
docker-compose up airflow-init

# Sobe o ambiente completo
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
