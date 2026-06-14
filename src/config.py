import os

BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

DEFAULT_PARAMS = {
    "codigoModalidadeContratacao": 8,   # 8 = dispensa de licitacao
    "tamanhoPagina": 50,
}

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB  = os.getenv("MONGO_DB", "pncp")

KAFKA_BOOTSTRAP_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW             = os.getenv("KAFKA_TOPIC_RAW",             "pncp.contratos.raw")
KAFKA_TOPIC_TRANSFORMED     = os.getenv("KAFKA_TOPIC_TRANSFORMED",     "pncp.contratos.transformed")
KAFKA_TOPIC_RAW_DLQ         = os.getenv("KAFKA_TOPIC_RAW_DLQ",         "pncp.contratos.raw.dlq")
KAFKA_TOPIC_TRANSFORMED_DLQ = os.getenv("KAFKA_TOPIC_TRANSFORMED_DLQ", "pncp.contratos.transformed.dlq")
