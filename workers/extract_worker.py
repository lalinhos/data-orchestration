import json
from datetime import datetime, timezone
from confluent_kafka import Producer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW


def janela_coleta(**kwargs) -> tuple[str, str]:
    params = kwargs.get("params", {})
    ano, mes, dia = params.get("ano"), params.get("mes"), params.get("dia")

    if ano and mes and dia:
        data = f"{int(ano):04d}{int(mes):02d}{int(dia):02d}"
        return data, data

    hoje = datetime.now(timezone.utc).strftime("%Y%m%d")
    return hoje, hoje


def _publicar_contratos(registros: list[dict], topic: str = KAFKA_TOPIC_RAW) -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    for registro in registros:
        producer.produce(
            topic=topic,
            key=str(registro.get("numeroControlePNCP", "")),
            value=json.dumps(registro, ensure_ascii=False),
        )
        producer.poll(0)

    producer.flush()
    print(f"  [Kafka] {len(registros)} mensagens publicadas em '{topic}'")


def run(**kwargs):
    from src.config import BASE_URL, DEFAULT_PARAMS, MONGO_URI, MONGO_DB
    from src.ingestion import PNCPClient, ContratacoesExtractor
    from src.loading import MongoRepository

    di, data_final = janela_coleta(**kwargs)
    print(f"Coletando de {di} até {data_final}")

    client = PNCPClient(BASE_URL)
    extractor = ContratacoesExtractor(
        client,
        modalidade=DEFAULT_PARAMS["codigoModalidadeContratacao"],
        tamanho=DEFAULT_PARAMS["tamanhoPagina"],
    )
    repositorio = MongoRepository(MONGO_URI, MONGO_DB)

    for pagina, total_paginas, registros in extractor.iter_pages(di, data_final):
        print(f"  [{pagina}/{total_paginas}] {len(registros)} registros")
        if registros:
            repositorio.upsert_many("contratacoes_raw", registros, ["numeroControlePNCP"])
            _publicar_contratos(registros)