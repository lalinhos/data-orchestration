import json
from confluent_kafka import Consumer, KafkaError, Producer
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    KAFKA_TOPIC_TRANSFORMED,
    KAFKA_TOPIC_RAW_DLQ,
)


def _consumir_contratos(topic: str, group_id: str, timeout: float = 30.0) -> list[dict]:
    assigned_partitions: set[int] = set()
    eof_partitions: set[int] = set()

    def on_assign(consumer, partitions):
        for p in partitions:
            assigned_partitions.add(p.partition)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    consumer.subscribe([topic], on_assign=on_assign)

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=timeout)

            if msg is None:
                # Nenhuma mensagem chegou dentro do timeout — fim do lote
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof_partitions.add(msg.partition())
                    if assigned_partitions and eof_partitions >= assigned_partitions:
                        break
                    continue
                raise RuntimeError(f"Erro Kafka: {msg.error()}")

            registros.append(json.loads(msg.value().decode("utf-8")))

        consumer.commit()
    finally:
        consumer.close()

    print(f"  [Kafka] {len(registros)} mensagens consumidas de '{topic}'")
    return registros


def _publicar_contratos(registros: list[dict], topic: str) -> None:
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
    from src.config import MONGO_URI, MONGO_DB
    from src.loading import MongoRepository
    from src.transform import SparkSessionFactory, ContratacoesTransformer

    registros_raw = _consumir_contratos(KAFKA_TOPIC_RAW, group_id="pncp-silver")
    if not registros_raw:
        print("Nenhum registro no Kafka para processar.")
        return

    try:
        spark = SparkSessionFactory.create()
        try:
            registros_processados = ContratacoesTransformer(spark).transform(registros_raw)
        finally:
            spark.stop()

        registros_serializados = [
            {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in r.items()}
            for r in registros_processados
        ]

        repositorio = MongoRepository(MONGO_URI, MONGO_DB)
        repositorio.upsert_many("contratacoes_processadas", registros_serializados, ["numero_controle_pncp"])
        print(f"Silver: {repositorio.count('contratacoes_processadas')} documentos totais.")
        _publicar_contratos(registros_serializados, KAFKA_TOPIC_TRANSFORMED)

    except Exception as e:
        print(f"[DLQ] Erro no processamento — enviando {len(registros_raw)} registros para DLQ: {e}")
        _publicar_contratos(registros_raw, KAFKA_TOPIC_RAW_DLQ)
        raise