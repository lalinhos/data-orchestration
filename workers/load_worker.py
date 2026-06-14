import json
from confluent_kafka import Consumer, KafkaError, Producer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_TRANSFORMED, KAFKA_TOPIC_TRANSFORMED_DLQ


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


def _publicar_dlq(registros: list[dict], topic: str = KAFKA_TOPIC_TRANSFORMED_DLQ) -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    for registro in registros:
        producer.produce(
            topic=topic,
            key=str(registro.get("numero_controle_pncp", "")),
            value=json.dumps(registro, ensure_ascii=False),
        )
        producer.poll(0)

    producer.flush()
    print(f"  [Kafka] {len(registros)} mensagens publicadas em '{topic}'")


def run(**kwargs):
    from src.config import MONGO_URI, MONGO_DB
    from src.loading import MongoRepository
    from src.transform import SparkSessionFactory, GoldAggregator
    from workers.extract_worker import janela_coleta

    di, data_final = janela_coleta(**kwargs)

    registros = _consumir_contratos(KAFKA_TOPIC_TRANSFORMED, group_id="pncp-gold")
    if not registros:
        print("Nenhum registro no Kafka para agregar.")
        return

    try:
        spark = SparkSessionFactory.create()
        try:
            agregacoes = GoldAggregator(spark).build(registros, di, data_final)
        finally:
            spark.stop()

        chaves = {
            "gold_area_de_servico": ["periodo_inicio", "periodo_fim", "uf", "ramo_mei"],
            "gold_estado":          ["periodo_inicio", "periodo_fim", "uf"],
            "gold_faixa_de_valor":  ["periodo_inicio", "periodo_fim", "uf", "faixa_valor"],
            "gold_situacao":        ["periodo_inicio", "periodo_fim", "uf", "situacao_nome"],
            "gold_por_mes":         ["uf", "ano", "mes"],
        }
        repositorio = MongoRepository(MONGO_URI, MONGO_DB)
        for colecao, registros_gold in agregacoes.items():
            repositorio.upsert_many(colecao, registros_gold, chaves[colecao])

    except Exception as e:
        print(f"[DLQ] Erro na agregação — enviando {len(registros)} registros para DLQ: {e}")
        _publicar_dlq(registros, KAFKA_TOPIC_TRANSFORMED_DLQ)
        raise