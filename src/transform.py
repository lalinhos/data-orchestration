import json
import os
import tempfile
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


class SparkSessionFactory:
    """Cria sessões Spark configuradas para o pipeline."""

    @staticmethod
    def create(app_name: str = "PNCP-Pipeline") -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.debug.maxToStringFields", "50")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark


class SparkRecordLoader:
    """Classe base: carrega listas de dicts (JSON) em DataFrames Spark."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_from_records(self, registros: list[dict]) -> DataFrame:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            json.dump(registros, f, ensure_ascii=False)
            tmp = f.name
        try:
            df = self.spark.read.option("multiline", "true").json(tmp)
            df.cache().count()
            return df
        finally:
            os.unlink(tmp)


class ContratacoesTransformer(SparkRecordLoader):
    """Transforma contratos crus do PNCP em registros normalizados (camada Silver)."""

    def flatten_and_select(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("numeroControlePNCP").alias("numero_controle_pncp"),
            F.col("orgaoEntidade.cnpj").alias("cnpj_orgao"),
            F.col("orgaoEntidade.razaoSocial").alias("razao_social"),
            F.col("unidadeOrgao.municipioNome").alias("municipio_nome"),
            F.col("unidadeOrgao.ufSigla").alias("uf"),
            F.col("dataPublicacaoPncp").alias("data_publicacao_pncp"),
            F.col("anoCompra").alias("ano_compra"),
            F.col("objetoCompra").alias("objeto_compra"),
            F.col("modalidadeId").alias("modalidade_id"),
            F.col("modalidadeNome").alias("modalidade_nome"),
            F.col("situacaoCompraId").alias("situacao_id"),
            F.col("situacaoCompraNome").alias("situacao_nome"),
            F.col("valorTotalEstimado").cast(DoubleType()).alias("valor_total_estimado"),
        )

    def classify_ramo_mei(self, df: DataFrame) -> DataFrame:
        objeto = F.lower(F.col("objeto_compra"))
        return df.withColumn(
            "ramo_mei",
            F.when(objeto.rlike("obra|constru|reforma|paviment|calçament|edificaç"), "Obras")
             .when(objeto.rlike("software|sistema|tecnologia|informátic|ti |suporte técnico|hardware"), "TI")
             .when(objeto.rlike("serviç|manutenç|limpeza|vigilânc|conservaç|lavanderia|portaria|copeira"), "Serviços")
             .when(objeto.rlike("aquisiç|forneciment|material|equipament|produto|compra|item|insumo"), "Compras")
             .otherwise("Outros")
        )

    def deduplicate(self, df: DataFrame) -> DataFrame:
        return df.dropDuplicates(["numero_controle_pncp"])

    def add_data_coleta(self, df: DataFrame) -> DataFrame:
        return df.withColumn("data_coleta", F.current_timestamp())

    def transform(self, registros: list[dict]) -> list[dict]:
        df = (
            self.load_from_records(registros)
            .transform(self.flatten_and_select)
            .transform(self.classify_ramo_mei)
            .transform(self.deduplicate)
            .transform(self.add_data_coleta)
        )
        return df.toPandas().to_dict(orient="records")


class GoldAggregator(SparkRecordLoader):
    """Gera as agregações analíticas da camada Gold a partir dos registros Silver."""

    def _add_faixa_valor(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "faixa_valor",
            F.when(F.col("valor_total_estimado").isNull(), "Sem valor informado")
             .when(F.col("valor_total_estimado") <= 17600, "Ate R$17.600")
             .when(F.col("valor_total_estimado") <= 80000, "R$17.601 a R$80.000")
             .when(F.col("valor_total_estimado") <= 500000, "R$80.001 a R$500.000")
             .otherwise("Acima de R$500.000")
        )

    def agregar_por_area(self, df: DataFrame, periodo_inicio: str, periodo_fim: str) -> list[dict]:
        ts = datetime.now(timezone.utc).isoformat()
        return [
            {
                "periodo_inicio": periodo_inicio,
                "periodo_fim": periodo_fim,
                "uf": row["uf"],
                "ramo_mei": row["ramo_mei"],
                "total_contratacoes": row["total_contratacoes"],
                "valor_total": round(float(row["valor_total"] or 0), 2),
                "valor_maximo": round(float(row["valor_maximo"] or 0), 2),
                "data_atualizacao": ts,
            }
            for row in (
                df.groupBy("uf", "ramo_mei")
                .agg(
                    F.count("*").alias("total_contratacoes"),
                    F.sum("valor_total_estimado").alias("valor_total"),
                    F.max("valor_total_estimado").alias("valor_maximo"),
                )
                .orderBy("uf", F.desc("total_contratacoes"))
                .collect()
            )
        ]

    def agregar_por_estado(self, df: DataFrame, periodo_inicio: str, periodo_fim: str) -> list[dict]:
        ts = datetime.now(timezone.utc).isoformat()
        return [
            {
                "periodo_inicio": periodo_inicio,
                "periodo_fim": periodo_fim,
                "uf": row["uf"],
                "total_contratacoes": row["total_contratacoes"],
                "valor_total": round(float(row["valor_total"] or 0), 2),
                "orgaos_distintos": row["orgaos_distintos"],
                "data_atualizacao": ts,
            }
            for row in (
                df.groupBy("uf")
                .agg(
                    F.count("*").alias("total_contratacoes"),
                    F.sum("valor_total_estimado").alias("valor_total"),
                    F.countDistinct("cnpj_orgao").alias("orgaos_distintos"),
                )
                .orderBy(F.desc("total_contratacoes"))
                .collect()
            )
        ]

    def agregar_por_faixa(self, df: DataFrame, periodo_inicio: str, periodo_fim: str) -> list[dict]:
        ts = datetime.now(timezone.utc).isoformat()
        return [
            {
                "periodo_inicio": periodo_inicio,
                "periodo_fim": periodo_fim,
                "uf": row["uf"],
                "faixa_valor": row["faixa_valor"],
                "total_contratacoes": row["total_contratacoes"],
                "valor_total": round(float(row["valor_total"] or 0), 2),
                "data_atualizacao": ts,
            }
            for row in (
                self._add_faixa_valor(df)
                .groupBy("uf", "faixa_valor")
                .agg(
                    F.count("*").alias("total_contratacoes"),
                    F.sum("valor_total_estimado").alias("valor_total"),
                )
                .orderBy("uf", "faixa_valor")
                .collect()
            )
        ]

    def agregar_por_situacao(self, df: DataFrame, periodo_inicio: str, periodo_fim: str) -> list[dict]:
        ts = datetime.now(timezone.utc).isoformat()
        return [
            {
                "periodo_inicio": periodo_inicio,
                "periodo_fim": periodo_fim,
                "uf": row["uf"],
                "situacao_nome": row["situacao_nome"],
                "total_contratacoes": row["total_contratacoes"],
                "valor_total": round(float(row["valor_total"] or 0), 2),
                "data_atualizacao": ts,
            }
            for row in (
                df.groupBy("uf", "situacao_nome")
                .agg(
                    F.count("*").alias("total_contratacoes"),
                    F.sum("valor_total_estimado").alias("valor_total"),
                )
                .orderBy("uf", F.desc("total_contratacoes"))
                .collect()
            )
        ]

    def agregar_por_mes(self, df: DataFrame) -> list[dict]:
        ts = datetime.now(timezone.utc).isoformat()
        return [
            {
                "ano": row["ano"],
                "mes": row["mes"],
                "uf": row["uf"],
                "total_contratacoes": row["total_contratacoes"],
                "valor_total": round(float(row["valor_total"] or 0), 2),
                "orgaos_distintos": row["orgaos_distintos"],
                "data_atualizacao": ts,
            }
            for row in (
                df.withColumn("ano", F.col("ano_compra").cast("int"))
                .withColumn("mes", F.month(F.to_date(F.col("data_publicacao_pncp"))))
                .groupBy("uf", "ano", "mes")
                .agg(
                    F.count("*").alias("total_contratacoes"),
                    F.sum("valor_total_estimado").alias("valor_total"),
                    F.countDistinct("cnpj_orgao").alias("orgaos_distintos"),
                )
                .orderBy("uf", "ano", "mes")
                .collect()
            )
        ]

    def build(self, registros: list[dict], periodo_inicio: str, periodo_fim: str) -> dict[str, list[dict]]:
        df = self.load_from_records(registros)
        df.cache()
        try:
            return {
                "gold_area_de_servico": self.agregar_por_area(df, periodo_inicio, periodo_fim),
                "gold_estado":          self.agregar_por_estado(df, periodo_inicio, periodo_fim),
                "gold_faixa_de_valor":  self.agregar_por_faixa(df, periodo_inicio, periodo_fim),
                "gold_situacao":        self.agregar_por_situacao(df, periodo_inicio, periodo_fim),
                "gold_por_mes":         self.agregar_por_mes(df),
            }
        finally:
            df.unpersist()
