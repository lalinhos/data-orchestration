from abc import ABC, abstractmethod
import certifi
from pymongo import MongoClient, ASCENDING


class Repository(ABC):
    """Interface para repositórios de persistência das camadas Bronze/Silver/Gold."""

    @abstractmethod
    def upsert_many(self, colecao: str, registros: list[dict], chave: list[str]) -> dict:
        ...

    @abstractmethod
    def count(self, colecao: str) -> int:
        ...


class MongoRepository(Repository):
    """Repositório MongoDB Atlas: upsert idempotente por chave de negócio."""

    def __init__(self, uri: str, db_name: str):
        self._client = MongoClient(uri, tlsCAFile=certifi.where())
        self._db = self._client[db_name]

    def upsert_many(self, colecao: str, registros: list[dict], chave: list[str]) -> dict:
        col = self._db[colecao]
        col.create_index([(campo, ASCENDING) for campo in chave], unique=True)

        inseridos = 0
        atualizados = 0
        for reg in registros:
            reg = {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in reg.items()}
            filtro = {k: reg[k] for k in chave}
            resultado = col.update_one(filtro, {"$set": reg}, upsert=True)
            if resultado.upserted_id:
                inseridos += 1
            elif resultado.modified_count:
                atualizados += 1

        print(f"MongoDB {colecao} -> inseridos: {inseridos} | atualizados: {atualizados}")
        return {"inseridos": inseridos, "atualizados": atualizados}

    def count(self, colecao: str) -> int:
        return self._db[colecao].count_documents({})
