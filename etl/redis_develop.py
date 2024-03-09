import datetime
import backoff
import json
import redis
from redis.commands.search.field import TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from psycopg2.extras import DictRow
from typing import List


class Redis:
    def __init__(self, host, port, connect=None) -> None:
        self.host = host
        self.port = port
        self.connect = connect
        if self.connect is None:
            self.connect = self.create_connection()
        # self.create_schema()

    def create_connection(self) -> redis.Redis:
        connect = redis.Redis(host=self.host, port=self.port, decode_responses=True)
        return connect

    def create_schema(self):
        redis_index = self.connect.ft("idx:data")
        elastic_index = self.connect.ft("idx:film_work_id")
        schema = (
            TextField("$.id", as_name="id"),
            TextField("$.operation", as_name="operation"),
            TextField("$.state", as_name="state"),
            TextField("$.table", as_name="table"),
        )
        schema_elastic_search = (TextField("$.id_elastic", as_name="id_elastic"),)
        redis_index.create_index(
            schema,
            definition=IndexDefinition(prefix=["data:"], index_type=IndexType.JSON),
        )
        elastic_index.create_index(
            schema_elastic_search,
            definition=IndexDefinition(
                prefix=["film_work_id:"], index_type=IndexType.JSON
            ),
        )

    @backoff.on_exception(backoff.expo, redis.exceptions.DataError, max_time=2)
    def save_sate(self, film_work, func_name, state, table, many):
        if many:
            for data in film_work:
                if data["fw_id"] is not None:
                    query = {
                        "id": data["fw_id"],
                        "operation": func_name,
                        "state": state,
                        "table": table,
                    }
                    try:
                        self.connect.hset(f'data:{data["id"]}', mapping=query)
                    except redis.exceptions.DataError as error:
                        raise error
        else:
            query = {
                "id": film_work["id"],
                "operation": func_name,
                "state": state,
                "table": table,
            }
            try:
                self.connect.hset(f'data:{film_work["id"]}', mapping=query)
            except redis.exceptions.DataError as error:
                raise error

    def create_chahe(
        self, film_work_dates: List[DictRow], keyname_id, tablename, many: bool
    ):
        if not many:
            for row in film_work_dates:
                row["created_at"] = row["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                if row.get("update_at"):
                    row["update_at"] = row["update_at"].strftime("%Y-%m-%d %H:%M:%S")
                self.connect.set(
                    f"{tablename}:{row[keyname_id]}", json.dumps(dict(row))
                )
        else:
            self._save_many_dates(film_work_dates, keyname_id, tablename)

    def _save_many_dates(self, film_work_dates, keyname_id, tablename):
        unique_ids = tuple(d[keyname_id] for d in film_work_dates)
        for id_value in unique_ids:
            redis_key = f"{tablename}:{id_value}"
            values = [
                {
                    **dict(item),
                    "created_at": dict(item)["created_at"].strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "update_at": (
                        dict(item)["update_at"].strftime("%Y-%m-%d %H:%M:%S")
                        if dict(item).get("update_at")
                        else None
                    ),
                }
                for item in film_work_dates
                if dict(item)[keyname_id] == id_value
            ]
            self.connect.set(redis_key, json.dumps(values))

    def get_chache(self, id, name):
        return self.connect.get(f"{name}:{id}")

    def save_id_elascit_search(self, movie_id, elastic_id):
        print(movie_id, elastic_id)
        self.connect.set(f"film_work_id:{movie_id}", elastic_id)
