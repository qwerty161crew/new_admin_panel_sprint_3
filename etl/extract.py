from datetime import datetime, timedelta

import backoff
import psycopg2
from psycopg2.extras import DictCursor

from redis_develop import Redis
from query import REQUEST_ALL_DATA, REQUEST_ALL_DATA_WHERE
from settings import Settings

settings = Settings()


class PostgresExtractor:
    def __init__(self) -> None:
        self.connection = self.create_connection()
        self.stop = False
        self.redis = Redis(host=settings.redis_host, port=settings.redis_port)

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def create_connection(self):
        connection = psycopg2.connect(
            database=settings.pg_name,
            user=settings.pg_user,
            password=settings.pg_password,
            host=settings.pg_host,
            port=settings.pg_port,
            cursor_factory=DictCursor,
        )
        return connection

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def transfer_of_all_data(self):
        with self.connection.cursor() as cursor:
            limit = 100
            offset = 0
            while not self.stop:
                try:
                    cursor.execute(REQUEST_ALL_DATA.format(limit=limit, offset=offset))
                    queryset = cursor.fetchall()
                except psycopg2.OperationalError as error:
                    raise error
                if queryset == []:
                    self.stop = True
                else:
                    self.redis.save_sate(
                        queryset,
                        func_name="transfer_of_all_data",
                        state="collected",
                        table="movies",
                        many=True,
                    )
                    self.redis.create_chahe(
                        queryset, keyname_id="fw_id", tablename="film_work", many=False
                    )
                    offset += limit
                    yield queryset

    def save_person_film_work(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT pfw.*, p.full_name, p.id FROM content.person_film_work pfw LEFT JOIN content.person p ON p.id = pfw.person_id"
            )
            queryset = cursor.fetchall()
            self.redis.create_chahe(
                queryset,
                keyname_id="film_work_id",
                tablename="person_film_work",
                many=True,
            )

    def get_update(self, sql_request: str, tablename):
        with self.connection.cursor() as cursor:
            current_time = datetime.now()
            five_minutes_ago = current_time - timedelta(minutes=925)
            cursor.execute(sql_request.format(five_minutes_ago=five_minutes_ago))
            queryset = cursor.fetchall()
            if queryset:
                self.redis.create_chahe(
                    queryset,
                    keyname_id="film_work_id",
                    tablename=tablename,
                    many=True,
                )
                return queryset
            return False

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def transfer_of_data(self, ids):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(REQUEST_ALL_DATA_WHERE.format(ids=str(ids)))
                queryset = cursor.fetchall()
            except psycopg2.OperationalError as error:
                raise error
            self.redis.save_sate(
                queryset,
                func_name="transfer_of_all_data",
                state="collected",
                table="movies",
                many=True,
            )
            self.redis.create_chahe(
                queryset, keyname_id="fw_id", tablename="film_work", many=False
            )
            return queryset
