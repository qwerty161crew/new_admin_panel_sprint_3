import json
from typing import List

import requests

from query import INDEX
from redis_develop import Redis


class ElasticSearchLoader:
    def __init__(self, host, port, redis: Redis):
        self.host = host
        self.port = port
        self.redis = redis

    def create_index(self):
        url = f"http://127.0.0.1:9200/movies"
        headers = {"Content-Type": "application/json"}
        requests.put(url, headers=headers, json=INDEX)

    def save_all_data(self, movies_data: List[dict]):
        url = f"http://127.0.0.1:9200/movies/_doc"
        headers = {"Content-Type": "application/json"}
        for data in movies_data:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 201:
                self.redis.save_sate(
                    data,
                    func_name="transfer_of_all_data",
                    state="loaded",
                    table="movies",
                    many=False,
                )
                self.redis.save_id_elascit_search(
                    elastic_id=response.json()["_id"], movie_id=data["id"]
                )

    def update_film(self, movies_data: List[dict]):
        url = "http://127.0.0.1:9200/movies/_doc/{id}"
        headers = {"Content-Type": "application/json"}
        for data in movies_data:
            elastic = self.redis.get_chache(id=data["id"], name="film_work_id")
            response = requests.post(
                url.format(id=elastic), headers=headers, json=data
            )
            if response.status_code == 201:
                self.redis.save_sate(
                    data,
                    func_name="transfer_of_all_data",
                    state="loaded",
                    table="movies",
                    many=False,
                )
