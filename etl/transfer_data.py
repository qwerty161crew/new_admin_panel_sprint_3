import json
import time


from extract import PostgresExtractor
from dataloader import ElasticSearchLoader
from redis_develop import Redis
from settings import Settings

settings = Settings()


class DataTransform:
    def __init__(
        self,
        postgres_class: PostgresExtractor,
        elasticsearch_class: ElasticSearchLoader,
        redis: Redis,
    ):
        self.postgres_class = postgres_class
        self.elasticsearch_class = elasticsearch_class
        self.redis = redis
    
    def transform_data_all(self):
        mapping = []
        for filmworks in self.postgres_class.transfer_of_all_data():
            for filmwork in filmworks:

                person_film_work = self.redis.get_chache(
                    id=filmwork["fw_id"], name="person_film_work"
                )
                if person_film_work is not None:
                    person_film_work = json.loads(person_film_work)
                    mapping.append(
                        {
                            "id": filmwork["id"],
                            "imdb_rating": filmwork["rating"],
                            "genre": filmwork["name"],
                            "title": filmwork["title"],
                            "description": filmwork["description"],
                            "director": [
                                person["full_name"]
                                for person in person_film_work
                                if person["role"] == "director"
                            ],
                            "actors_names": [
                                person["full_name"]
                                for person in person_film_work
                                if person["role"] == "actor"
                            ],
                            "writers_names": [
                                person["full_name"]
                                for person in person_film_work
                                if person["role"] == "writer"
                            ],
                            "actors": [
                                {"id": actor["id"], "name": actor["full_name"]}
                                for actor in person_film_work
                                if actor["role"] == "actor"
                            ],
                            "writers": [
                                {"id": actor["id"], "name": actor["full_name"]}
                                for actor in person_film_work
                                if actor["role"] == "writer"
                            ],
                        }
                    )
                    self.redis.save_sate(
                        filmwork,
                        func_name="transfer_of_all_data",
                        state="transform",
                        table="movies",
                        many=False,
                    )
                else:
                    break
        self.elasticsearch_class.save_all_data(mapping)

    def get_update(self):
        edit_film_work_id = []
        person_request = """SELECT * FROM content.person p
        LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
        WHERE p.update_at > '{five_minutes_ago}'"""

        genre_request = """SELECT * FROM content.genre g
        LEFT JOIN content.genre_film_work gfw ON gfw.genre_id = g.id 
        WHERE g.update_at > '{five_minutes_ago}'"""
        for request in ((person_request, "person"), (genre_request, "genre")):
            queruset = self.postgres_class.get_update(request[0], request[1])
            if queruset:
                for film_work_data in queruset:
                    if film_work_data["film_work_id"] not in edit_film_work_id:
                        edit_film_work_id.append(film_work_data["film_work_id"])

        if edit_film_work_id != []:
            self.edit_film_work_id = tuple(id for id in edit_film_work_id)
            self._сonvert_changed_data(self.edit_film_work_id)

    def _сonvert_changed_data(self, film_work_ids):
        mapping = []
        filmworks = self.postgres_class.transfer_of_data(film_work_ids)
        for filmwork in filmworks:
            person_film_work = self.redis.get_chache(
                id=filmwork["fw_id"], name="person"
            )
            genre_film_work = self.redis.get_chache(id=filmwork["fw_id"], name="genre")
            if person_film_work is not None:
                person_film_work = json.loads(person_film_work)
                mapping.append(
                    {
                        "id": filmwork["id"],
                        "imdb_rating": filmwork["rating"],
                        "genre": [
                            genre
                            for genre in (
                                genre_film_work
                                if genre_film_work is not None
                                else filmwork["name"]
                            )
                        ],
                        "title": filmwork["title"],
                        "description": filmwork["description"],
                        "director": [
                            person["full_name"]
                            for person in person_film_work
                            if person["role"] == "director"
                        ],
                        "actors_names": [
                            person["full_name"]
                            for person in person_film_work
                            if person["role"] == "actor"
                        ],
                        "writers_names": [
                            person["full_name"]
                            for person in person_film_work
                            if person["role"] == "writer"
                        ],
                        "actors": [
                            {"id": actor["id"], "name": actor["full_name"]}
                            for actor in person_film_work
                            if actor["role"] == "actor"
                        ],
                        "writers": [
                            {"id": actor["id"], "name": actor["full_name"]}
                            for actor in person_film_work
                            if actor["role"] == "writer"
                        ],
                    }
                )
                self.redis.save_sate(
                    filmwork,
                    func_name="transfer_of_all_data",
                    state="transform",
                    table="movies",
                    many=False,
                )
            else:
                break
        self.elasticsearch_class.update_film(mapping)


if __name__ == "__main__":
    transform = DataTransform(
        redis=Redis(port=settings.redis_port, host=settings.redis_host),
        postgres_class=PostgresExtractor(),
        elasticsearch_class=ElasticSearchLoader(
            port=settings.els_port,
            host=settings.els_host,
            redis=Redis(port=settings.redis_port, host=settings.redis_host),
        ),
    )
    transform.transform_data_all()
    while True:
        transform.get_update()
        transform.redis.get_raw_data()
        time.sleep(310)
