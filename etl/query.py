REQUEST_ALL_DATA = """
SELECT
fw.id as fw_id,
fw.title,
fw.description,
fw.rating,
fw.type,
fw.created_at,
fw.update_at,
pfw.role,
p.id,
p.full_name,
g.name,
CASE WHEN pfw.role = 'writer' THEN p.full_name ELSE NULL END as writer,
CASE WHEN pfw.role = 'director' THEN p.full_name ELSE NULL END as director,
CASE WHEN pfw.role = 'actor' THEN p.full_name ELSE NULL END as actor
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
LIMIT {limit}
OFFSET {offset}
"""


INDEX = {
    "settings": {
        "refresh_interval": "1s",
        "analysis": {
            "filter": {
                "english_stop": {"type": "stop", "stopwords": "_english_"},
                "english_stemmer": {"type": "stemmer", "language": "english"},
                "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english",
                },
                "russian_stop": {"type": "stop", "stopwords": "_russian_"},
                "russian_stemmer": {"type": "stemmer", "language": "russian"},
            },
            "analyzer": {
                "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer",
                    ],
                }
            },
        },
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "imdb_rating": {"type": "float"},
            "genre": {"type": "keyword"},
            "title": {
                "type": "text",
                "analyzer": "ru_en",
                "fields": {"raw": {"type": "keyword"}},
            },
            "description": {"type": "text", "analyzer": "ru_en"},
            "director": {"type": "text", "analyzer": "ru_en"},
            "actors_names": {"type": "text", "analyzer": "ru_en"},
            "writers_names": {"type": "text", "analyzer": "ru_en"},
            "actors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "ru_en"},
                },
            },
            "writers": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "ru_en"},
                },
            },
        },
    },
}
PERSON_REQUEST = """SELECT id, modified
FROM content.person
WHERE modified > {time}
ORDER BY modified
"""
PERSON_FILMWORK_REQUEST = """
SELECT fw.id, fw.modified
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
WHERE pfw.person_id IN (<id_всех_людей>)
ORDER BY fw.modified
LIMIT {limit}
OFFSET {offset} 
"""
REQUEST_ALL_DATA_WHERE = """
SELECT
    fw.id as fw_id, 
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.created_at, 
    fw.update_at, 
    pfw.role, 
    p.id, 
    p.full_name,
    g.name
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id IN {ids}
"""
REQUEST_GENRE_FILM_WORK = """
SELECT gfw.*, g.*, fw.id
FROM content.genre_film_work gfw
JOIN content.genre g ON gfw.genre_id = g.id 
JOIN content.film_work fw ON gfw.film_work_id = fw.id 
"""

PERSON_FILMWORK_REQUEST_UPDATE = """
SELECT fw.id, fw.modified
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
WHERE pfw.person_id IN (<id_всех_людей>)
ORDER BY fw.modified
LIMIT {limit}
OFFSET {offset} 
"""
