import logging
from functools import lru_cache
from typing import Optional
from uuid import UUID

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from db.elastic import get_elastic
from models.film import ESFilm
from models.person import ROLES, ESPerson, ESPersonFilm
from services.pagination import es_search_from_size

logger = logging.getLogger(__name__)


class PersonService:
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get_by_id(self, person_id: UUID) -> Optional[ESPerson]:
        try:
            persons_get = await self.elastic.get('persons', person_id)
        except NotFoundError:
            return None

        films_search = await self._search_person_films(
            [person_id],
            ['id', *[f'{role}s.id' for role in ROLES]]
        )
        return ESPerson(films=self._get_person_films(person_id, films_search),
                        **persons_get['_source'])

    async def search(self, query: str, page_number: int, page_size: int
                     ) -> list[ESPerson]:
        search_from, search_size = es_search_from_size(page_number, page_size)
        if not search_size:
            return []

        persons_search = await self.elastic.search(
            body={
                "query": {
                    "match": {
                        "full_name": {
                            "query": query,
                            "fuzziness": "auto"
                        }
                    }
                },
                "from": search_from,
                "size": search_size,
            },
            index='persons'
        )

        person_ids = list(
            map(lambda person_doc: person_doc['_source']['id'],
                persons_search['hits']['hits'])
        )
        films_search = await self._search_person_films(
            person_ids,
            ['id', *[f'{role}s.id' for role in ROLES]]
        )
        return [
            ESPerson(
                films=self._get_person_films(person_doc['_source']['id'],
                                             films_search),
                **person_doc['_source']
            )
            for person_doc in persons_search['hits']['hits']
        ]

    async def list_films(self, person_id: UUID) -> list[ESFilm]:
        films_search = await self._search_person_films(
            [person_id],
            ['id', 'title', 'imdb_rating']
        )
        return [
            ESFilm(
                id=film['fields']['id'][0],
                title=film['fields']['title'][0],
                imdb_rating=film['fields']['imdb_rating'][0]
            )
            for film in films_search['hits']['hits']
        ]

    async def _search_person_films(self, person_ids: list[UUID],
                                   fields: list[str]):
        return await self.elastic.search(
            body={
                "query": {
                    "bool": {
                        "filter": {
                            "bool": {
                                "should": [
                                    {
                                        "nested": {
                                            "path": f'{role}s',
                                            "query": {
                                                "terms": {
                                                    f'{role}s.id': person_ids
                                                }
                                            }
                                        }
                                    }
                                    for role in ROLES
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    }
                },
                "fields": fields,
                "_source": False,
                # Предполагается, что запрошенные персоны суммарно участвовали
                # не более, чем в 10000 фильмах. Потенциально пагинация с from
                # и size даёт просмотреть до 10000 значений:
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
                "size": 10000,
            },
            index='movies'
        )

    def _get_person_films(self, person_id: UUID, films_search
                          ) -> list[ESPersonFilm]:
        return [
            ESPersonFilm(
                id=film['fields']['id'][0],
                roles=roles
            )
            for film in films_search['hits']['hits']
            if (roles := self._get_person_roles(person_id, film))
        ]

    def _get_person_roles(self, person_id: UUID, film) -> list[str]:
        return [
            role for role in ROLES
            if str(person_id) in self._get_film_role_person_ids(film, role)
        ]

    def _get_film_role_person_ids(self, film, role: str) -> list[UUID]:
        return [p['id'][0] for p in film['fields'].get(f'{role}s', [])]


@lru_cache()
def get_person_service(
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(elastic)
