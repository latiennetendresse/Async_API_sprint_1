import logging
from functools import lru_cache
from typing import Literal, Optional
from uuid import UUID

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from db.elastic import get_elastic
from models.film import ESFilm, ESFilmFull, ESFilmPerson
from models.genre import ESGenre
from models.person import ROLES
from services.pagination import es_search_from_size

logger = logging.getLogger(__name__)


class FilmService:
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get_by_id(self, film_id: UUID) -> Optional[ESFilmFull]:
        try:
            doc = await self.elastic.get(
                'movies', film_id, _source=list(ESFilmFull.__fields__.keys()))
        except NotFoundError:
            return None
        flat_fields = ['id', 'title', 'imdb_rating', 'description']
        nested_fields = {
            'genres': ESGenre,
            **{f'{role}s': ESFilmPerson for role in ROLES}
        }
        return ESFilmFull(
            **{field: doc['_source'][field] for field in flat_fields},
            **{
                field: [model(**item) for item in doc['_source'][field]]
                for field, model in nested_fields.items()
            },
        )

    async def search(self, query: str, page_number: int, page_size: int
                     ) -> list[ESFilm]:
        search_from, search_size = es_search_from_size(page_number, page_size)
        if not search_size:
            return []

        films_search = await self.elastic.search(
            body={
                "query": {
                    "match": {
                        "title": {
                            "query": query,
                            "fuzziness": "auto"
                        }
                    }
                },
                "_source": list(ESFilm.__fields__.keys()),
                "from": search_from,
                "size": search_size,
            },
            index='movies'
        )

        return [ESFilm(**doc['_source'])
                for doc in films_search['hits']['hits']]

    async def list(self,
                   genre_id: Optional[UUID],
                   sort_params: list[Literal[
                       'imdb_rating', '-imdb_rating',
                       'title', '-title']
                   ],
                   page_number: int, page_size: int) -> list[ESFilm]:
        search_from, search_size = es_search_from_size(page_number, page_size)
        if not search_size:
            return []

        query = {'match_all': {}}
        if genre_id:
            query = {
                "bool": {
                    "filter": {
                        "bool": {
                            "should": [
                                {
                                    "nested": {
                                        "path": "genres",
                                        "query": {
                                            "term": {
                                                "genres.id": str(genre_id)
                                            }
                                        }
                                    }
                                }
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                }
            }

        films_search = await self.elastic.search(
            body={
                "query": query,
                "sort": [FilmService.sort_param_query(param)
                         for param in sort_params],
                "_source": list(ESFilm.__fields__.keys()),
                "from": search_from,
                "size": search_size,
            },
            index='movies'
        )

        return [ESFilm(**doc['_source'])
                for doc in films_search['hits']['hits']]

    @staticmethod
    def sort_param_query(sort_param: str):
        field = sort_param.strip('-')
        # Text fields are not optimised for sorting. Use a keyword field.
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
        if field == 'title':
            field = 'title.raw'

        return {field: 'desc' if sort_param.startswith('-') else 'asc'}


@lru_cache()
def get_film_service(
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(elastic)
