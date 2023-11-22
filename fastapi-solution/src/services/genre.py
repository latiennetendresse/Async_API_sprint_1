from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from db.elastic import get_elastic
from models.genre import ESGenre


class GenreService:
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get_by_id(self, genre_id: str) -> Optional[ESGenre]:
        try:
            doc = await self.elastic.get('genres', genre_id)
        except NotFoundError:
            return None
        return ESGenre(**doc['_source'])

    async def get_all(self) -> list[ESGenre]:
        docs = await self.elastic.search(index='genres', body={
            'size': 100,
            'query': {
                'match_all': {}
            }
        })
        return [ESGenre(**doc['_source']) for doc in docs['hits']['hits']]


@lru_cache()
def get_genre_service(
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(elastic)
