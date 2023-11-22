import logging.config

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis.asyncio import Redis

from api.v1 import films, genres, persons
from core.logging import LOGGING
from core.settings import settings
from db import elastic, redis

logging.config.dictConfig(LOGGING)
logger = logging.getLogger(__name__)

app = FastAPI(
    title='movies',
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
)


@app.on_event('startup')
async def startup():
    redis.redis = Redis.from_url(settings.redis_dsn)
    FastAPICache.init(RedisBackend(redis.redis), prefix='fastapi-cache')
    elastic.es = AsyncElasticsearch(hosts=[settings.elastic_dsn])


@app.on_event('shutdown')
async def shutdown():
    await redis.redis.close()
    await elastic.es.close()


app.include_router(films.router, prefix='/api/v1/films', tags=['Фильмы'])
app.include_router(persons.router, prefix='/api/v1/persons', tags=['Персоны'])
app.include_router(genres.router, prefix='/api/v1/genres', tags=['Жанры'])
