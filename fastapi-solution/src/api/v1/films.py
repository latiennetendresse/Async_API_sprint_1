import logging
from http import HTTPStatus
from typing import Annotated, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_cache.decorator import cache
from pydantic import Field

from api.v1.genres import Genre
from core.settings import settings
from models.base import OrjsonBaseModel
from models.person import ROLES
from services.film import FilmService, get_film_service

logger = logging.getLogger(__name__)

router = APIRouter()


class Film(OrjsonBaseModel):
    uuid: UUID = Field(title='id фильма')
    title: str = Field(title='Название')
    imdb_rating: Optional[float] = Field('Рейтинг')


class FilmPerson(OrjsonBaseModel):
    uuid: UUID = Field(title='id персоны')
    full_name: str = Field(title='Имя')


class FilmFull(Film):
    description: str = Field(title='Описание')
    genre: list[Genre] = Field(title='Жанры')
    actors: list[FilmPerson] = Field(title='Актёры')
    writers: list[FilmPerson] = Field(title='Сценаристы')
    directors: list[FilmPerson] = Field(title='Режиссёры')


@router.get(
    '',
    response_model=list[Film],
    summary='Список фильмов',
    description='Возвращает список фильмов с учётом фильтров и сортировки.',
)
@cache(expire=settings.redis_cache_expire_seconds)
async def film_list(
    genre: Annotated[Optional[UUID], Query(description='id жанра')] = None,
    sort: Annotated[
        list[Literal['imdb_rating', '-imdb_rating',
                     'title', '-title']],
        Query(description='Сортировка')
    ] = [],
    page_number: Annotated[int,
                           Query(description='Номер страницы', ge=1)] = 1,
    page_size: Annotated[int,
                         Query(description='Размер страницы', ge=1)] = 50,
    film_service: FilmService = Depends(get_film_service)
) -> list[Film]:
    films = await film_service.list(genre, sort, page_number, page_size)
    return [
        Film(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating)
        for film in films
    ]


@router.get(
    '/search',
    response_model=list[Film],
    summary='Поиск по фильмам',
    description='Возвращает список фильмов по поисковому запросу.',
)
@cache(expire=settings.redis_cache_expire_seconds)
async def film_search(
    query: Annotated[str, Query(description='Поисковый запрос')],
    page_number: Annotated[int,
                           Query(description='Номер страницы', ge=1)] = 1,
    page_size: Annotated[int,
                         Query(description='Размер страницы', ge=1)] = 50,
    film_service: FilmService = Depends(get_film_service)
) -> list[Film]:
    films = await film_service.search(query, page_number, page_size)
    return [
        Film(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating)
        for film in films
    ]


@router.get(
        '/{film_id}',
        response_model=FilmFull,
        summary='Информация о фильме',
        description='Позволяет получить информацию о фильме по id.',
)
@cache(expire=settings.redis_cache_expire_seconds)
async def film_details(
    film_id: UUID,
    film_service: FilmService = Depends(get_film_service)
) -> FilmFull:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='film not found')

    return FilmFull(
        uuid=film.id, title=film.title, imdb_rating=film.imdb_rating,
        description=film.description,
        genre=[Genre(uuid=genre.id, name=genre.name) for genre in film.genres],
        **{
            f'{role}s': [FilmPerson(uuid=fp.id, full_name=fp.name)
                         for fp in getattr(film, f'{role}s')]
            for role in ROLES
        }
    )
