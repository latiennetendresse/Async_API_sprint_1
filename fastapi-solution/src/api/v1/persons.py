from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_cache.decorator import cache
from pydantic import Field

from api.v1.films import Film
from core.settings import settings
from models.base import OrjsonBaseModel
from models.person import ESPerson
from services.person import PersonService, get_person_service

router = APIRouter()


class PersonFilm(OrjsonBaseModel):
    uuid: UUID = Field(title='id фильма')
    roles: list[str] = Field(title='Роли в фильме')


class Person(OrjsonBaseModel):
    uuid: UUID = Field(title='id персоны')
    full_name: str = Field(title='Имя')
    films: list[PersonFilm] = Field(title='Фильмы')


def create_api_person(person: ESPerson) -> Person:
    return Person(
        uuid=person.id,
        full_name=person.full_name,
        films=[PersonFilm(uuid=pf.id, roles=pf.roles)
               for pf in person.films]
    )


@router.get(
    '/search',
    response_model=list[Person],
    summary='Поиск по персонам',
    description='Возвращает список персон по поисковому запросу.',
)
@cache(expire=settings.redis_cache_expire_seconds)
async def person_search(
    query: Annotated[str, Query(description='Поисковый запрос')],
    page_number: Annotated[int,
                           Query(description='Номер страницы', ge=1)] = 1,
    page_size: Annotated[int,
                         Query(description='Размер страницы', ge=1)] = 50,
    person_service: PersonService = Depends(get_person_service)
) -> list[Person]:
    persons = await person_service.search(query, page_number, page_size)
    return [
        create_api_person(person)
        for person in persons
    ]


@router.get(
    '/{person_id}',
    response_model=Person,
    summary='Информация о персоне',
    description='Позволяет получить информацию о персоне по id.',
)
@cache(expire=settings.redis_cache_expire_seconds)
async def person_details(
    person_id: UUID,
    person_service: PersonService = Depends(get_person_service)
) -> Person:
    person = await person_service.get_by_id(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='person not found')

    return create_api_person(person)


@router.get(
    '/{person_id}/film',
    response_model=list[Film],
    summary='Список фильмов персоны',
    description='Возвращает список фильмов по id персоны.',
)
@cache(expire=settings.redis_cache_expire_seconds)
async def person_films(
    person_id: UUID,
    person_service: PersonService = Depends(get_person_service)
) -> list[Film]:
    films = await person_service.list_films(person_id)
    return [
        Film(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating)
        for film in films
    ]
