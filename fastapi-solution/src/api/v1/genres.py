from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi_cache.decorator import cache
from pydantic import BaseModel

from core.settings import settings
from services.genre import GenreService, get_genre_service

router = APIRouter()


class Genre(BaseModel):
    uuid: UUID
    name: str


@router.get('/{genre_id}',
            response_model=Genre,
            summary='Информация о жанре.',
            description='Позволяет получить информацию о жанре по id.',
            response_description='Жанры фильмов')
@cache(expire=settings.redis_cache_expire_seconds)
async def genre_details(
        genre_id: str,
        genre_service: GenreService = Depends(get_genre_service)
) -> Genre:
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        # Если жанр не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail='genre not found')
    return Genre(uuid=genre.id, name=genre.name)


@router.get('',
            response_model=list[Genre],
            summary='Информация о жанрах.',
            description='Позволяет получить информацию о жанрах',
            response_description='Жанры фильмов')
@cache(expire=settings.redis_cache_expire_seconds)
async def all_genres(
        genre_service: GenreService = Depends(get_genre_service)
) -> list[Genre]:
    genres = await genre_service.get_all()
    return [Genre(uuid=genre.id, name=genre.name) for genre in genres]
