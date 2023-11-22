from typing import Optional
from uuid import UUID

from models.base import OrjsonBaseModel
from models.genre import ESGenre


class ESFilm(OrjsonBaseModel):
    id: UUID
    title: str
    imdb_rating: Optional[float]


class ESFilmPerson(OrjsonBaseModel):
    id: UUID
    name: str


class ESFilmFull(ESFilm):
    description: str
    genres: list[ESGenre]
    actors: list[ESFilmPerson]
    writers: list[ESFilmPerson]
    directors: list[ESFilmPerson]
