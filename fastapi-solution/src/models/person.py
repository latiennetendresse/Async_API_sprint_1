from uuid import UUID

from models.base import OrjsonBaseModel

ROLES = ['actor', 'writer', 'director']


class ESPersonFilm(OrjsonBaseModel):
    id: UUID
    roles: list[str]


class ESPerson(OrjsonBaseModel):
    id: UUID
    full_name: str
    films: list[ESPersonFilm]
