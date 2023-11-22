from uuid import UUID

from models.base import OrjsonBaseModel


class ESGenre(OrjsonBaseModel):
    id: UUID
    name: str
