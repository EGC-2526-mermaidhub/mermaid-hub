from sqlalchemy import func

from app.modules.mermaiddiagram.models import MermaidDiagram, MDMetaData
from core.repositories.BaseRepository import BaseRepository


class MermaidDiagramRepository(BaseRepository):
    def __init__(self):
        super().__init__(MermaidDiagram)

    def count_mermaid_diagrams(self) -> int:
        max_id = self.model.query.with_entities(func.max(self.model.id)).scalar()
        return max_id if max_id is not None else 0


class MDMetaDataRepository(BaseRepository):
    def __init__(self):
        super().__init__(MDMetaData)
