from app.modules.mermaiddiagram.repositories import MermaidDiagramRepository, MDMetaDataRepository
from app.modules.hubfile.services import HubfileService
from core.services.BaseService import BaseService


class MermaidDiagramService(BaseService):
    def __init__(self):
        super().__init__(MermaidDiagramRepository())
        self.hubfile_service = HubfileService()

    def total_mermaid_diagram_views(self) -> int:
        return self.hubfile_service.total_hubfile_views()

    def total_mermaid_diagram_downloads(self) -> int:
        return self.hubfile_service.total_hubfile_downloads()

    def count_mermaid_diagrams(self):
        return self.repository.count_mermaid_diagrams()

    class FMMetaDataService(BaseService):
        def __init__(self):
            super().__init__(MDMetaDataRepository())
