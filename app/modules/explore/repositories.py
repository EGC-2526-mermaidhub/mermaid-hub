import re

import unidecode
from sqlalchemy import or_

from app.modules.dataset.models import Author, DataSet, DiagramType, DSMetaData
from app.modules.dataset.services import TrendingDatasetsService
from app.modules.mermaiddiagram.models import MDMetaData, MermaidDiagram
from core.repositories.BaseRepository import BaseRepository


class ExploreRepository(BaseRepository):
    def __init__(self):
        super().__init__(DataSet)

    def filter(self, query="", sorting="newest", diagram_type="any", tags=[], **kwargs):
        # Normalize and remove unwanted characters
        normalized_query = unidecode.unidecode(query).lower()
        cleaned_query = re.sub(r'[,.":\'()\[\]^;!¡¿?]', "", normalized_query)

        filters = []
        for word in cleaned_query.split():
            filters.append(DSMetaData.title.ilike(f"%{word}%"))
            filters.append(DSMetaData.description.ilike(f"%{word}%"))
            filters.append(Author.name.ilike(f"%{word}%"))
            filters.append(Author.affiliation.ilike(f"%{word}%"))
            filters.append(Author.orcid.ilike(f"%{word}%"))
            filters.append(MDMetaData.mmd_filename.ilike(f"%{word}%"))
            filters.append(MDMetaData.title.ilike(f"%{word}%"))
            filters.append(MDMetaData.description.ilike(f"%{word}%"))
            filters.append(MDMetaData.publication_doi.ilike(f"%{word}%"))
            filters.append(MDMetaData.tags.ilike(f"%{word}%"))
            filters.append(DSMetaData.tags.ilike(f"%{word}%"))

        datasets = (
            self.model.query.join(DataSet.ds_meta_data)
            .join(DSMetaData.authors)
            .join(DataSet.mermaid_diagrams)
            .join(MermaidDiagram.md_meta_data)
            .filter(or_(*filters))
            .filter(DSMetaData.dataset_doi.isnot(None))
        )

        if diagram_type != "any":
            matching_type = None
            for member in DiagramType:
                if member.value.lower() == diagram_type:
                    matching_type = member
                    break
            if matching_type is not None:
                datasets = datasets.filter(DSMetaData.diagram_type == matching_type.name)

        if tags:
            datasets = datasets.filter(or_(*[DSMetaData.tags.ilike(f"%{tag}%") for tag in tags]))

        # Get distinct datasets first
        dataset_ids = [d[0] for d in datasets.with_entities(DataSet.id).distinct().all()]
        datasets = DataSet.query.filter(DataSet.id.in_(dataset_ids)).all() if dataset_ids else []

        # Order by sorting parameter
        if sorting == "oldest":
            datasets.sort(key=lambda x: x.created_at)
        elif sorting == "trending_week" or sorting == "trending_month":
            period = "week" if sorting == "trending_week" else "month"
            trending_service = TrendingDatasetsService()
            trending_tuples = trending_service.get_trending_datasets(limit=1000, period=period)
            trending_ids = [d[0].id for d in trending_tuples]  # d[0] is the DataSet
            id_to_dataset = {d.id: d for d in datasets}
            trending_filtered = [id_to_dataset[tid] for tid in trending_ids if tid in id_to_dataset]
            non_trending = [d for d in datasets if d.id not in trending_ids]
            datasets = trending_filtered + non_trending
        else:
            # Default to newest (descending)
            datasets.sort(key=lambda x: x.created_at, reverse=True)

        return datasets
