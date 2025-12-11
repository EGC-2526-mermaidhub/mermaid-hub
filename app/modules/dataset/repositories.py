import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from flask_login import current_user
from sqlalchemy import desc, func

from app.modules.dataset.models import Author, DataSet, DOIMapping, DSDownloadRecord, DSMetaData, DSViewRecord
from core.repositories.BaseRepository import BaseRepository

logger = logging.getLogger(__name__)


class AuthorRepository(BaseRepository):
    def __init__(self):
        super().__init__(Author)


class DSDownloadRecordRepository(BaseRepository):
    def __init__(self):
        super().__init__(DSDownloadRecord)

    def total_dataset_downloads(self) -> int:
        max_id = self.model.query.with_entities(func.max(self.model.id)).scalar()
        return max_id if max_id is not None else 0

    def dataset_downloads_id(self, dataset_id: int) -> int:
        count = self.model.query.filter(self.model.dataset_id == dataset_id).count()
        return count

    def register_download(self, dataset_id: int, user_id: int = None, download_cookie: str = None) -> DSDownloadRecord:

        if not download_cookie:
            download_cookie = str(uuid.uuid4())

        download_record = self.create(
            dataset_id=dataset_id,
            user_id=user_id,
            download_cookie=download_cookie,
            download_date=datetime.utcnow(),
        )

        return download_record


class DSMetaDataRepository(BaseRepository):
    def __init__(self):
        super().__init__(DSMetaData)

    def filter_by_doi(self, doi: str) -> Optional[DSMetaData]:
        return self.model.query.filter_by(dataset_doi=doi).first()


class DSViewRecordRepository(BaseRepository):
    def __init__(self):
        super().__init__(DSViewRecord)

    def total_dataset_views(self) -> int:
        max_id = self.model.query.with_entities(func.max(self.model.id)).scalar()
        return max_id if max_id is not None else 0

    def the_record_exists(self, dataset: DataSet, user_cookie: str):
        return self.model.query.filter_by(
            user_id=current_user.id if current_user.is_authenticated else None,
            dataset_id=dataset.id,
            view_cookie=user_cookie,
        ).first()

    def create_new_record(self, dataset: DataSet, user_cookie: str) -> DSViewRecord:
        return self.create(
            user_id=current_user.id if current_user.is_authenticated else None,
            dataset_id=dataset.id,
            view_date=datetime.now(timezone.utc),
            view_cookie=user_cookie,
        )


class DataSetRepository(BaseRepository):
    def __init__(self):
        super().__init__(DataSet)

    def get_synchronized(self, current_user_id: int) -> DataSet:
        return (
            self.model.query.join(DSMetaData)
            .filter(DataSet.user_id == current_user_id, DSMetaData.is_draft == 0)
            .order_by(self.model.created_at.desc())
            .all()
        )

    def get_unsynchronized(self, current_user_id: int) -> DataSet:
        return (
            self.model.query.join(DSMetaData)
            .filter(DataSet.user_id == current_user_id, DSMetaData.is_draft == 1)
            .order_by(self.model.created_at.desc())
            .all()
        )

    def get_unsynchronized_dataset(self, current_user_id: int, dataset_id: int) -> DataSet:
        return (
            self.model.query.join(DSMetaData)
            .filter(DataSet.user_id == current_user_id, DataSet.id == dataset_id, DSMetaData.is_draft == 1)
            .first()
        )

    def count_synchronized_datasets(self):
        return self.model.query.join(DSMetaData).filter(DSMetaData.is_draft == 0).count()

    def count_unsynchronized_datasets(self):
        return self.model.query.join(DSMetaData).filter(DSMetaData.is_draft == 1).count()

    def latest_synchronized(self):
        return self.model.query.join(DSMetaData).filter(DSMetaData.is_draft == 0).order_by(desc(self.model.id)).limit(5).all()


class DOIMappingRepository(BaseRepository):
    def __init__(self):
        super().__init__(DOIMapping)

    def get_new_doi(self, old_doi: str) -> str:
        return self.model.query.filter_by(dataset_doi_old=old_doi).first()


class TrendingDatasetsRepository(BaseRepository):
    def __init__(self):
        super().__init__(DataSet)

    def get_top_downloaded_datasets(self, limit: int = 10, period_days: int = None) -> List[tuple]:
        query = (
            self.model.query.join(DSMetaData, DataSet.ds_meta_data_id == DSMetaData.id)
            .join(DSDownloadRecord, DataSet.id == DSDownloadRecord.dataset_id)
            .filter(DSMetaData.dataset_doi.isnot(None))  # Only synchronized datasets
        )

        # Apply period filter only if period_days is specified (None = all time)
        if period_days is not None:
            start_date = datetime.now(timezone.utc) - timedelta(days=period_days)
            query = query.filter(DSDownloadRecord.download_date >= start_date)

        results = (
            query.with_entities(DataSet, func.count(DSDownloadRecord.id).label("download_count"))
            .group_by(DataSet.id)
            .order_by(desc("download_count"))
            .limit(limit)
            .all()
        )

        return results

    def get_top_downloaded_datasets_metadata(self, limit: int = 10, period_days: int = None) -> List[dict]:
        results = self.get_top_downloaded_datasets(limit=limit, period_days=period_days)

        trending_datasets = []
        for dataset, download_count in results:
            trending_datasets.append(
                {
                    "id": dataset.id,
                    "title": dataset.ds_meta_data.title,
                    "description": dataset.ds_meta_data.description,
                    "diagram_type": dataset.get_cleaned_diagram_type(),
                    "dataset_doi": dataset.ds_meta_data.dataset_doi,
                    "publication_doi": dataset.ds_meta_data.publication_doi,
                    "tags": dataset.ds_meta_data.tags,
                    "created_at": dataset.created_at,
                    "download_count": download_count,
                    "user_id": dataset.user_id,
                    "files_count": dataset.get_files_count(),
                    "total_size": dataset.get_file_total_size(),
                    "total_size_human": dataset.get_file_total_size_for_human(),
                    "doi": dataset.get_mermaidhub_doi(),  # Add full URL for linking
                }
            )

        return trending_datasets
