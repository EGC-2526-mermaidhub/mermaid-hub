import hashlib
import logging
import os
import shutil
import uuid
from typing import Optional

from flask import request, url_for
from sqlalchemy import func

from app import db
from app.modules.auth.services import AuthenticationService
from app.modules.dataset.models import DataSet, DSDownloadRecord, DSMetaData, DSViewRecord
from app.modules.dataset.repositories import (
    AuthorRepository,
    DataSetRepository,
    DOIMappingRepository,
    DSDownloadRecordRepository,
    DSMetaDataRepository,
    DSViewRecordRepository,
    TrendingDatasetsRepository,
)
from app.modules.hubfile.repositories import HubfileDownloadRecordRepository, HubfileRepository, HubfileViewRecordRepository
from app.modules.mermaiddiagram.repositories import MDMetaDataRepository, MermaidDiagramRepository
from core.services.BaseService import BaseService

logger = logging.getLogger(__name__)


def calculate_checksum_and_size(file_path):
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as file:
        content = file.read()
        hash_sha256 = hashlib.sha256(content).hexdigest()
        return hash_sha256, file_size


class DataSetService(BaseService):
    def __init__(self):
        super().__init__(DataSetRepository())
        self.mermaid_diagram_repository = MermaidDiagramRepository()
        self.author_repository = AuthorRepository()
        self.dsmetadata_repository = DSMetaDataRepository()
        self.mdmetadata_repository = MDMetaDataRepository()
        self.dsdownloadrecord_repository = DSDownloadRecordRepository()
        self.hubfiledownloadrecord_repository = HubfileDownloadRecordRepository()
        self.hubfilerepository = HubfileRepository()
        self.dsviewrecord_repostory = DSViewRecordRepository()
        self.hubfileviewrecord_repository = HubfileViewRecordRepository()

    def move_mermaid_diagrams(self, dataset: DataSet):
        current_user = AuthenticationService().get_authenticated_user()
        source_dir = current_user.temp_folder()

        working_dir = os.getenv("WORKING_DIR", "")
        dest_dir = os.path.join(working_dir, "uploads", f"user_{current_user.id}", f"dataset_{dataset.id}")

        os.makedirs(dest_dir, exist_ok=True)

        for mermaid_diagram in dataset.mermaid_diagrams:
            mmd_filename = mermaid_diagram.md_meta_data.mmd_filename
            shutil.move(os.path.join(source_dir, mmd_filename), dest_dir)

    def get_synchronized(self, current_user_id: int) -> DataSet:
        return self.repository.get_synchronized(current_user_id)

    def get_unsynchronized(self, current_user_id: int) -> DataSet:
        return self.repository.get_unsynchronized(current_user_id)

    def get_unsynchronized_dataset(self, current_user_id: int, dataset_id: int) -> DataSet:
        return self.repository.get_unsynchronized_dataset(current_user_id, dataset_id)

    def latest_synchronized(self):
        datasets = self.repository.latest_synchronized()
        for dataset in datasets:
            dataset.download_count = self.dataset_downloads_id(dataset.id)
        return datasets

    def register_download(self, dataset_id: int, user_id: int = None) -> int:
        import uuid

        from flask import request

        download_cookie = request.cookies.get("download_cookie")
        if not download_cookie:
            download_cookie = str(uuid.uuid4())

        self.dsdownloadrecord_repository.register_download(
            dataset_id=dataset_id, user_id=user_id, download_cookie=download_cookie
        )

        return self.dataset_downloads_id(dataset_id)

    def get_download_count(self, dataset_id):
        count = db.session.query(func.count(DSDownloadRecord.id)).filter(DSDownloadRecord.dataset_id == dataset_id).scalar()
        return count or 0

    def count_synchronized_datasets(self):
        return self.repository.count_synchronized_datasets()

    def count_mermaid_diagrams(self):
        return self.mermaid_diagram_service.count_mermaid_diagrams()

    def count_authors(self) -> int:
        return self.author_repository.count()

    def count_dsmetadata(self) -> int:
        return self.dsmetadata_repository.count()

    def total_dataset_downloads(self) -> int:
        return self.dsdownloadrecord_repository.total_dataset_downloads()

    def dataset_downloads_id(self, dataset_id) -> int:
        return self.dsdownloadrecord_repository.dataset_downloads_id(dataset_id)

    def total_dataset_views(self) -> int:
        return self.dsviewrecord_repostory.total_dataset_views()

    def create_from_form(self, form, current_user, is_draft: bool = True) -> DataSet:
        main_author = {
            "name": f"{current_user.profile.surname}, {current_user.profile.name}",
            "affiliation": current_user.profile.affiliation,
            "orcid": current_user.profile.orcid,
        }
        try:
            dsmetadata_kwargs = form.get_dsmetadata()

            dsmetadata_kwargs["is_draft"] = is_draft

            logger.info(f"Creating dsmetadata...: {dsmetadata_kwargs}")

            dsmetadata = self.dsmetadata_repository.create(**dsmetadata_kwargs)

            for author_data in [main_author] + form.get_authors():
                author = self.author_repository.create(commit=False, ds_meta_data_id=dsmetadata.id, **author_data)
                dsmetadata.authors.append(author)

            dataset = self.create(commit=False, user_id=current_user.id, ds_meta_data_id=dsmetadata.id)

            for mermaid_diagram in form.mermaid_diagrams:
                mmd_filename = mermaid_diagram.mmd_filename.data
                mdmetadata = self.mdmetadata_repository.create(commit=False, **mermaid_diagram.get_mdmetadata())
                for author_data in mermaid_diagram.get_authors():
                    author = self.author_repository.create(commit=False, md_meta_data_id=mdmetadata.id, **author_data)
                    mdmetadata.authors.append(author)

                md = self.mermaid_diagram_repository.create(commit=False, data_set_id=dataset.id, md_meta_data_id=mdmetadata.id)

                file_path = os.path.join(current_user.temp_folder(), mmd_filename)
                checksum, size = calculate_checksum_and_size(file_path)

                file = self.hubfilerepository.create(
                    commit=False, name=mmd_filename, checksum=checksum, size=size, mermaid_diagram_id=md.id
                )
                md.files.append(file)
            self.repository.session.commit()
        except Exception as exc:
            logger.info(f"Exception creating dataset from form...: {exc}")
            self.repository.session.rollback()
            raise exc
        return dataset

    def update_dsmetadata(self, id, **kwargs):
        return self.dsmetadata_repository.update(id, **kwargs)

    def publish(self, dataset):
        zenodo_result = self.zenodo_service.publish_dataset(dataset)

        dataset.ds_meta_data.is_draft = False
        dataset.ds_meta_data.publication_doi = zenodo_result.publication_doi
        dataset.ds_meta_data.dataset_doi = zenodo_result.dataset_doi
        dataset.ds_meta_data.deposition_id = zenodo_result.deposition_id

        self.db.session.commit()

    def get_mermaidhub_doi(self, dataset: DataSet) -> str:
        try:
            # Build an absolute URL using Flask's url_for so it works both locally and when deployed
            return url_for("dataset.subdomain_index", doi=dataset.ds_meta_data.dataset_doi, _external=True)
        except Exception:
            # Fallback to DOMAIN env var for contexts where url_for is not available
            domain = os.getenv("DOMAIN", "localhost")
            return f"http://{domain}/doi/{dataset.ds_meta_data.dataset_doi}"

    def diagram_similarity(self, ds1, ds2):
        return 1 if ds1.ds_meta_data.diagram_type == ds2.ds_meta_data.diagram_type else 0

    def tag_similarity(self, ds1, ds2):
        tags1 = set((ds1.ds_meta_data.tags or "").replace(",", " ").split())
        tags2 = set((ds2.ds_meta_data.tags or "").replace(",", " ").split())
        return len(tags1.intersection(tags2))

    def author_similarity(self, ds1, ds2):
        a1 = {a.name for a in ds1.ds_meta_data.authors}
        a2 = {a.name for a in ds2.ds_meta_data.authors}
        return len(a1.intersection(a2))

    def get_popularity(self, dataset_id):
        views = self.dsviewrecord_repostory.model.query.filter_by(dataset_id=dataset_id).count()
        downloads = self.dsdownloadrecord_repository.model.query.filter_by(dataset_id=dataset_id).count()
        return views + downloads

    def recommend_simple(self, dataset_id, top_n=3):
        datasets = self.repository.model.query.all()

        target = next((ds for ds in datasets if ds.id == dataset_id), None)
        if not target:
            return []

        popularity_list = []
        for ds in datasets:
            pop = self.get_popularity(ds.id)
            popularity_list.append(pop)

        max_popularity = max(popularity_list) or 1

        results = []

        for ds, pop in zip(datasets, popularity_list):
            if ds.id == dataset_id:
                continue

            diag_sim = self.diagram_similarity(target, ds)
            tag_sim = self.tag_similarity(target, ds)
            author_sim = self.author_similarity(target, ds)
            popularity_score = 0.5 * (pop / max_popularity)

            score = 3 * diag_sim + 1 * tag_sim + 1 * author_sim + popularity_score

            results.append((ds, score))

        results.sort(key=lambda x: x[1], reverse=True)

        return [ds for ds, s in results[:top_n]]


class AuthorService(BaseService):
    def __init__(self):
        super().__init__(AuthorRepository())


class DSDownloadRecordService(BaseService):
    def __init__(self):
        super().__init__(DSDownloadRecordRepository())

    def get_download_count(self, dataset_id: int) -> int:
        count = db.session.query(func.count(DSDownloadRecord.id)).filter(DSDownloadRecord.dataset_id == dataset_id).scalar()
        return count or 0


class DSMetaDataService(BaseService):
    def __init__(self):
        super().__init__(DSMetaDataRepository())

    def update(self, id, **kwargs):
        return self.repository.update(id, **kwargs)

    def filter_by_doi(self, doi: str) -> Optional[DSMetaData]:
        return self.repository.filter_by_doi(doi)


class DSViewRecordService(BaseService):
    def __init__(self):
        super().__init__(DSViewRecordRepository())

    def the_record_exists(self, dataset: DataSet, user_cookie: str):
        return self.repository.the_record_exists(dataset, user_cookie)

    def create_new_record(self, dataset: DataSet, user_cookie: str) -> DSViewRecord:
        return self.repository.create_new_record(dataset, user_cookie)

    def create_cookie(self, dataset: DataSet) -> str:

        user_cookie = request.cookies.get("view_cookie")
        if not user_cookie:
            user_cookie = str(uuid.uuid4())

        existing_record = self.the_record_exists(dataset=dataset, user_cookie=user_cookie)

        if not existing_record:
            self.create_new_record(dataset=dataset, user_cookie=user_cookie)

        return user_cookie


class DOIMappingService(BaseService):
    def __init__(self):
        super().__init__(DOIMappingRepository())

    def get_new_doi(self, old_doi: str) -> str:
        doi_mapping = self.repository.get_new_doi(old_doi)
        if doi_mapping:
            return doi_mapping.dataset_doi_new
        else:
            return None


class SizeService:

    def __init__(self):
        pass

    def get_human_readable_size(self, size: int) -> str:
        if size < 1024:
            return f"{size} bytes"
        elif size < 1024**2:
            return f"{round(size / 1024, 2)} KB"
        elif size < 1024**3:
            return f"{round(size / (1024 ** 2), 2)} MB"

        else:
            return f"{round(size / (1024 ** 3), 2)} GB"


class TrendingDatasetsService(BaseService):
    def __init__(self):
        super().__init__(TrendingDatasetsRepository())

    def get_trending_datasets(self, limit: int = 10, period: str = "week") -> list:
        period_days = self._get_period_days(period)
        return self.repository.get_top_downloaded_datasets(limit=limit, period_days=period_days)

    def get_trending_datasets_metadata(self, limit: int = 10, period: str = "week") -> list:
        period_days = self._get_period_days(period)
        return self.repository.get_top_downloaded_datasets_metadata(limit=limit, period_days=period_days)

    def get_weekly_trending_datasets(self, limit: int = 10) -> list:
        return self.get_trending_datasets(limit=limit, period="week")

    def get_monthly_trending_datasets(self, limit: int = 10) -> list:
        return self.get_trending_datasets(limit=limit, period="month")

    def get_weekly_trending_datasets_metadata(self, limit: int = 10) -> list:
        return self.get_trending_datasets_metadata(limit=limit, period="week")

    def get_monthly_trending_datasets_metadata(self, limit: int = 10) -> list:
        return self.get_trending_datasets_metadata(limit=limit, period="month")

    def _get_period_days(self, period: str) -> int:
        period_mapping = {"week": 7, "month": 30}

        if period not in period_mapping:
            raise ValueError(f"Invalid period '{period}'. Must be 'week' or 'month'.")

        return period_mapping[period]
