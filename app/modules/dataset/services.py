import hashlib
import logging
import os
import shutil
import uuid
from typing import Optional

from flask import request, url_for

from app.modules.auth.services import AuthenticationService
from app.modules.dataset.models import DataSet, DSMetaData, DSViewRecord
from app.modules.dataset.repositories import (
    AuthorRepository,
    DataSetRepository,
    DOIMappingRepository,
    DSDownloadRecordRepository,
    DSMetaDataRepository,
    DSViewRecordRepository,
)
from app.modules.mermaiddiagram.repositories import MermaidDiagramRepository, MDMetaDataRepository
from app.modules.hubfile.repositories import (
    HubfileDownloadRecordRepository,
    HubfileRepository,
    HubfileViewRecordRepository,
)
from core.services.BaseService import BaseService

from app import db
from sqlalchemy import func
from app.modules.dataset.models import DSDownloadRecord

logger = logging.getLogger(__name__)


def calculate_checksum_and_size(file_path):
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as file:
        content = file.read()
        hash_md5 = hashlib.md5(content).hexdigest()
        return hash_md5, file_size


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
        from flask import request
        import uuid

        download_cookie = request.cookies.get("download_cookie")
        if not download_cookie:
            download_cookie = str(uuid.uuid4())

        self.dsdownloadrecord_repository.register_download(
            dataset_id=dataset_id, user_id=user_id, download_cookie=download_cookie
        )

        return self.dataset_downloads_id(dataset_id)

    def get_download_count(self, dataset_id):
        count = (
            db.session.query(func.count(DSDownloadRecord.id)).filter(DSDownloadRecord.dataset_id == dataset_id).scalar()
        )
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

    def create_from_form(self, form, current_user) -> DataSet:
        main_author = {
            "name": f"{current_user.profile.surname}, {current_user.profile.name}",
            "affiliation": current_user.profile.affiliation,
            "orcid": current_user.profile.orcid,
        }
        try:
            logger.info(f"Creating dsmetadata...: {form.get_dsmetadata()}")
            dsmetadata = self.dsmetadata_repository.create(**form.get_dsmetadata())
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

                md = self.mermaid_diagram_repository.create(
                    commit=False, data_set_id=dataset.id, md_meta_data_id=mdmetadata.id
                )

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

    def get_mermaidhub_doi(self, dataset: DataSet) -> str:
        try:
            # Build an absolute URL using Flask's url_for so it works both locally and when deployed
            return url_for("dataset.subdomain_index", doi=dataset.ds_meta_data.dataset_doi, _external=True)
        except Exception:
            # Fallback to DOMAIN env var for contexts where url_for is not available
            domain = os.getenv("DOMAIN", "localhost")
            return f"http://{domain}/doi/{dataset.ds_meta_data.dataset_doi}"


class AuthorService(BaseService):
    def __init__(self):
        super().__init__(AuthorRepository())


class DSDownloadRecordService(BaseService):
    def __init__(self):
        super().__init__(DSDownloadRecordRepository())

    def get_download_count(self, dataset_id: int) -> int:
        count = (
            db.session.query(func.count(DSDownloadRecord.id)).filter(DSDownloadRecord.dataset_id == dataset_id).scalar()
        )
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
