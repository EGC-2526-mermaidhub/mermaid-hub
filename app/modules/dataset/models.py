from datetime import datetime
from enum import Enum

from flask import request
from sqlalchemy import Enum as SQLAlchemyEnum

from app import db


class DiagramType(Enum):
    FLOWCHART = "FLOWCHART"
    SEQUENCE_DIAGRAM = "SEQUENCE_DIAGRAM"
    CLASS_DIAGRAM = "CLASS_DIAGRAM"
    STATE_DIAGRAM = "STATE_DIAGRAM"
    ENTITY_RELATIONSHIP_DIAGRAM = "ENTITY_RELATIONSHIP_DIAGRAM"
    USER_JOURNEY = "USER_JOURNEY"
    GANTT = "GANTT"
    PIE_CHART = "PIE_CHART"
    QUADRANT_CHART = "QUADRANT_CHART"
    REQUIREMENT_DIAGRAM = "REQUIREMENT_DIAGRAM"
    GITGRAPH_DIAGRAM = "GITGRAPH_DIAGRAM"
    C4_DIAGRAM = "C4_DIAGRAM"
    MINDMAPS = "MINDMAPS"
    TIMELINE = "TIMELINE"
    ZENUML = "ZENUML"
    SANKEY = "SANKEY"
    XYCHART = "XYCHART"
    BLOCKDIAGRAM = "BLOCKDIAGRAM"
    PACKET = "PACKET"
    KANBAN = "KANBAN"
    ARCHITECTURE = "ARCHITECTURE"
    RADAR = "RADAR"
    TREEMAP = "TREEMAP"


class Author(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    affiliation = db.Column(db.String(120))
    orcid = db.Column(db.String(120))
    ds_meta_data_id = db.Column(db.Integer, db.ForeignKey("ds_meta_data.id"))
    md_meta_data_id = db.Column(db.Integer, db.ForeignKey("md_meta_data.id"))

    def to_dict(self):
        return {"name": self.name, "affiliation": self.affiliation, "orcid": self.orcid}


class DSMetrics(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    number_of_diagrams = db.Column(db.String(120))
    number_of_features = db.Column(db.String(120))

    def __repr__(self):
        return f"DSMetrics<models={self.number_of_diagrams}, features={self.number_of_features}>"


class DSMetaData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    deposition_id = db.Column(db.Integer)
    title = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text, nullable=False)
    diagram_type = db.Column(SQLAlchemyEnum(DiagramType), nullable=False)
    publication_doi = db.Column(db.String(120))
    dataset_doi = db.Column(db.String(120))
    tags = db.Column(db.String(120))
    ds_metrics_id = db.Column(db.Integer, db.ForeignKey("ds_metrics.id"))
    ds_metrics = db.relationship("DSMetrics", uselist=False, backref="ds_meta_data", cascade="all, delete")
    authors = db.relationship("Author", backref="ds_meta_data", lazy=True, cascade="all, delete")


class DataSet(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    ds_meta_data_id = db.Column(db.Integer, db.ForeignKey("ds_meta_data.id"), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    ds_meta_data = db.relationship("DSMetaData", backref=db.backref("data_set", uselist=False))
    mermaid_diagrams = db.relationship("MermaidDiagram", backref="data_set", lazy=True, cascade="all, delete")

    def name(self):
        return self.ds_meta_data.title

    def files(self):
        return [file for md in self.mermaid_diagrams for file in md.files]

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    def get_cleaned_diagram_type(self):
        return self.ds_meta_data.diagram_type.name.replace("_", " ").title()

    def get_zenodo_url(self):
        return f"https://zenodo.org/record/{self.ds_meta_data.deposition_id}" if self.ds_meta_data.dataset_doi else None

    def get_files_count(self):
        return sum(len(fm.files) for fm in self.mermaid_diagrams)

    def get_file_total_size(self):
        return sum(file.size for fm in self.mermaid_diagrams for file in fm.files)

    def get_file_total_size_for_human(self):
        from app.modules.dataset.services import SizeService

        return SizeService().get_human_readable_size(self.get_file_total_size())

    def get_mermaidhub_doi(self):
        from app.modules.dataset.services import DataSetService

        return DataSetService().get_mermaidhub_doi(self)

    def to_dict(self):
        from app.modules.dataset.services import DSDownloadRecordService

        return {
            "title": self.ds_meta_data.title,
            "id": self.id,
            "created_at": self.created_at,
            "created_at_timestamp": int(self.created_at.timestamp()),
            "description": self.ds_meta_data.description,
            "authors": [author.to_dict() for author in self.ds_meta_data.authors],
            "diagram_type": self.get_cleaned_diagram_type(),
            "publication_doi": self.ds_meta_data.publication_doi,
            "dataset_doi": self.ds_meta_data.dataset_doi,
            "tags": self.ds_meta_data.tags.split(",") if self.ds_meta_data.tags else [],
            "url": self.get_mermaidhub_doi(),
            "download": f'{request.host_url.rstrip("/")}/dataset/download/{self.id}',
            "zenodo": self.get_zenodo_url(),
            "files": [file.to_dict() for fm in self.mermaid_diagrams for file in fm.files],
            "files_count": self.get_files_count(),
            "total_size_in_bytes": self.get_file_total_size(),
            "total_size_in_human_format": self.get_file_total_size_for_human(),
            "download_count": DSDownloadRecordService().get_download_count(self.id),
        }

    def __repr__(self):
        return f"DataSet<{self.id}>"


class DSDownloadRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    dataset_id = db.Column(db.Integer, db.ForeignKey("data_set.id"))
    download_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    download_cookie = db.Column(db.String(36), nullable=False)  # Assuming UUID4 strings

    def __repr__(self):
        return (
            f"<Download id={self.id} "
            f"dataset_id={self.dataset_id} "
            f"date={self.download_date} "
            f"cookie={self.download_cookie}>"
        )


class DSViewRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    dataset_id = db.Column(db.Integer, db.ForeignKey("data_set.id"))
    view_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    view_cookie = db.Column(db.String(36), nullable=False)  # Assuming UUID4 strings

    def __repr__(self):
        return f"<View id={self.id} dataset_id={self.dataset_id} date={self.view_date} cookie={self.view_cookie}>"


class DOIMapping(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    dataset_doi_old = db.Column(db.String(120))
    dataset_doi_new = db.Column(db.String(120))
