from sqlalchemy import Enum as SQLAlchemyEnum

from app import db
from app.modules.dataset.models import Author, DiagramType


class MermaidDiagram(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data_set_id = db.Column(db.Integer, db.ForeignKey("data_set.id"), nullable=False)
    md_meta_data_id = db.Column(db.Integer, db.ForeignKey("md_meta_data.id"))
    files = db.relationship("Hubfile", backref="mermaid_diagram", lazy=True, cascade="all, delete")
    md_meta_data = db.relationship("MDMetaData", uselist=False, backref="mermaid_diagram", cascade="all, delete")

    def __repr__(self):
        return f"MermaidDiagram<{self.id}>"


class MDMetaData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    mmd_filename = db.Column(db.String(120), nullable=False)
    title = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text, nullable=False)
    publication_type = db.Column(SQLAlchemyEnum(DiagramType), nullable=False)
    publication_doi = db.Column(db.String(120))
    tags = db.Column(db.String(120))
    mmd_version = db.Column(db.String(120))
    md_metrics_id = db.Column(db.Integer, db.ForeignKey("md_metrics.id"))
    md_metrics = db.relationship("MDMetrics", uselist=False, backref="md_meta_data")
    authors = db.relationship(
        "Author", backref="md_metadata", lazy=True, cascade="all, delete", foreign_keys=[Author.md_meta_data_id]
    )

    def __repr__(self):
        return f"MDMetaData<{self.title}"


class MDMetrics(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    solver = db.Column(db.Text)
    not_solver = db.Column(db.Text)

    def __repr__(self):
        return f"MDMetrics<solver={self.solver}, not_solver={self.not_solver}>"
