from flask_wtf import FlaskForm
from wtforms import BooleanField, FieldList, FormField, SelectField, StringField, SubmitField, TextAreaField
from wtforms.validators import URL, DataRequired, Optional

from app.modules.dataset.models import DiagramType


class AuthorForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired()])
    affiliation = StringField("Affiliation")
    orcid = StringField("ORCID")
    gnd = StringField("GND")

    class Meta:
        csrf = False  # disable CSRF because is subform

    def get_author(self):
        return {
            "name": self.name.data,
            "affiliation": self.affiliation.data,
            "orcid": self.orcid.data,
        }


class MermaidDiagramForm(FlaskForm):
    mmd_filename = StringField("MMD Filename", validators=[DataRequired()])
    title = StringField("Title", validators=[Optional()])
    desc = TextAreaField("Description", validators=[Optional()])
    diagram_type = SelectField(
        "Diagram type",
        choices=[(pt.value, pt.name.replace("_", " ").title()) for pt in DiagramType],
        validators=[Optional()],
    )
    publication_doi = StringField("Publication DOI", validators=[Optional(), URL()])
    tags = StringField("Tags (separated by commas)")
    version = StringField("MMD Version")
    authors = FieldList(FormField(AuthorForm))

    class Meta:
        csrf = False  # disable CSRF because is subform

    def get_authors(self):
        return [author.get_author() for author in self.authors]

    def get_mdmetadata(self):
        return {
            "mmd_filename": self.mmd_filename.data,
            "title": self.title.data,
            "description": self.desc.data,
            "diagram_type": self.diagram_type.data,
            "publication_doi": self.publication_doi.data,
            "tags": self.tags.data,
            "mmd_version": self.version.data,
        }


class PublishDatasetForm(FlaskForm):
    pass


class DataSetForm(FlaskForm):
    title = StringField("Title", validators=[DataRequired()])
    desc = TextAreaField("Description", validators=[DataRequired()])
    diagram_type = SelectField(
        "Diagram type",
        choices=[(pt.value, pt.name.replace("_", " ").title()) for pt in DiagramType],
        validators=[DataRequired()],
    )
    publication_doi = StringField("Publication DOI", validators=[Optional(), URL()])
    dataset_doi = StringField("Dataset DOI", validators=[Optional(), URL()])
    tags = StringField("Tags (separated by commas)")
    authors = FieldList(FormField(AuthorForm))
    mermaid_diagrams = FieldList(FormField(MermaidDiagramForm), min_entries=1)
    is_draft = BooleanField("Save as draft?", default=True)

    submit = SubmitField("Submit")

    def get_dsmetadata(self):

        diagram_type_converted = self.convert_diagram_type(self.diagram_type.data)

        return {
            "title": self.title.data,
            "description": self.desc.data,
            "diagram_type": diagram_type_converted,
            "publication_doi": self.publication_doi.data,
            "dataset_doi": self.dataset_doi.data,
            "tags": self.tags.data,
        }

    def convert_diagram_type(self, value):
        for pt in DiagramType:
            if pt.value == value:
                return pt.name
        return "NONE"

    def get_authors(self):
        return [author.get_author() for author in self.authors]

    def get_mermaid_diagrams(self):
        return [md.get_mermaid_diagrams() for md in self.mermaid_diagrams]
