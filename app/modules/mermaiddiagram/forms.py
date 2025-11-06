from flask_wtf import FlaskForm
from wtforms import SubmitField


class MermaidDiagramForm(FlaskForm):
    submit = SubmitField("Save mermaiddiagram")
