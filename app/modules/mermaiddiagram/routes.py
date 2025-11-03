from flask import render_template

from app.modules.mermaiddiagram import mermaiddiagram_bp


@mermaiddiagram_bp.route("/mermaiddiagram", methods=["GET"])
def index():
    return render_template("mermaiddiagram/index.html")
