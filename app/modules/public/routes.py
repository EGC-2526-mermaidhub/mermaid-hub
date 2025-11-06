import logging

from flask import render_template

from app.modules.dataset.services import DataSetService
from app.modules.mermaiddiagram.services import MermaidDiagramService
from app.modules.public import public_bp

logger = logging.getLogger(__name__)


@public_bp.route("/")
def index():
    logger.info("Access index")
    dataset_service = DataSetService()
    mermaid_diagram_service = MermaidDiagramService()

    # Statistics: total datasets and feature models
    datasets_counter = dataset_service.count_synchronized_datasets()
    mermaid_diagrams_counter = mermaid_diagram_service.count_mermaid_diagrams()

    # Statistics: total downloads
    total_dataset_downloads = dataset_service.total_dataset_downloads()
    total_mermaid_diagram_downloads = mermaid_diagram_service.total_mermaid_diagram_downloads()

    # Statistics: total views
    total_dataset_views = dataset_service.total_dataset_views()
    total_mermaid_diagram_views = mermaid_diagram_service.total_mermaid_diagram_views()

    return render_template(
        "public/index.html",
        datasets=dataset_service.latest_synchronized(),
        datasets_counter=datasets_counter,
        mermaid_diagrams_counter=mermaid_diagrams_counter,
        total_dataset_downloads=total_dataset_downloads,
        total_mermaid_diagram_downloads=total_mermaid_diagram_downloads,
        total_dataset_views=total_dataset_views,
        total_mermaid_diagram_views=total_mermaid_diagram_views,
    )
