import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from zipfile import ZipFile

from flask import (
    abort,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from flask_login import current_user, login_required

from app.modules.dataset import dataset_bp
from app.modules.dataset.forms import DataSetForm, PublishDatasetForm
from app.modules.dataset.services import (
    AuthorService,
    DataSetService,
    DOIMappingService,
    DSDownloadRecordService,
    DSMetaDataService,
    DSViewRecordService,
)
from app.modules.zenodo.services import FakenodoService

logger = logging.getLogger(__name__)


dataset_service = DataSetService()
author_service = AuthorService()
dsmetadata_service = DSMetaDataService()
fakenodo_service = FakenodoService()
doi_mapping_service = DOIMappingService()
ds_view_record_service = DSViewRecordService()


@dataset_bp.route("/dataset/upload", methods=["GET", "POST"])
@login_required
def create_dataset():
    form = DataSetForm()
    if request.method == "POST":

        dataset = None

        data = request.form

        is_draft_str = data.get("is_draft", "false")
        is_draft_status = str(is_draft_str).lower() == "true"
        should_publish = not is_draft_status

        if not form.validate_on_submit():
            return jsonify({"message": form.errors}), 400

        try:
            logger.info("Creating dataset...")
            dataset = dataset_service.create_from_form(form=form, current_user=current_user, is_draft=is_draft_status)
            print(dataset)
            logger.info(f"Created dataset: {dataset}")
            dataset_service.move_mermaid_diagrams(dataset)
        except Exception as exc:
            logger.exception(f"Exception while create dataset data in local {exc}")
            return jsonify({"Exception while create dataset data in local: ": str(exc)}), 400

        if should_publish:

            logger.info("Publishing dataset to Zenodo...")

            # send dataset as deposition to Zenodo
            data = {}
            try:
                zenodo_response_json = fakenodo_service.create_new_deposition(dataset)
                response_data = json.dumps(zenodo_response_json)
                data = json.loads(response_data)
            except Exception as exc:
                data = {}
                zenodo_response_json = {}
                logger.exception(f"Exception while create dataset data in Zenodo {exc}")

            if data.get("conceptrecid"):
                deposition_id = data.get("id")

                # update dataset with deposition id in Zenodo
                dataset_service.update_dsmetadata(dataset.ds_meta_data_id, deposition_id=deposition_id)

                try:
                    # iterate for each mermaid diagram (one mermaid diagram = one request to Zenodo)
                    for mermaid_diagram in dataset.mermaid_diagrams:
                        fakenodo_service.upload_file(dataset, deposition_id, mermaid_diagram)

                    # publish deposition
                    fakenodo_service.publish_deposition(deposition_id)

                    # update DOI and set is_draft=False
                    deposition_doi = fakenodo_service.get_doi(deposition_id)
                    dataset_service.update_dsmetadata(dataset.ds_meta_data_id, dataset_doi=deposition_doi, is_draft=False)
                except Exception as e:
                    msg = f"it has not been possible upload mermaid diagrams in Zenodo and update the DOI: {e}"
                    return jsonify({"message": msg}), 200

        file_path = current_user.temp_folder()
        if os.path.exists(file_path) and os.path.isdir(file_path):
            shutil.rmtree(file_path)

        msg = "Everything works!"
        return jsonify({"message": msg}), 200

    return render_template("dataset/upload_dataset.html", form=form)


"""
# Ejemplo en routes.py
@dataset_bp.route("/dataset/edit/<int:dataset_id>", methods=["GET", "POST"])
@login_required
def edit_dataset(dataset_id):
    dataset = dataset_service.get_dataset_by_id(dataset_id)
    # 1. Rellenar el formulario con los datos existentes
    form = DataSetForm(obj=dataset.ds_meta_data, data={'mermaid_diagrams': dataset.mermaid_diagrams})
    # 2. Manejar la petición POST con la lógica AJAX/JSON (similar a create_dataset)
    if request.method == "POST":
        # ... Lógica de procesamiento de JSON y validación (ya discutida) ...
        # ... Redirigir a view_dataset o list_dataset ...
        pass # Implementar lógica de guardado aquí

    # 3. Renderizar la plantilla
    return render_template(
        "dataset/edit_dataset.html",
        form=form,
        dataset=dataset, # <- ¡Esencial para el estado de draft!
        is_editing=True
    )
"""


@dataset_bp.route("/dataset/list", methods=["GET", "POST"])
@login_required
def list_dataset():
    return render_template(
        "dataset/list_datasets.html",
        datasets=dataset_service.get_synchronized(current_user.id),
        local_datasets=dataset_service.get_unsynchronized(current_user.id),
    )


@dataset_bp.route("/dataset/edit/<int:dataset_id>", methods=["GET", "POST"])
@login_required
def edit_dataset(dataset_id):
    dataset = dataset_service.get_or_404(dataset_id)

    if dataset.user_id != current_user.id:
        abort(403)

    form = DataSetForm(obj=dataset.ds_meta_data)

    if request.method == "POST":
        if form.validate_on_submit():
            action = request.form.get("action")

            try:
                dsmetadata_updates = {
                    "title": form.title.data,
                    "description": form.desc.data,
                    "tags": form.tags.data,
                    "diagram_type": form.diagram_type.data,
                }

                if action == "save":
                    dsmetadata_updates["is_draft"] = True
                    dataset_service.save_dataset(dataset, **dsmetadata_updates)
                    flash("Changes saved successfully as draft.", "success")
                    return redirect(url_for("dataset.view_dataset", dataset_id=dataset.id))

                elif action == "publish":
                    dataset_service.publish_dataset(dataset)
                    flash("Dataset published successfully!", "success")
                    return redirect(url_for("dataset.view_dataset", dataset_id=dataset.id))

            except Exception as e:
                logger.error(f"Error updating dataset: {e}")
                flash(f"Error updating dataset: {str(e)}", "error")

        else:
            flash("There were errors in the form submission.", "error")

    return render_template("dataset/edit_dataset.html", form=form, dataset=dataset)


@dataset_bp.route("/dataset/save/<int:dataset_id>", methods=["POST"])
@login_required
def save_dataset(dataset_id):

    dataset = dataset_service.get_unsynchronized_dataset(current_user.id, dataset_id)
    if not dataset:
        abort(404)

    logger.info(f"Saving draft dataset {dataset_id}.")

    try:
        dsmetadata_updates = {}
        if request.form.get("title"):
            dsmetadata_updates["title"] = request.form.get("title")
        if request.form.get("desc"):
            dsmetadata_updates["description"] = request.form.get("desc")
        if request.form.get("tags"):
            dsmetadata_updates["tags"] = request.form.get("tags")
        if request.form.get("diagram_type"):
            dsmetadata_updates["diagram_type"] = request.form.get("diagram_type")

        if dsmetadata_updates:
            try:
                dataset_service.update_dsmetadata(dataset.ds_meta_data_id, **dsmetadata_updates)
            except Exception:
                logger.exception("Failed to update dsmetadata before saving")

        dataset_service.add_mermaid_diagrams_from_temp(dataset)

        dsmetadata_updates["is_draft"] = True
        dataset_service.save_dataset(dataset, **dsmetadata_updates)

        flash("Changes saved successfully as draft.", "success")

        return redirect(url_for("dataset.list_dataset"))

    except Exception as exc:
        logger.exception(f"Error while saving dataset {dataset_id}: {exc}")
        flash("Failed to save dataset. Check logs for details.", "error")
        return redirect(url_for("dataset.view_dataset", dataset_id=dataset_id)), 500


@dataset_bp.route("/dataset/file/upload", methods=["POST"])
@login_required
def upload():
    file = request.files.get("file")
    temp_folder = current_user.temp_folder()

    if not file or not file.filename.endswith(".mmd"):
        return jsonify({"message": "No valid file"}), 400

    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    file_path = os.path.join(temp_folder, file.filename)

    if os.path.exists(file_path):
        base_name, extension = os.path.splitext(file.filename)
        i = 1
        while os.path.exists(os.path.join(temp_folder, f"{base_name} ({i}){extension}")):
            i += 1
        new_filename = f"{base_name} ({i}){extension}"
        file_path = os.path.join(temp_folder, new_filename)
    else:
        new_filename = file.filename

    try:
        file.save(file_path)
    except Exception as e:
        return jsonify({"message": str(e)}), 500

    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            content = fh.read()
    except Exception:
        content = ""

    keywords = [
        "graph",
        "flowchart",
        "sequenceDiagram",
        "classDiagram",
        "stateDiagram",
        "pie",
        "gantt",
        "erDiagram",
        "journey",
        "gitGraph",
        "gitgraph",
        "c4",
        "mindmap",
        "timeline",
        "sankey",
        "radar",
    ]
    keywords_re = re.compile(r"^(?:" + "|".join(keywords) + r")\b", re.I)
    blocks = []
    current = []
    for line in content.splitlines():
        if keywords_re.match(line.strip()):
            if current:
                blocks.append("\n".join(current))
            current = [line]
        else:
            if current:
                current.append(line)
    if current:
        blocks.append("\n".join(current))

    if not blocks:
        try:
            os.remove(file_path)
        except Exception:
            pass
        return jsonify({"message": "No Mermaid diagram detected in the uploaded file"}), 400

    if len(blocks) > 1:
        try:
            os.remove(file_path)
        except Exception:
            pass
        msg = "Multiple Mermaid diagrams detected in the uploaded file. " "Please upload one diagram per file."
        return (jsonify({"message": msg}), 400)

    try:
        mmdc_path = shutil.which("mmdc")
        if mmdc_path:
            tmp_out = tempfile.NamedTemporaryFile(suffix=".svg", delete=False)
            tmp_out.close()
            proc = subprocess.run(
                [
                    mmdc_path,
                    "-i",
                    file_path,
                    "-o",
                    tmp_out.name,
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if proc.returncode != 0:
                try:
                    os.remove(file_path)
                except Exception:
                    pass
                stderr = proc.stderr.strip() if proc.stderr else "Unknown mmdc error"
                try:
                    os.remove(tmp_out.name)
                except Exception:
                    pass
                return jsonify({"message": f"Mermaid validation failed: {stderr}"}), 400
            try:
                os.remove(tmp_out.name)
            except Exception:
                pass
    except Exception:
        logger.exception("Exception while running mmdc validation")

    return (
        jsonify(
            {
                "message": "MMD uploaded and validated successfully",
                "filename": new_filename,
            }
        ),
        200,
    )


@dataset_bp.route("/dataset/publish/<int:dataset_id>", methods=["POST"])
@login_required
def publish_dataset(dataset_id):
    dataset = dataset_service.get_unsynchronized_dataset(current_user.id, dataset_id)
    if not dataset:
        abort(404)

    logger.info(f"Publishing draft dataset {dataset_id} to Zenodo.")

    try:
        dsmetadata_updates = {}
        if request.form.get("title"):
            dsmetadata_updates["title"] = request.form.get("title")
        if request.form.get("desc"):
            dsmetadata_updates["description"] = request.form.get("desc")
        if request.form.get("tags"):
            dsmetadata_updates["tags"] = request.form.get("tags")
        if request.form.get("diagram_type"):
            dsmetadata_updates["diagram_type"] = request.form.get("diagram_type")
        if dsmetadata_updates:
            try:
                dataset_service.update_dsmetadata(dataset.ds_meta_data_id, **dsmetadata_updates)
            except Exception:
                logger.exception("Failed to update dsmetadata before publish")

        dataset_service.add_mermaid_diagrams_from_temp(dataset)

        dataset = dataset_service.get_unsynchronized_dataset(current_user.id, dataset_id)

        dataset_service.publish(dataset)

        return redirect(url_for("dataset.list_dataset"))

    except Exception as exc:
        logger.exception(f"Error while publishing dataset {dataset_id}: {exc}")
        flash("Failed to publish dataset. Check logs for details.", "error")
        return redirect(url_for("dataset.view_dataset", dataset_id=dataset_id)), 500


@dataset_bp.route("/dataset/file/delete", methods=["POST"])
def delete():
    data = request.get_json()
    filename = data.get("file")
    temp_folder = current_user.temp_folder()
    filepath = os.path.join(temp_folder, filename)

    if os.path.exists(filepath):
        os.remove(filepath)
        return jsonify({"message": "File deleted successfully"})

    return jsonify({"error": "Error: File not found"})


@dataset_bp.route("/dataset/download/<int:dataset_id>", methods=["GET"])
def download_dataset(dataset_id):
    dataset = dataset_service.get_or_404(dataset_id)

    file_path = f"uploads/user_{dataset.user_id}/dataset_{dataset.id}/"

    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, f"dataset_{dataset_id}.zip")

    with ZipFile(zip_path, "w") as zipf:
        for subdir, dirs, files in os.walk(file_path):
            for file in files:
                full_path = os.path.join(subdir, file)
                relative_path = os.path.relpath(full_path, file_path)
                zipf.write(
                    full_path,
                    arcname=os.path.join(os.path.basename(zip_path[:-4]), relative_path),
                )

    user_cookie = request.cookies.get("download_cookie")
    if not user_cookie:
        user_cookie = str(uuid.uuid4())

    DSDownloadRecordService().create(
        user_id=current_user.id if current_user.is_authenticated else None,
        dataset_id=dataset_id,
        download_date=datetime.now(timezone.utc),
        download_cookie=user_cookie,
    )

    resp = make_response(
        send_from_directory(
            temp_dir,
            f"dataset_{dataset_id}.zip",
            as_attachment=True,
            mimetype="application/zip",
        )
    )
    resp.set_cookie("download_cookie", user_cookie)

    return resp


@dataset_bp.route("/dataset/view/<int:dataset_id>", methods=["GET"])
def view_dataset(dataset_id):
    dataset = dataset_service.get_or_404(dataset_id)

    if dataset.ds_meta_data.is_draft:
        dataset = dataset_service.get_unsynchronized_dataset(current_user.id, dataset_id)
        if not dataset:
            abort(404)

    dataset.download_count = DSDownloadRecordService().get_download_count(dataset.id)
    form = PublishDatasetForm()
    return render_template("dataset/view_dataset.html", dataset=dataset, form=form)


@dataset_bp.route("/doi/<path:doi>/", methods=["GET"])
def subdomain_index(doi):
    new_doi = doi_mapping_service.get_new_doi(doi)
    if new_doi:
        return redirect(url_for("dataset.subdomain_index", doi=new_doi), code=302)

    ds_meta_data = dsmetadata_service.filter_by_doi(doi)
    if not ds_meta_data:
        abort(404)

    dataset = ds_meta_data.data_set

    dataset.download_count = DSDownloadRecordService().get_download_count(dataset.id)

    user_cookie = ds_view_record_service.create_cookie(dataset=dataset)
    form = PublishDatasetForm()
    resp = make_response(render_template("dataset/view_dataset.html", dataset=dataset, form=form))

    resp.set_cookie("view_cookie", user_cookie)
    return resp


@dataset_bp.route("/dataset/unsynchronized/<int:dataset_id>/", methods=["GET"])
@login_required
def get_unsynchronized_dataset(dataset_id):
    dataset = dataset_service.get_unsynchronized_dataset(current_user.id, dataset_id)

    if not dataset:
        abort(404)

    dataset.download_count = DSDownloadRecordService().get_download_count(dataset.id)

    form = PublishDatasetForm()

    return render_template("dataset/view_dataset.html", dataset=dataset, form=form)
