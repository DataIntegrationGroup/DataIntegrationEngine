# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
import hashlib
import json
import os
from typing import Optional

from fastapi import FastAPI, HTTPException
from google.api_core.exceptions import NotFound
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from google.cloud import tasks_v2
from google.cloud import storage

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GCS bucket holding cached API outputs; Cloud Tasks queue the trigger endpoints
# enqueue worker jobs onto.
_CACHE_BUCKET = "die_cache"
_TASK_QUEUE = "die-queue"


class BboxModel(BaseModel):
    minLat: float
    maxLat: float
    minLng: float
    maxLng: float


class ConfigModel(BaseModel):
    bbox: Optional[BboxModel] = None
    county: str = ""
    wkt: str = ""
    site_limit: int = 0
    force: bool = False
    output_name: str = ""
    output_summary: bool = True
    start_date: str = ""
    end_date: str = ""


@app.post("/trigger_unify_waterlevels")
def router_unify_waterlevels(item: ConfigModel):
    exists = False

    cfgobj = item.model_dump()
    itemhash = hashlib.md5(json.dumps(cfgobj, sort_keys=True).encode()).hexdigest()

    if not item.force:
        storage_client = storage.Client()
        if item.output_summary:
            bucket = storage_client.bucket(_CACHE_BUCKET)
            exists = bucket.blob(f"{itemhash}.csv").exists()
        else:
            bucket = storage_client.bucket(_CACHE_BUCKET)
            exists = bucket.blob(f"{itemhash}.zip").exists()

    response = None
    if not exists:
        client = tasks_v2.CloudTasksClient()
        project = os.getenv("PROJECT_ID")
        location = os.getenv("LOCATION")
        url = os.getenv("WORKER_URL")
        queue = _TASK_QUEUE

        cfgobj["output_name"] = itemhash
        task = tasks_v2.Task(
            http_request=tasks_v2.HttpRequest(
                http_method=tasks_v2.HttpMethod.POST,
                url=f"{url}/unify_waterlevels",
                headers={"Content-type": "application/json"},
                body=json.dumps(cfgobj).encode(),
            ),
        )
        response = client.create_task(
            tasks_v2.CreateTaskRequest(
                # The queue to add the task to
                parent=client.queue_path(project, location, queue),
                # The task itself
                task=task,
            )
        )

        response = {"name": response.name, "dispatch_count": response.dispatch_count}

    return dict(
        message="triggered unify waterlevels",
        downloadhash=itemhash,
        task_response=response,
    )


@app.get("/parameters")
def router_parameters():
    parameters = [
        {"name": "Depth To Groundwater", "code": "dtw"},
        {"name": "TDS", "code": "tds"},
    ]
    return parameters


@app.get("/status")
def router_status(task_id: str):
    status = "running"
    client = tasks_v2.CloudTasksClient()
    try:
        task = client.get_task(name=task_id)
    except NotFound as e:
        status = "finished"

    return {"status": status}


@app.get("/download_unified_waterlevels")
def router_download_unified_waterlevels(downloadhash: str, output_summary: bool):

    storage_client = storage.Client()
    bucket = storage_client.bucket(_CACHE_BUCKET)

    if output_summary:
        blob = bucket.blob(f"{downloadhash}.csv")
        if not blob.exists():
            return HTTPException(status_code=404, detail="No such file")

        response = StreamingResponse(
            iter([blob.download_as_string()]), media_type="text/csv"
        )

        response.headers["Content-Disposition"] = f"attachment; filename=output.csv"
    else:
        blob = bucket.blob(f"{downloadhash}.zip")
        if not blob.exists():
            return HTTPException(status_code=404, detail="No such file")
        response = StreamingResponse(
            iter([blob.download_as_string()]), media_type="application/zip"
        )
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8080, reload=True)
# ============= EOF =============================================
