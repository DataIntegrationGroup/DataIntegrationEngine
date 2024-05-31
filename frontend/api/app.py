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
import multiprocessing
import os
import time
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
    sources: list = []
    output_name: str = ""
    output_summary: bool = True


# def create_queue(project: str, location: str, queue_id: str) -> tasks_v2.Queue:
#     """Create a queue.
#     Args:
#         project: The project ID to create the queue in.
#         location: The location to create the queue in.
#         queue_id: The ID to use for the new queue.
#
#     Returns:
#         The newly created queue.
#     """
#
#     # Create a client.
#     client = tasks_v2.CloudTasksClient()
#     queue_path = client.queue_path(project, location, queue_id)
#     queue = client.get_queue(name=queue_path)
#     if not queue:
#         # Use the client to send a CreateQueueRequest.
#         client.create_queue(
#             tasks_v2.CreateQueueRequest(
#                 parent=client.common_location_path(project, location),
#                 queue=tasks_v2.Queue(name=queue_path),
#             )
#         )


@app.post("/trigger_unify_waterlevels")
def router_unify_waterlevels(item: ConfigModel):
    print("unify waterlevels", item)

    exists = False

    cfgobj = item.model_dump()
    itemhash = hashlib.md5(json.dumps(cfgobj, sort_keys=True).encode()).hexdigest()

    if not item.force:
        storage_client = storage.Client()
        if item.output_summary:
            bucket = storage_client.bucket("waterdatainitiative")
            exists = bucket.blob(f"die/{itemhash}.csv").exists()
        else:
            bucket = storage_client.bucket("waterdatainitiative")
            combined_exists = bucket.blob(f"die/{itemhash}.combined.csv").exists()
            timeseries_exists = bucket.blob(f"die/{itemhash}_timeseries/sites.csv").exists()
            exists = combined_exists or timeseries_exists

    response = None
    if not exists:
        client = tasks_v2.CloudTasksClient()
        project = os.getenv("PROJECT_ID")
        location = os.getenv("LOCATION")
        url = os.getenv("WORKER_URL")
        queue = "die-queue"

        task_id = None

        cfgobj["output_name"] = itemhash
        # Construct the task.
        name = None
        if task_id is not None:
            name = client.task_path(project, location, queue, task_id)

        task = tasks_v2.Task(
            http_request=tasks_v2.HttpRequest(
                http_method=tasks_v2.HttpMethod.POST,
                url=f"{url}/unify_waterlevels",
                headers={"Content-type": "application/json"},
                body=json.dumps(cfgobj).encode(),
            ),
            name=name,
        )
        response = client.create_task(
            tasks_v2.CreateTaskRequest(
                # The queue to add the task to
                parent=client.queue_path(project, location, queue),
                # The task itself
                task=task,
            )
        )
        # parent = client.queue_path(project, location, queue)
        # task = {
        #     'app_engine_http_request': {
        #         'http_method': 'POST',
        #         'relative_uri': f'{url}/unify_waterlevels',
        #         'body': jcfg
        #     }
        # }
        # response = client.create_task(parent=parent, task=task)

        response = {"name": response.name, "dispatch_count": response.dispatch_count}

    return dict(
        message="triggered unify waterlevels",
        downloadhash=itemhash,
        task_response=response,
    )


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
def router_download_unified_waterlevels(downloadhash: str,
                                        output_summary: bool):

    storage_client = storage.Client()
    bucket = storage_client.bucket("waterdatainitiative")

    if output_summary:
        blob = bucket.blob(f"die/{downloadhash}.csv")
        if not blob.exists():
            return HTTPException(status_code=404, detail="No such file")

        response = StreamingResponse(
            iter([blob.download_as_string()]), media_type="text/csv"
        )

        response.headers["Content-Disposition"] = f"attachment; filename=output.csv"
    else:
        blob = bucket.blob(f"die/{downloadhash}.zip")
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
