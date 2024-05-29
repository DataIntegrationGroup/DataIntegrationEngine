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
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from backend.config import Config
from frontend.unifier import unify_waterlevels

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
    county: str = ''
    wkt: str = ''
    site_limit: int = 0


active_processes: dict = {}


def cleanup():
    rm = []
    for k,v in active_processes.items():
        if not v.is_alive():
            rm.append(k)

    if rm:
        for r in rm:
            del active_processes[r]


@app.post("/trigger_unify_waterlevels")
def router_unify_waterlevels(item: ConfigModel):
    print("unify waterlevels", item)
    cfg = Config(model=item)
    cfg.output_summary = True

    # cfg.use_source_nwis = True
    # cfg.use_source_isc_seven_rivers = False
    # cfg.use_source_bor = False
    # cfg.use_source_dwb = False
    # cfg.use_source_wqp = False
    # cfg.use_source_ampapi = False
    # cfg.use_source_st2 = False
    # cfg.use_source_ose_roswell = False

    exists = False

    itemhash = hashlib.md5(
        json.dumps(item.model_dump(), sort_keys=True).encode()
    ).hexdigest()
    name = f"{itemhash}.csv"

    if os.getenv('USE_LOCAL_CACHE', False):
        pp = os.path.join("cache", name)

        if os.path.isfile(pp):
            # how old is the file
            st = os.stat(pp)
            if time.time() - st.st_mtime > 3600:
                os.remove(pp)

        if not os.path.isfile(pp):
            cfg.output_name = pp
        else:
            exists = True
    else:
        # get from storage bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket("waterdatainitiative")
        exists = bucket.blob(f'die/{name}').exists()
        cfg.output_name = itemhash

    if not exists:
        cleanup()
        # spawn a new process to do the work
        if len(active_processes.keys()) > 5:
            raise HTTPException(status_code=429, detail="Too many active processes")

        cfg.use_cloud_storage = not os.getenv('USE_LOCAL_CACHE', False)
        proc = multiprocessing.Process(target=unify_waterlevels, args=(cfg,))
        proc.start()

        active_processes[str(itemhash)] = proc
    return dict(message="triggered unify waterlevels", downloadhash=itemhash)


@app.get("/status")
def router_status(process_id: Optional[str] = None):
    cleanup()

    if process_id:
        if process_id not in active_processes:
            return dict(message=f"no such process {active_processes.keys()}")
        else:
            return dict(
                message="active process",
                process=str(active_processes[process_id]),
            )
    else:

        return dict(
            message="active processes",
            processes=[str(v) for v in active_processes.values() if v.is_alive()],
        )


@app.get("/download_unified_waterlevels")
def router_download_unified_waterlevels(downloadhash: str):
    if os.getenv('USE_LOCAL_CACHE', False):
        downloadhash = os.path.join("cache", f"{downloadhash}.csv")
        if not os.path.isfile(downloadhash):
            return HTTPException(status_code=404, detail="No such file")

        with open(downloadhash, "r") as f:
            response = StreamingResponse(iter([f.read()]), media_type="text/csv")
    else:
        storage_client = storage.Client()
        bucket = storage_client.bucket("waterdatainitiative")
        blob = bucket.blob(f'die/{downloadhash}.csv')
        if not blob.exists():
            return HTTPException(status_code=404, detail="No such file")

        response = StreamingResponse(iter([blob.download_as_string()]), media_type="text/csv")

    response.headers["Content-Disposition"] = f"attachment; filename=output.csv"
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8080, reload=True)
# ============= EOF =============================================
