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
    county: str
    wkt: str


active_processes = []


def cleanup():
    rm = []
    for a in active_processes:
        if not a.is_alive():
            rm.append(a)

    if rm:
        for r in rm:
            active_processes.remove(r)


@app.post("/trigger_unify_waterlevels")
def router_unify_waterlevels(item: ConfigModel):
    print("unify waterlevels", item)
    cfg = Config(model=item)
    cfg.output_summary_waterlevel_stats = True

    itemhash = hashlib.md5(
        json.dumps(item.model_dump(), sort_keys=True).encode()
    ).hexdigest()
    name = f"{itemhash}.csv"
    pp = os.path.join("cache", name)

    if os.path.isfile(pp):
        # how old is the file
        st = os.stat(pp)
        if time.time() - st.st_mtime > 3600:
            os.remove(pp)

    if not os.path.isfile(pp):
        cfg.output_path = pp

        cleanup()
        # spawn a new process to do the work
        if len(active_processes) > 5:
            raise HTTPException(status_code=429, detail="Too many active processes")

        proc = multiprocessing.Process(target=unify_waterlevels, args=(cfg,))
        proc.start()

        active_processes.append(proc)
    return dict(message="triggered unify waterlevels", downloadhash=itemhash)


@app.get("/status")
def router_status(process_id: Optional[int] = None):
    cleanup()

    if process_id:
        for p in active_processes:
            if p.pid == process_id:
                return dict(message="active process", process=str(p))
    else:

        return dict(
            message="active processes",
            processes=[str(p) for p in active_processes if p.is_alive()],
        )


@app.get("/download_unified_waterlevels")
def router_download_unified_waterlevels(downloadhash: str):
    downloadhash = os.path.join("cache", f"{downloadhash}.csv")
    if not os.path.isfile(downloadhash):
        return HTTPException(status_code=404, detail="No such file")

    with open(downloadhash, "r") as f:
        response = StreamingResponse(iter([f.read()]), media_type="text/csv")

    response.headers["Content-Disposition"] = f"attachment; filename=output.csv"
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8080, reload=True)
# ============= EOF =============================================
