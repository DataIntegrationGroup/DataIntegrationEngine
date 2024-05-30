# ===============================================================================
# Copyright 2024 ross
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
# this is a Google App Engine task handler

from flask import Flask, request


app = Flask(__name__)


@app.route("/unify_waterlevels", methods=["POST"])
def unify_waterlevels_handler():
    from backend.config import Config
    from backend.unifier import unify_waterlevels

    payload = request.get_json()
    print(f"Recieved payload {payload}")
    cfg = Config(payload=payload)
    cfg.use_cloud_storage = True

    # cfg.county = "eddy"
    # cfg.bbox = "-104.5 32.5,-104 33"
    # cfg.start_date = "2020-01-01"
    # cfg.end_date = "2024-5-01"
    # cfg.output_summary = False
    #
    # cfg.use_source_ampapi = False
    # cfg.use_source_wqp = False
    # cfg.use_source_isc_seven_rivers = False
    # cfg.use_source_nwis = False
    # cfg.use_source_ose_roswell = False
    # cfg.use_source_st2 = False
    # cfg.use_source_bor = False
    # cfg.use_source_dwb = False
    #
    # cfg.site_limit = 10

    if unify_waterlevels(cfg):
        return "OK"
    else:
        return "Failed"


# ============= EOF =============================================
