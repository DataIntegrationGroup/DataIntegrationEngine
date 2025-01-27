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

from flask import Flask, request, jsonify

app = Flask(__name__)


def handler(unifier):
    from backend.config import Config

    payload = request.get_json()
    print(f"Recieved payload {payload}")
    cfg = Config(payload=payload)
    cfg.use_cloud_storage = True

    if unifier(cfg):
        result = "OK"
    else:
        result = "Failed"
    return make_cors_response({"result": result})


def make_cors_response(payload):
    response = jsonify(payload)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


@app.route("/health", methods=["GET"])
def health_handler():
    from backend.unifier import health_check

    source = request.args.get("source")
    health_response = health_check(source)

    return make_cors_response({"health": "healthy" if health_response else "unhealthy"})


@app.route("/sources", methods=["GET"])
def sources_handler():
    from backend.unifier import get_sources
    from backend.config import Config

    polygon = request.args.get("wkt")
    parameter = request.args.get("parameter")
    config = Config()
    if polygon:
        config.wkt = polygon

    config.parameter = parameter

    sources = get_sources(config)
    return make_cors_response({"sources": [s.tag for s in sources]})


@app.route("/source_bounds", methods=["GET"])
def source_bounds_handler():
    sourcekey = request.args.get("sources")
    from backend.unifier import get_source_bounds

    bounds = get_source_bounds(sourcekey, as_str=True)

    return make_cors_response({"wkt": bounds})


@app.route("/county_bounds", methods=["GET"])
def county_bounds_handler():
    county = request.args.get("county")
    from backend.unifier import get_county_bounds

    bounds = get_county_bounds(county)

    return make_cors_response({"wkt": bounds})


# @app.route("/sources_in_polygon")
# def sources_in_polygon_handler():
#     from backend.unifier import get_sources_in_polygon
#     polygon = request.args.get("wkt")
#     sources = get_sources_in_polygon(polygon)
#
#     return make_cors_response({"sources": sources})


@app.route("/unify_analytes", methods=["POST"])
def unify_analytes_handler():
    from backend.unifier import unify_analytes

    return handler(unify_analytes)


@app.route("/unify_waterlevels", methods=["POST"])
def unify_waterlevels_handler():
    from backend.unifier import unify_waterlevels

    return handler(unify_waterlevels)


if __name__ == "__main__":
    app.run(debug=True, port=5001)
# ============= EOF =============================================
