# Pinned for patch_pygeoapi.py: pygeoapi 0.25.dev0 (:latest on 2026-09-24).
FROM geopython/pygeoapi@sha256:f3dd50a56f870d80df67416b6b07bf4d92aefa78b7e6f697564288f1cdf1df3f

# Unbaked images read GCS directly (GDAL /vsigs/, Application Default
# Credentials).

WORKDIR /pygeoapi

# pygeoapi's Parquet provider needs pyarrow and geopandas, which the base
# image doesn't ship (s3fs, shapely, pandas are already in /venv).
# Pinned to the versions tested locally against pygeoapi 0.25.
RUN /venv/bin/python3 -m pip install --no-cache-dir --quiet \
      pyarrow==25.0.1 geopandas==1.1.4

# Fix Parquet provider bugs; fails the build if the patched source changed.
COPY pygeoapi/patch_pygeoapi.py /tmp/patch_pygeoapi.py
RUN /venv/bin/python3 /tmp/patch_pygeoapi.py

# Copy generation inputs
# Build context is orchestration/ (see cloudbuild.yaml)
COPY pygeoapi/config.yml.j2 /tmp/config.yml.j2
COPY pygeoapi/generate_config.py /tmp/generate_config.py
COPY config/products.yaml /tmp/products.yaml

# Bake config into image at build time.
# §V: config generated from products.yaml, not hand-edited.
# jinja2 and PyYAML are pygeoapi dependencies, already in /venv.
# BAKE_PRODUCTS=1 (cloudbuild.yaml): copy each product's file from GCS into
# /data/products and serve it locally; needs GCS access at build time. Use
# --no-cache locally to re-download.
# CHECK_PARQUET=1: choose Parquet vs GeoJSON the same way, but read GCS.
# Neither: GeoJSON from GCS.
ARG BAKE_PRODUCTS=0
ARG CHECK_PARQUET=0
RUN /venv/bin/python3 /tmp/generate_config.py \
      --products /tmp/products.yaml \
      --template /tmp/config.yml.j2 \
      --output /pygeoapi/local.config.yml \
      $([ "$CHECK_PARQUET" = "1" ] && echo --check-parquet) \
      $([ "$BAKE_PRODUCTS" = "1" ] && echo --bake-to /data/products)

EXPOSE 80

# pygeoapi reads PYGEOAPI_CONFIG env var; default is local.config.yml
ENV PYGEOAPI_CONFIG=/pygeoapi/local.config.yml
