# Pinned: patch_pygeoapi.py edits this version's Parquet provider source.
# pygeoapi 0.25.dev0 (geopython/pygeoapi:latest as of 2026-09-24).
FROM geopython/pygeoapi@sha256:f3dd50a56f870d80df67416b6b07bf4d92aefa78b7e6f697564288f1cdf1df3f

# pygeoapi base image includes GDAL with /vsigs/ GCS support.
# Auth uses Application Default Credentials — no key file needed on Cloud Run.

WORKDIR /pygeoapi

# pygeoapi's Parquet provider needs pyarrow and geopandas, which the base
# image doesn't ship (s3fs, shapely, pandas are already in /venv).
# Pinned to the versions tested locally against pygeoapi 0.25.
RUN /venv/bin/python3 -m pip install --no-cache-dir --quiet \
      pyarrow==25.0.1 geopandas==1.1.4

# Fix the Parquet provider's paging and date-only datetime queries. Fails the
# build if the source it patches has changed (see the script's docstring).
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
# CHECK_PARQUET=1 (set by cloudbuild.yaml) serves each product from its
# latest.parquet when a usable one is in GCS; the build needs GCS access for
# that (docker build --network=cloudbuild). Without it every product is GeoJSON.
ARG CHECK_PARQUET=0
RUN /venv/bin/python3 /tmp/generate_config.py \
      --products /tmp/products.yaml \
      --template /tmp/config.yml.j2 \
      --output /pygeoapi/local.config.yml \
      $([ "$CHECK_PARQUET" = "1" ] && echo --check-parquet)

EXPOSE 80

# pygeoapi reads PYGEOAPI_CONFIG env var; default is local.config.yml
ENV PYGEOAPI_CONFIG=/pygeoapi/local.config.yml
