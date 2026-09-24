FROM geopython/pygeoapi:latest

# pygeoapi base image includes GDAL with /vsigs/ GCS support.
# Auth uses Application Default Credentials — no key file needed on Cloud Run.

WORKDIR /pygeoapi

# Copy generation inputs
# Build context is orchestration/ (see cloudbuild.yaml)
COPY pygeoapi/config.yml.j2 /tmp/config.yml.j2
COPY pygeoapi/generate_config.py /tmp/generate_config.py
COPY config/products.yaml /tmp/products.yaml

# Bake config into image at build time.
# §V: config generated from products.yaml, not hand-edited.
# jinja2 and PyYAML are pygeoapi dependencies, already in /venv.
RUN /venv/bin/python3 /tmp/generate_config.py \
      --products /tmp/products.yaml \
      --template /tmp/config.yml.j2 \
      --output /pygeoapi/local.config.yml

EXPOSE 80

# pygeoapi reads PYGEOAPI_CONFIG env var; default is local.config.yml
ENV PYGEOAPI_CONFIG=/pygeoapi/local.config.yml
