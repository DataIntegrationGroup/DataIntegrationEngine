import os
from typing import Optional
import dagster as dg
from backend.config import Config, SOURCE_KEYS


class DIEConfigResource(dg.ConfigurableResource):
    """Dagster resource that constructs a DIE Config from a product spec dict."""

    # USGS/NWIS API key. Without it the USGS water data API is heavily
    # rate-limited. Sourced from the USGS_API_KEY env var (a Dagster+ secret) in
    # definitions.py; exported back to the environment in get_config so the
    # backend NWIS connector — which reads os.environ["USGS_API_KEY"] at request
    # time — picks it up.
    usgs_api_key: Optional[str] = None

    def get_config(self, product: dict, parameter: Optional[str] = None) -> Config:
        """Translate a products.yaml entry into a finalized DIE ``Config``.

        Mapping:
        - ``output_type`` → ``output_summary``. ``ogc_summary``,
          ``ogc_major_chemistry``, and ``ogc_mcl_exceedance`` run in summary mode
          (the pivot products fold per-analyte summaries into one feature per
          well); everything else is timeseries mode.
        - ``spatial_filter.county`` → ``county``. ``spatial_filter.state`` sets
          ``wkt = None`` (statewide; DIE applies the NM extent downstream).
        - ``sources.include`` → enable only those sources (all others off).
          ``sources.exclude`` → disable those, leave the rest at their defaults.
        - ``parameter`` is set on the Config, then ``finalize()`` resolves the
          parameter-dependent output units.

        *parameter* overrides ``product["parameter"]`` — used by the
        major-chemistry product, which has no single parameter and calls this
        once per analyte.
        """
        # Make the USGS key visible to the backend NWIS connector (reads it from
        # the environment). Only set when provided so we never clobber an
        # ambient value with an empty one.
        if self.usgs_api_key:
            os.environ["USGS_API_KEY"] = self.usgs_api_key

        spatial = product.get("spatial_filter", {})
        sources_spec = product.get("sources", {})

        output_type = product.get("output_type", "ogc_summary")
        is_summary = output_type in (
            "ogc_summary",
            "ogc_major_chemistry",
            "ogc_mcl_exceedance",
        )

        # backend only distinguishes summary vs timeseries; major-chemistry is a
        # summary variant as far as unification is concerned.
        payload: dict = {"output_summary": is_summary}

        if spatial.get("county"):
            payload["county"] = spatial["county"]
        if spatial.get("state"):
            payload["wkt"] = None

        if sources_spec.get("include"):
            # Enable only the included sources. Derived from the backend's
            # canonical source list so a new source can't be silently dropped
            # from an include-list product.
            for s in SOURCE_KEYS:
                payload[f"use_source_{s}"] = s in sources_spec["include"]
        elif sources_spec.get("exclude"):
            for s in sources_spec["exclude"]:
                payload[f"use_source_{s}"] = False

        config = Config(payload=payload)
        # An empty parameter is valid for sites-only flows (e.g. the well
        # correlation product), so fall back to "" when the product has none.
        config.parameter = parameter or product.get("parameter", "")
        config.finalize()
        return config
