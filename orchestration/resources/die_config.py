from typing import Optional
import dagster as dg
from backend.config import Config


class DIEConfigResource(dg.ConfigurableResource):
    """Dagster resource that constructs a DIE Config from a product spec dict."""

    usgs_api_key: Optional[str] = None

    def get_config(self, product: dict) -> Config:
        spatial = product.get("spatial_filter", {})
        sources_spec = product.get("sources", {})

        payload: dict = {
            "yes": True,
            "output_summary": product.get("output_type") == "ogc_summary",
            "output_format": product.get("output_type", "ogc_summary"),
        }

        if spatial.get("county"):
            payload["county"] = spatial["county"]
        if spatial.get("state"):
            payload["wkt"] = None

        if sources_spec.get("include"):
            all_sources = [
                "bernco", "bor", "cabq", "ebid", "nmbgmr_amp",
                "nmed_dwb", "nmose_isc_seven_rivers", "nmose_pod",
                "nmose_roswell", "nwis", "pvacd", "wqp",
            ]
            for s in all_sources:
                payload[f"use_source_{s}"] = s in sources_spec["include"]
        elif sources_spec.get("exclude"):
            for s in sources_spec["exclude"]:
                payload[f"use_source_{s}"] = False

        config = Config(payload=payload)
        config.parameter = product["parameter"]
        config.finalize()
        return config
