from datetime import datetime, timezone

from backend.transformer import SiteTransformer


def _arcgis_date_to_iso(value) -> str | None:
    """Convert an ArcGIS ``esriFieldTypeDate`` value (epoch milliseconds) to an
    ISO-8601 date string ``YYYY-MM-DD``. Returns ``None`` for missing/unparseable
    values so downstream consumers can skip wells with no recorded date."""
    if value is None:
        return None
    try:
        return (
            datetime.fromtimestamp(value / 1000, tz=timezone.utc).date().isoformat()
        )
    except (TypeError, ValueError, OverflowError, OSError):
        return None


class NMOSEPODSiteTransformer(SiteTransformer):
    def _transform(self, record) -> dict:
        """
        Transform the record into a dictionary format.

        Args:
            record (dict): The record to transform.

        Returns:
            dict: The transformed record.
        """

        properties = record["attributes"]
        geometry = record["geometry"]

        # print(properties.keys())
        # print(geometry.keys())
        rec = {
            "source": "NMOSEPOD",
            "id": properties["pod_file"],
            # "name": record["station_nm"],
            "latitude": geometry["y"],
            "longitude": geometry["x"],
            "elevation": properties["elevation"],
            "elevation_units": "ft",
            # "horizontal_datum": datum,
            # "vertical_datum": record["alt_datum_cd"],
            "aquifer": properties["aquifer"],
            "well_depth": properties["depth_well"],
            "well_depth_units": "ft",
            # Well completion date (finish_dat); the POD-age products bin by its
            # year. start_date is kept too but is unreliable (epoch-placeholder
            # values are common), so binning uses finish_dat only.
            "well_completion_date": _arcgis_date_to_iso(properties.get("finish_dat")),
            "well_start_date": _arcgis_date_to_iso(properties.get("start_date")),
        }
        return rec
