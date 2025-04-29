from backend.transformer import BaseTransformer, SiteTransformer


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
        }
        return rec
