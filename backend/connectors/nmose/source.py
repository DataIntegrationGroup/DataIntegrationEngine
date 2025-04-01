import os

from shapely import wkt
from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.connectors.nmose.transformer import NMOSEPODSiteTransformer
from backend.source import BaseSiteSource


def wkt_to_arcgis_json(obj):
    if isinstance(obj, str):
        obj = wkt.loads(obj)
    coords = [[coord[0], coord[1]] for coord in obj.exterior.coords]
    return {
        'rings': [coords],
        'spatialReference': {
            'wkid': 4326
        }
    }

class NMOSEPODSiteSource(BaseSiteSource):
    """
    NMOSEPODSiteSource is a class that inherits from BaseSiteSource.
    It is used to fetch site data from the NMOSEPOD API.
    """

    transformer_klass = NMOSEPODSiteTransformer
    chunk_size = 5000
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def get_records(self, *args, **kw) -> dict:
        config = self.config
        params = {}
        # if config.has_bounds():
        #     bbox = config.bbox_bounding_points()
        #     params["bBox"] = ",".join([str(b) for b in bbox])
        # else:
        #     params["stateCd"] = "NM"
        #
        # if config.start_date:
        #     params["startDt"] = config.start_dt.date().isoformat()
        # if config.end_date:
        #     params["endDt"] = config.end_dt.date().isoformat()

        url = "https://services2.arcgis.com/qXZbWTdPDbTjl7Dy/arcgis/rest/services/OSE_PODs/FeatureServer/0/query"

        params['where'] = "pod_status = 'ACT' AND pod_basin IN ('A','B','C','CC','CD','CL','CP','CR','CT','E','FS','G','GSF','H', 'HA','HC','HS','HU','J','L','LA','LRG','LV','M','MR','NH','P','PL','PN','RA','RG','S','SB','SJ','SS','T','TU','UP','VV')"
        params["outFields"] = "OBJECTID,pod_basin,pod_status,easting,northing,datum,utm_accura,status,county,pod_name,pod_nbr,pod_suffix,pod_file"
        params["outSR"] = 4326
        params["f"] = "json"
        params["resultRecordCount"] = self.chunk_size
        params['resultOffset'] = 0

        if config.has_bounds():
            wkt = config.bounding_wkt()
            params["geometry"] = wkt_to_arcgis_json(wkt)
            params["geometryType"] = "esriGeometryPolygon"

        records = []
        i=1
        while 1:
            rs = self._execute_json_request(url, params, tag='features')
            records.extend(rs)
            params['resultOffset'] += self.chunk_size
            if len(rs) < self.chunk_size:
                break
            i+=1

        return records