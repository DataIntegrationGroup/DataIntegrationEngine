import os
from typing import Optional

import dagster as dg
import requests


class GeoServerResource(dg.ConfigurableResource):
    """Register product GeoJSON layers in GeoServer via its REST API.

    Reads connection settings from the environment (set as Dagster+ secrets):

      GEOSERVER_URL        base url, e.g. https://geoserver.newmexicowaterdata.org/geoserver
      GEOSERVER_USER       admin user
      GEOSERVER_PASSWORD   admin password
      GEOSERVER_WORKSPACE  target workspace (default "die")

    The layer is published from an OGR-backed datastore that points at the
    product's public GCS GeoJSON via GDAL's /vsicurl/ virtual filesystem, so the
    layer always reflects the latest object in the bucket. Requires the GeoServer
    OGR/GDAL vector extension to be installed.

    Optional overrides:
      GEOSERVER_OGR_VSI_PREFIX   GDAL VSI prefix (default "/vsicurl/")
      GEOSERVER_NATIVE_NAME      OGR layer name inside the datasource
                                 (default "latest" — the GeoJSON file stem)
    """

    timeout: int = 30

    # -- config / http helpers ------------------------------------------------
    def _cfg(self) -> dict:
        missing = [
            k
            for k in ("GEOSERVER_URL", "GEOSERVER_USER", "GEOSERVER_PASSWORD")
            if not os.environ.get(k)
        ]
        if missing:
            raise RuntimeError(
                f"GeoServer registration not configured; missing env: {', '.join(missing)}"
            )
        return {
            "url": os.environ["GEOSERVER_URL"].rstrip("/"),
            "auth": (os.environ["GEOSERVER_USER"], os.environ["GEOSERVER_PASSWORD"]),
            "workspace": os.environ.get("GEOSERVER_WORKSPACE", "die"),
            "vsi_prefix": os.environ.get("GEOSERVER_OGR_VSI_PREFIX", "/vsicurl/"),
            "native_name": os.environ.get("GEOSERVER_NATIVE_NAME", "latest"),
        }

    def _req(self, method, url, auth, json=None):
        resp = requests.request(
            method,
            url,
            auth=auth,
            json=json,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=self.timeout,
        )
        return resp

    @staticmethod
    def _raise(resp):
        """raise_for_status, but include GeoServer's response body — it carries
        the real reason (e.g. auth/role failure) that the bare status hides."""
        if resp.status_code >= 400:
            body = (resp.text or "").strip()
            raise requests.HTTPError(
                f"{resp.status_code} {resp.reason} for {resp.request.method} "
                f"{resp.url}: {body[:500]}",
                response=resp,
            )

    # -- public api -----------------------------------------------------------
    def register_geojson(
        self,
        layer_name: str,
        geojson_url: str,
        title: Optional[str] = None,
        abstract: Optional[str] = None,
    ) -> dict:
        """Create-or-update the workspace, OGR datastore, and featuretype so
        *geojson_url* is published as *layer_name*. Idempotent. Returns a dict
        describing the actions taken."""
        cfg = self._cfg()
        base = cfg["url"]
        ws = cfg["workspace"]
        auth = cfg["auth"]
        datasource = f"{cfg['vsi_prefix']}{geojson_url}"

        actions = {"workspace": None, "datastore": None, "featuretype": None}

        # 1. workspace
        r = self._req("GET", f"{base}/rest/workspaces/{ws}.json", auth)
        if r.status_code == 404:
            r = self._req(
                "POST", f"{base}/rest/workspaces", auth, json={"workspace": {"name": ws}}
            )
            self._raise(r)
            actions["workspace"] = "created"
        else:
            self._raise(r)
            actions["workspace"] = "exists"

        # 2. OGR datastore
        ds_body = {
            "dataStore": {
                "name": layer_name,
                "type": "OGR",
                "enabled": True,
                "connectionParameters": {
                    "entry": [
                        {"@key": "DatasourceName", "$": datasource},
                        {"@key": "DriverName", "$": "GeoJSON"},
                    ]
                },
            }
        }
        ds_url = f"{base}/rest/workspaces/{ws}/datastores/{layer_name}"
        r = self._req("GET", f"{ds_url}.json", auth)
        if r.status_code == 404:
            r = self._req(
                "POST",
                f"{base}/rest/workspaces/{ws}/datastores",
                auth,
                json=ds_body,
            )
            self._raise(r)
            actions["datastore"] = "created"
        else:
            self._raise(r)
            r = self._req("PUT", ds_url, auth, json=ds_body)
            self._raise(r)
            actions["datastore"] = "updated"

        # 3. featuretype (the published layer)
        ft_body = {
            "featureType": {
                "name": layer_name,
                "nativeName": cfg["native_name"],
                "title": title or layer_name,
                "abstract": abstract or "",
                "srs": "EPSG:4326",
                "enabled": True,
            }
        }
        ft_collection = f"{base}/rest/workspaces/{ws}/datastores/{layer_name}/featuretypes"
        ft_url = f"{ft_collection}/{layer_name}"
        r = self._req("GET", f"{ft_url}.json", auth)
        if r.status_code == 404:
            r = self._req("POST", ft_collection, auth, json=ft_body)
            self._raise(r)
            actions["featuretype"] = "created"
        else:
            self._raise(r)
            # recalculate bounding boxes on update so extent tracks new data
            r = self._req(
                "PUT", f"{ft_url}?recalculate=nativebbox,latlonbbox", auth, json=ft_body
            )
            self._raise(r)
            actions["featuretype"] = "updated"

        actions["layer_url"] = f"{base}/{ws}/wms?layers={ws}:{layer_name}"
        actions["datasource"] = datasource
        return actions
