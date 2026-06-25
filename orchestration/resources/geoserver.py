import os
from typing import Optional

import dagster as dg
import requests


class GeoServerResource(dg.ConfigurableResource):
    """Publish product layers in GeoServer via its REST API.

    Reads connection settings from the environment (set as Dagster+ secrets):

      GEOSERVER_URL        base url, e.g. https://geoserver.newmexicowaterdata.org/geoserver
      GEOSERVER_USER       admin user
      GEOSERVER_PASSWORD   admin password
      GEOSERVER_WORKSPACE  target workspace (default "die")

    The layer is published from a zipped ESRI Shapefile uploaded to a native
    shapefile datastore (no GeoServer extensions required). GeoServer keeps its
    own copy of the data, refreshed on each run.
    """

    timeout: int = 60

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
        }

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

    def _ensure_workspace(self, base, ws, auth):
        r = requests.get(
            f"{base}/rest/workspaces/{ws}.json", auth=auth, timeout=self.timeout
        )
        if r.status_code == 404:
            r = requests.post(
                f"{base}/rest/workspaces",
                auth=auth,
                json={"workspace": {"name": ws}},
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            self._raise(r)
            return "created"
        self._raise(r)
        return "exists"

    # -- public api -----------------------------------------------------------
    def publish_shapefile(
        self,
        layer_name: str,
        shapefile_zip_path: str,
        title: Optional[str] = None,
        abstract: Optional[str] = None,
    ) -> dict:
        """Create-or-update a native shapefile datastore named *layer_name* from
        the zipped shapefile at *shapefile_zip_path* and publish its layer.
        Idempotent — re-running overwrites the store's data. Returns a dict
        describing the actions taken."""
        cfg = self._cfg()
        base = cfg["url"]
        ws = cfg["workspace"]
        auth = cfg["auth"]

        actions = {"workspace": self._ensure_workspace(base, ws, auth)}

        # Delete any pre-existing datastore of this name first. PUT file.shp
        # reuses an existing store's factory type, so a store left over from a
        # different backend (e.g. a broken OGR store) would otherwise force a
        # 500. Recreating from scratch each run keeps this self-healing; the
        # data is re-uploaded every run regardless.
        ds_url = f"{base}/rest/workspaces/{ws}/datastores/{layer_name}"
        r = requests.delete(
            f"{ds_url}?recurse=true", auth=auth, timeout=self.timeout
        )
        if r.status_code not in (200, 404):
            self._raise(r)
        actions["datastore_reset"] = "deleted" if r.status_code == 200 else "absent"

        # Upload the zipped shapefile. PUT .../file.shp creates the datastore and
        # publishes the contained layer.
        with open(shapefile_zip_path, "rb") as f:
            data = f.read()
        url = f"{base}/rest/workspaces/{ws}/datastores/{layer_name}/file.shp?configure=all"
        r = requests.put(
            url,
            data=data,
            auth=auth,
            headers={"Content-Type": "application/zip"},
            timeout=self.timeout,
        )
        self._raise(r)
        actions["upload"] = "ok"

        # Set title/abstract on the published featuretype. The shapefile layer
        # name defaults to the .shp basename inside the zip (== layer_name here).
        if title is not None or abstract is not None:
            ft_url = (
                f"{base}/rest/workspaces/{ws}/datastores/{layer_name}"
                f"/featuretypes/{layer_name}"
            )
            r = requests.put(
                ft_url,
                auth=auth,
                json={
                    "featureType": {
                        "title": title or layer_name,
                        "abstract": abstract or "",
                    }
                },
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            # non-fatal: data is already published even if metadata update fails
            if r.status_code < 400:
                actions["metadata"] = "updated"
            else:
                actions["metadata"] = f"skipped ({r.status_code})"

        actions["layer_url"] = f"{base}/{ws}/wms?layers={ws}:{layer_name}"
        return actions
