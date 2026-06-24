import io
import os


class LocalFileStrategy:
    def write_bytes(self, path: str, content: bytes) -> None:
        with open(path, "wb") as f:
            f.write(content)

    def make_directory(self, path: str) -> None:
        os.mkdir(path)


class GCSStrategy:
    def __init__(self, bucket_name: str, output_name: str, output_format: str):
        self._bucket_name = bucket_name
        self._output_name = output_name
        self._output_format = output_format
        self._content: list[tuple[str, bytes]] = []

    def write_bytes(self, path: str, content: bytes) -> None:
        self._content.append((path, content))

    def make_directory(self, path: str) -> None:
        pass

    def finalize(self) -> None:
        from google.cloud import storage

        if not self._content:
            return

        client = storage.Client()
        bucket = client.bucket(self._bucket_name)

        if len(self._content) > 1:
            import zipfile

            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for path, cnt in self._content:
                    zf.writestr(path, cnt)
            blob = bucket.blob(f"{self._output_name}.zip")
            blob.upload_from_string(buf.getvalue())
        else:
            path, cnt = self._content[0]
            dirname = os.path.basename(os.path.dirname(path))
            blob_path = os.path.join(dirname, os.path.basename(path))
            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                cnt,
                content_type=(
                    "application/json"
                    if self._output_format == "geojson"
                    else "text/csv"
                ),
            )
