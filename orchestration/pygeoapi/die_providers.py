"""
Stock Parquet and OGR providers that list a layer's ``source`` values in the
collection schema (the "Values" column, from the field's ``enum``).

pygeoapi 0.25 has no config setting for this; delete these once it does.
generate_config.py writes ``source_values`` into each provider's config.
"""
from pygeoapi.provider.ogr import OGRProvider
from pygeoapi.provider.parquet import ParquetProvider


class SourceValuesMixin:
    def __init__(self, provider_def):
        # Set first: the parent __init__ calls get_fields().
        self.source_values = provider_def.get("source_values")
        super().__init__(provider_def)

    def get_fields(self):
        fields = super().get_fields()
        if self.source_values and "source" in fields:
            fields["source"]["enum"] = list(self.source_values)
        return fields


class DieParquetProvider(SourceValuesMixin, ParquetProvider):
    pass


class DieOGRProvider(SourceValuesMixin, OGRProvider):
    pass
