from backend import OutputFormat
from backend.persister import BasePersister, CloudStoragePersister


def make_persister(config) -> BasePersister:
    try:
        from backend.persisters.geoserver import GeoServerPersister
    except ImportError:
        GeoServerPersister = None

    if config.output_format == OutputFormat.GEOSERVER:
        if GeoServerPersister is None:
            raise ImportError(
                "GeoServer output requires 'geoserver' extras: "
                "pip install nmuwd[geoserver]"
            )
        return GeoServerPersister(config)

    if config.use_cloud_storage:
        return CloudStoragePersister(config)

    return BasePersister(config)
