from backend import OutputFormat
from backend.persister import BasePersister
from backend.persisters.strategies import GCSStrategy


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
        strategy = GCSStrategy(
            bucket_name="die_cache",
            output_name=config.output_name,
            output_format=config.output_format.value,
        )
        return BasePersister(config, strategy=strategy)

    return BasePersister(config)
