from backend.persister import BasePersister
from backend.persisters.strategies import GCSStrategy


def make_persister(config) -> BasePersister:
    if config.use_cloud_storage:
        strategy = GCSStrategy(
            bucket_name="die_cache",
            output_name=config.output_name,
            output_format=config.output_format.value,
        )
        return BasePersister(config, strategy=strategy)

    return BasePersister(config)
