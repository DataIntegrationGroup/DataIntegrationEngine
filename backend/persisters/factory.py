from backend.persister import BasePersister


def make_persister(config) -> BasePersister:
    # Single in-memory accumulator; the cloud/GeoServer/CSV write strategies went
    # with the CLI/worker output path.
    return BasePersister(config)
