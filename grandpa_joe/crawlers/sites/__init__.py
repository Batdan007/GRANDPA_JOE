"""Site-specific crawler adapters."""

from grandpa_joe.crawlers.sites import drf, equibase, twinspires

SITES = {
    "twinspires": twinspires,
    "equibase": equibase,
    "drf": drf,
}


def get(site_name: str):
    """Return the adapter module for a site name, or None."""
    return SITES.get(site_name.lower())


def all_sites() -> list[str]:
    return list(SITES.keys())
