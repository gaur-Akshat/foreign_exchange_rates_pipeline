"""Bronze layer: fetch raw FX data from an external API."""

import requests


def fetch_fx_rates(api_url: str, base_currency: str, target_currencies: list) -> dict:
    """
    Request foreign exchange rates from the configured API.

    Args:
        api_url: Endpoint URL for the FX API.
        base_currency: ISO currency code used as the quote base (e.g. USD).
        target_currencies: List of currency codes to fetch rates for.

    Returns:
        Raw API response as a dictionary (typically JSON-decoded).
    """
    pass
