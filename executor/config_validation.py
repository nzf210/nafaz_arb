"""Small live-exchange configuration validators kept outside the config facade."""


def tokocrypto_broker_id_missing(config) -> bool:
    """Whether a configured Tokocrypto trading account lacks its issued client-ID prefix."""
    credentials = config.exchanges.get("tokocrypto")
    if str(config.local_exchange_name or "").lower() == "tokocrypto":
        credentials = config.local_credentials
    if not credentials or not credentials.api_key or not credentials.api_secret:
        return False
    options = credentials.options if isinstance(credentials.options, dict) else {}
    broker = options.get("broker") if isinstance(options.get("broker"), dict) else {}
    broker_id = (
        options.get("broker_id")
        or options.get("brokerId")
        or broker.get("marketType")
        or broker.get("market_type")
    )
    return not bool(str(broker_id or "").strip())
