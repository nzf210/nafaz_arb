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



def missing_live_credentials(config) -> list:
    """Return every missing live prerequisite without mutating configuration."""
    missing = []
    local = config.local_credentials
    global_credentials = config.global_credentials
    if not local.api_key or not local.api_secret:
        missing.append(f"{config.local_exchange_name} API key/secret")
    if not global_credentials.api_key or not global_credentials.api_secret:
        missing.append(f"{config.global_exchange_name} API key/secret")
    if str(config.global_exchange_name or "").lower() == "okx" and not global_credentials.passphrase:
        missing.append("okx API passphrase")
    if not config.supabase.enabled or not config.supabase.url or not config.supabase.key:
        missing.append("Supabase durable storage (enabled, URL and service-role key)")
    if not str(config.supabase.outbox_path or "").strip():
        missing.append("Supabase durable outbox path")
    if not str(config.redis_url or "").strip():
        missing.append("Redis URL")
    if tokocrypto_broker_id_missing(config):
        missing.append(
            "tokocrypto registered broker_id (options.broker_id or options.broker.marketType)"
        )
    return missing
