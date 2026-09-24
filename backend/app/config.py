from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DATABASE_URL = f"sqlite+aiosqlite:///{DEFAULT_DATA_DIR / 'csl_trading_lab.db'}"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env", extra="ignore")
    app_env: str = "development"
    database_url: str = DEFAULT_DATABASE_URL
    data_dir: Path = DEFAULT_DATA_DIR
    mock_mode: bool = True
    trading_enabled: bool = False
    kalshi_api_key_id: str = ""
    kalshi_private_key_path: str = ""
    kalshi_rest_url: str = "https://external-api.kalshi.com/trade-api/v2"
    kalshi_ws_url: str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    csl_series_tickers: str = ""
    discovery_interval_seconds: float = 3
    app_version: str = "0.1.0"
    public_origin: str = "https://csltradinglab.duckdns.org"
    operator_username: str = "operator"
    auth_username: str = ""
    admin_username: str = ""
    auth_password_hash: str = ""
    admin_password_hash: str = ""
    auth_session_hours: int = 12
    auth_remember_days: int = 30
    admin_reauth_minutes: int = 15
    signal_event_ticker: str = ""
    signal_home_label: str = "CHINA"
    signal_away_label: str = "MALDIVES"
    realtime_signal_max_age_ms: int = 5000
    auto_discovery_enabled: bool = False
    match_config_path: Path = PROJECT_ROOT / "config" / "matches.v1.json"
    network_test_retention_days: int = 30
    network_test_max_events_per_run: int = 600
    network_test_run_max_minutes: int = 30
    network_test_rate_per_minute: int = 120
    network_test_event_max_age_ms: int = 30000

    def model_post_init(self, __context) -> None:
        if not self.data_dir.is_absolute():
            self.data_dir = (PROJECT_ROOT / self.data_dir).resolve()
        if not self.match_config_path.is_absolute():
            self.match_config_path = (PROJECT_ROOT / self.match_config_path).resolve()
        prefix = "sqlite+aiosqlite:///./"
        if self.database_url.startswith(prefix):
            relative = self.database_url.removeprefix(prefix)
            self.database_url = f"sqlite+aiosqlite:///{(PROJECT_ROOT / relative).resolve()}"
settings = Settings()
