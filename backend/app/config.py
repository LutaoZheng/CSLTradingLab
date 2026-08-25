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

    def model_post_init(self, __context) -> None:
        if not self.data_dir.is_absolute():
            self.data_dir = (PROJECT_ROOT / self.data_dir).resolve()
        prefix = "sqlite+aiosqlite:///./"
        if self.database_url.startswith(prefix):
            relative = self.database_url.removeprefix(prefix)
            self.database_url = f"sqlite+aiosqlite:///{(PROJECT_ROOT / relative).resolve()}"
settings = Settings()
