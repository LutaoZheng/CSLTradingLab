"""Disposable local server for browser-context auth tests; never uses project data."""
import os
import tempfile
from pathlib import Path

from app.auth import hash_password

with tempfile.TemporaryDirectory(prefix="csl-browser-e2e-") as directory:
    root=Path(directory)
    match_config=root/"matches.v1.json"
    match_config.write_text('{"schema_version":1,"matches":[]}')
    os.environ.update({
        "APP_ENV":"test",
        "PUBLIC_ORIGIN":"http://localhost:3000",
        "DATABASE_URL":f"sqlite+aiosqlite:///{root/'e2e.db'}",
        "DATA_DIR":str(root/"data"),
        "MATCH_CONFIG_PATH":str(match_config),
        "MOCK_MODE":"true",
        "TRADING_ENABLED":"false",
        "AUTO_DISCOVERY_ENABLED":"false",
        "OPERATOR_USERNAME":"operator",
        "AUTH_USERNAME":"michael",
        "AUTH_PASSWORD_HASH":hash_password("browser-operator-pass"),
        "ADMIN_PASSWORD_HASH":hash_password("browser-admin-pass"),
    })
    import uvicorn
    uvicorn.run("app.main:app",host="127.0.0.1",port=8000,log_level="warning")
