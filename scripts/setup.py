"""Create unique local tenant keys without replacing existing configuration."""

import json
import secrets
from pathlib import Path

path = Path(__file__).resolve().parents[1] / ".env"
try:
    with path.open("x") as output:
        path.chmod(0o600)
        keys = {"acme": secrets.token_hex(32), "globex": secrets.token_hex(32)}
        output.write("TENANT_KEYS_JSON='" + json.dumps(keys) + "'\n")
    print("Created .env with two distinct random tenant keys.")
except FileExistsError:
    print("Keeping existing .env.")
