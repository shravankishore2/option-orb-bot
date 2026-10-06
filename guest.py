"""
guest.py — the read-only demo link (/guest?k=<key>). The key is the only secret: it lives
in ~/.orbital_guest_key on the VM (mode 600), outside the repo, and the dashboard re-reads
it whenever the file changes, so rotating takes effect at once, without a restart.

    python guest.py rotate     # new key (the old link stops working); prints the new link
    python guest.py show       # print the current link
    python guest.py off        # delete the key: /guest answers 404 until the next rotate
"""

import os
import secrets
import sys
from pathlib import Path

KEY_FILE = Path(os.getenv("ORBITAL_GUEST_KEY_FILE", "~/.orbital_guest_key")).expanduser()
BASE_URL = os.getenv("ORBITAL_PUBLIC_URL", "https://orbital.68-233-96-25.sslip.io")


def link(key):
    return f"{BASE_URL}/guest?k={key}"


def rotate():
    key = secrets.token_urlsafe(24)
    tmp = KEY_FILE.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(key + "\n")
    os.replace(tmp, KEY_FILE)
    return key


def main(cmd):
    if cmd == "rotate":
        print(link(rotate()))
    elif cmd == "show":
        print(link(KEY_FILE.read_text().strip()) if KEY_FILE.exists() else "no guest key (python guest.py rotate)")
    elif cmd == "off":
        KEY_FILE.unlink(missing_ok=True)
        print("guest view off")
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "")
