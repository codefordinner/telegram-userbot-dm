"""HTTP integrations for triggering Jellyfin and Navidrome library scans."""

import hashlib
import logging
import uuid
from typing import Optional


log = logging.getLogger("userbot-dm-uploader")


async def refresh_jellyfin(http, base_url: str, api_key: str) -> Optional[bool]:
    """None means disabled; otherwise return whether Jellyfin accepted a scan request."""
    if not api_key:
        return None
    try:
        response = await http.post(
            f"{base_url}/Library/Refresh", params={"api_key": api_key}, timeout=15
        )
    except Exception as exc:
        log.warning("Jellyfin не ответил на запрос сканирования: %s", exc)
        return False
    ok = response.status_code in (200, 204)
    if not ok:
        log.warning("Jellyfin вернул %s на запрос сканирования", response.status_code)
    return ok


async def scan_navidrome(http, base_url: str, username: str, password: str) -> Optional[bool]:
    """None means disabled; otherwise return whether Navidrome accepted the scan request."""
    if not (username and password):
        return None
    salt = uuid.uuid4().hex[:8]
    params = {
        "u": username,
        "t": hashlib.md5((password + salt).encode()).hexdigest(),
        "s": salt,
        "v": "1.16.1",
        "c": "userbot",
        "f": "json",
        "fullScan": "true",
    }
    try:
        response = await http.get(f"{base_url.rstrip('/')}/rest/startScan.view", params=params, timeout=15)
        ok = response.status_code == 200 and response.json().get("subsonic-response", {}).get("status") == "ok"
    except Exception as exc:
        log.warning("Navidrome не ответил на запрос сканирования: %s", exc)
        return False
    if not ok:
        log.warning("Navidrome отклонил запрос сканирования (%s)", response.status_code)
    return ok
