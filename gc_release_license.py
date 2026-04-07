#!/usr/bin/env python3
"""
Release GlobalCapture LAN licenses checked out to GC_USERNAME (Capture API).

Per Square 9 docs: GET .../admin/licenses, DELETE .../license/{LicenseID}
returns the license to its allocation pool.

Requires: GC_BASE_URL, GC_USERNAME, GC_PASSWORD
Safety: set GC_RELEASE_LICENSES=true to perform DELETEs (otherwise list only).
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

API_PREFIXES = ("Square9CaptureAPI", "Square9CaptureApi")


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def main() -> int:
    base = os.environ.get("GC_BASE_URL", "").strip().rstrip("/")
    user = os.environ.get("GC_USERNAME", "").strip()
    password = os.environ.get("GC_PASSWORD", "").strip()
    do_delete = _env_bool("GC_RELEASE_LICENSES")

    if not base or not user or not password:
        print("Set GC_BASE_URL, GC_USERNAME, GC_PASSWORD", file=sys.stderr)
        return 1

    auth = HTTPBasicAuth(user, password)
    session = requests.Session()
    session.auth = auth
    session.headers.setdefault("Accept", "application/json")

    licenses: list[dict[str, Any]] = []
    used_path = ""
    for prefix in API_PREFIXES:
        path = f"{prefix}/admin/licenses"
        url = f"{base}/{path}"
        try:
            r = session.get(url, timeout=60)
            print(f"GET {path} -> {r.status_code}")
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    licenses = data
                    used_path = path
                    break
        except requests.RequestException as e:
            print(f"GET {path} failed: {e}", file=sys.stderr)

    if not licenses:
        print("No license list retrieved (403/404 or empty). Try an admin-capable account or License Manager on the server.")
        return 2

    target = user.lower()
    mine = []
    for row in licenses:
        un = str(row.get("UserName") or row.get("userName") or "")
        if un.lower() == target:
            mine.append(row)

    if not mine:
        print(f"No active licenses for UserName matching {user!r} (found {len(licenses)} total).")
        for row in licenses[:20]:
            print(f"  - LicenseID={row.get('LicenseID')} UserName={row.get('UserName')!r}")
        return 0

    print(f"Licenses for {user!r}: {len(mine)}")
    for row in mine:
        print(f"  LicenseID={row.get('LicenseID')} Type={row.get('LicenseType')} Exp={row.get('Expiration')}")

    if not do_delete:
        print("\nDry run. To DELETE these licenses (release seats), set GC_RELEASE_LICENSES=true and run again.")
        return 0

    for row in mine:
        lid = row.get("LicenseID") or row.get("licenseID")
        if not lid:
            continue
        deleted = False
        for prefix in API_PREFIXES:
            path = f"{prefix}/license/{lid}"
            url = f"{base}/{path}"
            try:
                r = session.delete(url, timeout=60)
                print(f"DELETE {path} -> {r.status_code}")
                if r.status_code in (200, 204):
                    deleted = True
                    break
            except requests.RequestException as e:
                print(f"DELETE {path} failed: {e}", file=sys.stderr)
        if not deleted:
            print(f"Failed to release LicenseID={lid}", file=sys.stderr)

    print("Done. You can sign in again in GlobalCapture / Batch Manager.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
