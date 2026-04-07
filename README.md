# Cursor-GlobalCapture-test

Automation for Square 9 GlobalCapture (Capture API): `gc_cursor_agent.py` (workflow + CUR\_ metadata plan/execute) and `gc_release_license.py` (optional license release).

## Engine id vs UI

`GET /Square9CaptureAPI/engines` returns each engine’s **`ID`** (use this for `GC_ENGINE_ID` and workflow `Engines`). The **`UID`** field matches labels such as `GlobalCapture_1 -199737994` in admin UI—the API **`ID`** is different from **`UID`**.

## Single-license tenants — log out when done

After using GlobalCapture or Batch Manager (including after API runs that checked out a license), **explicitly log out or fully exit the client** so the seat is available for others. Short-lived HTTP calls from scripts do not replace UI logout.

Optional: `gc_release_license.py` (see script docstring; cloud tenants may return 404 on DELETE).

## Run

```bash
pip install -r requirements.txt
export GC_BASE_URL=... GC_USERNAME=... GC_PASSWORD=...
# Optional: GC_PORTAL_ID=1  GC_ENGINE_ID=<from GET .../engines "ID">
export DRY_RUN=true EXECUTE=false
python3 gc_cursor_agent.py
```
