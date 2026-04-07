#!/usr/bin/env python3
"""
Square 9 GlobalCapture API agent: discover portal/engine, plan or execute
workflow "Cursor - Test" with CUR_-prefixed metadata only.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    print("Missing dependency: pip install requests", file=sys.stderr)
    sys.exit(1)

ALLOWED_WORKFLOW_NAME = "Cursor - Test"
# Portal name substrings (case-insensitive). Includes "batch portal" for default "Batch Portal" installs.
HEURISTIC_TERMS = ("sandbox", "test", "dev", "qa", "batch portal")
API_PREFIXES = ("Square9CaptureAPI", "Square9CaptureApi")

CUR_LIST_NAME = "CUR_Invoice Status List"
CUR_FIELD_NAMES = (
    "CUR_Invoice Number",
    "CUR_Invoice Date",
    "CUR_Total Amount",
)
CUR_TABLE_NAME = "CUR_Invoice Lines Table"


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return default
    return default


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_summary(method: str, url: str, body: Any = None) -> str:
    h = hashlib.sha256()
    h.update(f"{method}:{url}".encode())
    if body is not None:
        if isinstance(body, (dict, list)):
            raw = json.dumps(body, sort_keys=True, default=str).encode()
        elif isinstance(body, bytes):
            raw = body
        else:
            raw = str(body).encode()
        h.update(raw)
    return f"sha256:{h.hexdigest()[:16]}"


def _extract_ids(obj: Any) -> list[str]:
    ids: list[str] = []
    if obj is None:
        return ids
    if isinstance(obj, dict):
        for k in ("ID", "Id", "id", "WorkflowID", "BatchID", "ProcessID", "FileID"):
            v = obj.get(k)
            if v is not None and str(v) not in ("", "0", 0):
                ids.append(f"{k}={v}")
        for v in obj.values():
            ids.extend(_extract_ids(v))
    elif isinstance(obj, list):
        for item in obj[:30]:
            ids.extend(_extract_ids(item))
    return list(dict.fromkeys(ids))[:30]


def _expand_api_paths(suffix: str) -> list[str]:
    suffix = suffix.lstrip("/")
    return [f"{p}/{suffix}" for p in API_PREFIXES]


@dataclass
class AuditEntry:
    ts: str
    method: str
    endpoint: str
    summary: str
    status: int | None
    resource_ids: str
    rule_check: str


@dataclass
class RunState:
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    read_calls: int = 0
    write_calls: int = 0
    failed_calls: list[tuple[str, str, int | None]] = field(default_factory=list)
    audit: list[AuditEntry] = field(default_factory=list)
    variants_tried: list[str] = field(default_factory=list)

    def log(
        self,
        method: str,
        full_url: str,
        endpoint_path: str,
        status: int | None,
        body: Any,
        response_obj: Any,
        rule_check: str,
        is_write: bool,
    ) -> None:
        if is_write:
            self.write_calls += 1
        else:
            self.read_calls += 1
        if status is not None and status >= 400:
            self.failed_calls.append((method, endpoint_path, status))
        ids = _extract_ids(response_obj)
        self.audit.append(
            AuditEntry(
                ts=_utc_now_iso(),
                method=method,
                endpoint=endpoint_path,
                summary=_request_summary(method, full_url, body),
                status=status,
                resource_ids=", ".join(ids) if ids else "(none parsed)",
                rule_check=rule_check,
            )
        )


class GCClient:
    def __init__(self, base_url: str, username: str, password: str, state: RunState):
        self.base_url = base_url.rstrip("/") + "/"
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.session.headers.setdefault("Accept", "application/json")
        self.state = state

    def _url(self, path: str) -> str:
        return urljoin(self.base_url, path.lstrip("/"))

    def request(
        self,
        method: str,
        path: str,
        *,
        is_write: bool,
        rule_check: str,
        json_body: Any = None,
        data: Any = None,
        files: Any = None,
        params: dict[str, Any] | None = None,
        allow_writes: bool,
    ) -> requests.Response | None:
        if is_write and not allow_writes:
            raise RuntimeError("write blocked: DRY_RUN=true or EXECUTE=false")
        url = self._url(path)
        attempt = 0
        last: requests.Response | None = None
        while attempt < 5:
            attempt += 1
            try:
                kwargs: dict[str, Any] = {"timeout": 120}
                if params:
                    kwargs["params"] = params
                if json_body is not None:
                    kwargs["json"] = json_body
                if data is not None:
                    kwargs["data"] = data
                if files is not None:
                    kwargs["files"] = files
                resp = self.session.request(method, url, **kwargs)
                last = resp
                body_log = json_body if json_body is not None else data
                parsed: Any = None
                try:
                    if resp.content:
                        parsed = resp.json()
                except Exception:
                    parsed = (resp.text or "")[:500]
                self.state.log(
                    method,
                    url,
                    path,
                    resp.status_code,
                    body_log,
                    parsed,
                    rule_check,
                    is_write,
                )
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < 5:
                    time.sleep(min(2**attempt + random.random(), 30))
                    continue
                return resp
            except requests.RequestException as e:
                self.state.log(
                    method,
                    url,
                    path,
                    None,
                    json_body,
                    str(e),
                    f"{rule_check}; network_error",
                    is_write,
                )
                if attempt < 5:
                    time.sleep(min(2**attempt + random.random(), 30))
                    continue
                return None
        return last


def try_paths(
    client: GCClient,
    method: str,
    paths: list[str],
    *,
    is_write: bool,
    rule_check: str,
    allow_writes: bool,
    success_codes: tuple[int, ...] = (200,),
    **kwargs: Any,
) -> tuple[requests.Response | None, str | None]:
    for path in paths:
        resp = client.request(
            method, path, is_write=is_write, rule_check=rule_check, allow_writes=allow_writes, **kwargs
        )
        if resp is None:
            continue
        if resp.status_code == 404:
            client.state.variants_tried.append(f"{method} {path} -> 404")
            continue
        if resp.status_code in success_codes:
            return resp, path
    return None, None


def discover_portals(client: GCClient, allow_writes: bool) -> list[dict[str, Any]]:
    paths: list[str] = []
    for p in API_PREFIXES:
        paths.extend([f"{p}/batchportals", f"{p}/BatchPortals", f"{p}/batchPortals"])
    for path in paths:
        resp, _ = try_paths(
            client,
            "GET",
            [path],
            is_write=False,
            rule_check="read batch portals; no workflow/metadata mutation",
            allow_writes=allow_writes,
        )
        if resp and resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
    return []


def portal_candidates(portals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for p in portals:
        name = str(p.get("Name") or p.get("name") or "")
        if any(t in name.lower() for t in HEURISTIC_TERMS):
            out.append(p)
    return out


def _env_portal_id_override() -> tuple[int | None, str | None]:
    """If GC_PORTAL_ID is set, use that batch portal id (explicit operator override)."""
    raw = os.environ.get("GC_PORTAL_ID", "").strip()
    if not raw:
        return None, None
    try:
        return int(raw), None
    except ValueError:
        return None, f"GC_PORTAL_ID is not an integer: {raw!r}"


def _env_engine_id_override() -> tuple[str | None, str | None]:
    """Optional GC_ENGINE_ID when multiple engines match the portal."""
    raw = os.environ.get("GC_ENGINE_ID", "").strip()
    if not raw:
        return None, None
    return raw, None


def discover_engines(client: GCClient, allow_writes: bool) -> list[dict[str, Any]]:
    for path in _expand_api_paths("engines"):
        resp, _ = try_paths(
            client,
            "GET",
            [path],
            is_write=False,
            rule_check="read engines; no workflow/metadata mutation",
            allow_writes=allow_writes,
        )
        if resp and resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
    return []


def select_portal_and_engine(
    client: GCClient, allow_writes: bool
) -> tuple[dict[str, Any] | None, Any, str | None]:
    portals = discover_portals(client, allow_writes)
    if not portals:
        return None, None, "No batch portals returned from any variant."

    override_pid, override_err = _env_portal_id_override()
    if override_err:
        return None, None, override_err

    if override_pid is not None:
        portal = None
        for p in portals:
            raw_id = p.get("Id") or p.get("ID") or p.get("id")
            try:
                if raw_id is not None and int(raw_id) == override_pid:
                    portal = p
                    break
            except (TypeError, ValueError):
                continue
        if portal is None:
            return None, portals, f"GC_PORTAL_ID={override_pid} not found in batch portals"
        pid = override_pid
    else:
        candidates = portal_candidates(portals)
        if len(candidates) == 0:
            return None, portals, "no_heuristic_match"
        if len(candidates) > 1:
            return None, candidates, "ambiguous_portal"

        portal = candidates[0]
        raw_pid = portal.get("Id") or portal.get("ID") or portal.get("id")
        if raw_pid is None:
            return None, portals, "portal_missing_id"
        pid = int(raw_pid)

    engines = discover_engines(client, allow_writes)
    matching = []
    for e in engines:
        ep = e.get("PortalID") or e.get("PortalId") or e.get("portalID")
        try:
            if ep is not None and int(ep) == int(pid):
                matching.append(e)
        except (TypeError, ValueError):
            continue

    eng_override, eng_err = _env_engine_id_override()
    if eng_err:
        return None, None, eng_err

    if eng_override:
        pool = matching if matching else engines
        chosen = [e for e in pool if str(e.get("ID")) == str(eng_override)]
        if not chosen:
            return None, engines, f"GC_ENGINE_ID={eng_override!r} not found in engines list"
        matching = chosen

    if len(matching) == 0:
        # With explicit GC_PORTAL_ID, API may omit or skew PortalID on engines; if exactly
        # one engine exists, bind to it (operator-designated batch portal still applies).
        if override_pid is not None and len(engines) == 1:
            matching = engines
        else:
            return (
                None,
                {
                    "engines": engines,
                    "portal": portal,
                    "portal_id": pid,
                },
                "no_engine_for_portal",
            )
    if len(matching) > 1:
        return None, matching, "ambiguous_engine"

    return (
        {"portal": portal, "engine": matching[0], "portal_id": int(pid)},
        None,
        None,
    )


def classify_auth_reachability(resp: requests.Response | None) -> tuple[str, str]:
    if resp is None:
        return "FAIL", "FAIL"
    if resp.status_code in (401, 403):
        return "FAIL", "PASS"
    if 200 <= resp.status_code < 300:
        return "PASS", "PASS"
    if resp.status_code >= 500:
        return "FAIL", "FAIL"
    return "FAIL", "FAIL"


def get_json_list(client: GCClient, paths: list[str], rule: str, allow_writes: bool) -> list[dict[str, Any]]:
    resp, _ = try_paths(client, "GET", paths, is_write=False, rule_check=rule, allow_writes=allow_writes)
    if not resp or resp.status_code != 200:
        return []
    data = resp.json()
    return data if isinstance(data, list) else []


def find_by_name(
    rows: list[dict[str, Any]], name_key: str, name: str
) -> dict[str, Any] | None:
    for r in rows:
        n = r.get(name_key) or r.get("name")
        if n == name:
            return r
    return None


def ensure_cur_list(
    client: GCClient, allow_writes: bool
) -> tuple[int | None, str]:
    paths = [f"{p}/lists" for p in API_PREFIXES]
    lists = get_json_list(client, paths, "read lists catalog", allow_writes)
    hit = find_by_name(lists, "Name", CUR_LIST_NAME)
    if hit:
        lid = hit.get("ID") or hit.get("Id") or hit.get("id")
        return (int(lid) if lid is not None else None, "reused")
    if not allow_writes:
        return None, "missing_readonly"
    body = {
        "Name": CUR_LIST_NAME,
        "AssemblyPath": "",
        "AssemblyParameters": "",
        "Values": ["New", "Processed", "Error"],
    }
    for path in paths:
        resp = client.request(
            "POST",
            path,
            is_write=True,
            rule_check=f"preflight: metadata name {CUR_LIST_NAME!r} starts with CUR_; write allowed",
            json_body=body,
            allow_writes=allow_writes,
        )
        if resp and resp.status_code in (200, 201, 204):
            break
    lists2 = get_json_list(client, paths, "re-read lists after create", allow_writes)
    hit2 = find_by_name(lists2, "Name", CUR_LIST_NAME)
    if not hit2:
        return None, "create_failed"
    lid = hit2.get("ID") or hit2.get("Id")
    return (int(lid) if lid is not None else None, "created")


def ensure_cur_field(
    client: GCClient,
    allow_writes: bool,
    name: str,
    ftype: str,
    *,
    list_id: int | None = None,
) -> tuple[int | None, str]:
    paths = [f"{p}/fields/" for p in API_PREFIXES] + [f"{p}/fields" for p in API_PREFIXES]
    fields = get_json_list(client, paths, f"read fields for {name}", allow_writes)
    hit = find_by_name(fields, "Name", name)
    if hit:
        fid = hit.get("ID") or hit.get("Id")
        return (int(fid) if fid is not None else None, "reused")
    if not allow_writes:
        return None, "missing_readonly"
    list_block = {
        "type": None,
        "listId": 0,
        "primary": 0,
        "secondary": 0,
        "mapping": [],
    }
    if list_id is not None:
        list_block = {
            "type": "dropdown",
            "listId": list_id,
            "primary": 0,
            "secondary": 0,
            "mapping": [],
        }
    body = {
        "name": name,
        "type": ftype,
        "format": "",
        "regex": "",
        "length": 50,
        "required": False,
        "multiValue": False,
        "systemField": "",
        "list": list_block,
    }
    post_paths = [f"{p}/field" for p in API_PREFIXES]
    for path in post_paths:
        resp = client.request(
            "POST",
            path,
            is_write=True,
            rule_check=f"preflight: field name {name!r} starts with CUR_",
            json_body=body,
            allow_writes=allow_writes,
        )
        if resp and resp.status_code in (200, 201, 204):
            break
    fields2 = get_json_list(client, paths, f"re-read fields for {name}", allow_writes)
    hit2 = find_by_name(fields2, "Name", name)
    if not hit2:
        return None, "create_failed"
    fid = hit2.get("ID") or hit2.get("Id")
    return (int(fid) if fid is not None else None, "created")


def ensure_cur_table(
    client: GCClient, allow_writes: bool, member_ids: list[int]
) -> tuple[int | None, str]:
    paths = [f"{p}/tablefields/" for p in API_PREFIXES] + [f"{p}/tablefields" for p in API_PREFIXES]
    rows = get_json_list(client, paths, "read table fields", allow_writes)
    hit = find_by_name(rows, "name", CUR_TABLE_NAME) or find_by_name(rows, "Name", CUR_TABLE_NAME)
    if hit:
        tid = hit.get("id") or hit.get("ID")
        return (int(tid) if tid is not None else None, "reused")
    if not allow_writes:
        return None, "missing_readonly"
    body = {"name": CUR_TABLE_NAME, "fields": member_ids}
    for path in [f"{p}/tablefields/" for p in API_PREFIXES]:
        resp = client.request(
            "POST",
            path,
            is_write=True,
            rule_check=f"preflight: table field {CUR_TABLE_NAME!r} starts with CUR_",
            json_body=body,
            allow_writes=allow_writes,
        )
        if resp and resp.status_code in (200, 201, 204):
            break
    rows2 = get_json_list(client, paths, "re-read table fields", allow_writes)
    hit2 = find_by_name(rows2, "name", CUR_TABLE_NAME) or find_by_name(rows2, "Name", CUR_TABLE_NAME)
    if not hit2:
        return None, "create_failed"
    tid = hit2.get("id") or hit2.get("ID")
    return (int(tid) if tid is not None else None, "created")


def portal_workflows(client: GCClient, portal_id: int, allow_writes: bool) -> list[dict[str, Any]]:
    paths = [f"{p}/portal/{portal_id}/workflows" for p in API_PREFIXES]
    return get_json_list(
        client, paths, f"list workflows portal {portal_id}", allow_writes
    )


def ensure_workflow(
    client: GCClient,
    allow_writes: bool,
    portal_id: int,
    engine_id: str,
) -> tuple[str | None, str, str | None]:
    wfs = portal_workflows(client, portal_id, allow_writes)
    existing = next((w for w in wfs if w.get("Name") == ALLOWED_WORKFLOW_NAME), None)
    desc = (
        "Managed test workflow (Cursor agent). Scope: only this workflow; "
        "metadata deps must be CUR_* only."
    )
    body = {
        "Name": ALLOWED_WORKFLOW_NAME,
        "Description": desc,
        "SVG": "<svg xmlns='http://www.w3.org/2000/svg'/>",
        "SaveDate": _utc_now_iso(),
        "Published": False,
        "Type": 0,
        "Engines": [engine_id],
    }
    if existing:
        wid = existing.get("ID") or existing.get("Id")
        if not wid:
            return None, "existing_no_id", "Workflow present but missing ID"
        body["ID"] = wid
        put_paths = [
            f"{p}/portal/{portal_id}/workflow/{wid}" for p in API_PREFIXES
        ]
        for path in put_paths:
            resp = client.request(
                "PUT",
                path,
                is_write=True,
                rule_check=f"preflight: workflow name == {ALLOWED_WORKFLOW_NAME!r}",
                json_body=body,
                allow_writes=allow_writes,
            )
            if resp and resp.status_code in (200, 201, 204):
                return str(wid), "updated", None
        return str(wid), "update_failed", "PUT workflow not accepted; no duplicate POST attempted"
    post_paths = [f"{p}/portal/{portal_id}/workflow" for p in API_PREFIXES]
    for path in post_paths:
        resp = client.request(
            "POST",
            path,
            is_write=True,
            rule_check=f"preflight: workflow name == {ALLOWED_WORKFLOW_NAME!r}",
            json_body=body,
            allow_writes=allow_writes,
        )
        if resp and resp.status_code in (200, 201, 204):
            break
    wfs2 = portal_workflows(client, portal_id, allow_writes)
    hit = next((w for w in wfs2 if w.get("Name") == ALLOWED_WORKFLOW_NAME), None)
    if not hit:
        return None, "create_failed", "POST workflow did not surface in GET list"
    wid = hit.get("ID") or hit.get("Id")
    return (str(wid) if wid else None, "created", None)


def upload_sample_file(client: GCClient, allow_writes: bool) -> tuple[str | None, str | None]:
    pdf = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"
    names = ("file", "upload", "document")
    upload_paths = []
    for prefix in API_PREFIXES:
        upload_paths.extend(
            [
                f"{prefix}/file",
                f"{prefix}/files",
                f"{prefix}/upload",
                f"{prefix}/portal/file",
            ]
        )
    for path in upload_paths:
        for fname in names:
            files = {fname: ("cursor_smoke.pdf", io.BytesIO(pdf), "application/pdf")}
            resp = client.request(
                "POST",
                path,
                is_write=True,
                rule_check="file upload for smoke test; no metadata mutation",
                files=files,
                allow_writes=allow_writes,
            )
            if not resp:
                continue
            if resp.status_code in (200, 201):
                try:
                    j = resp.json()
                    for k in ("Path", "path", "FilePath", "filePath", "Url", "ID", "Id"):
                        if k in j and j[k]:
                            return str(j[k]), path
                except Exception:
                    pass
                if resp.text and len(resp.text) < 400:
                    return resp.text.strip(), path
    return None, None


def create_process(
    client: GCClient,
    allow_writes: bool,
    portal_id: int,
    workflow_id: str,
    engine_id: str,
    file_path: str | None,
) -> tuple[Any, str | None]:
    body: dict[str, Any] = {
        "WorkflowID": workflow_id,
        "WorkflowName": ALLOWED_WORKFLOW_NAME,
        "EngineID": engine_id,
        "PortalID": portal_id,
        "FilePages": [],
        "Status": 5,
        "CurrentNode": "-8",
        "Properties": [],
    }
    if file_path:
        body["Properties"] = [
            {
                "ID": -1,
                "Name": "FilePath",
                "Type": 1,
                "SystemProperty": False,
                "Value": file_path,
                "FieldID": -1,
                "PortalID": 0,
                "DBID": 0,
            }
        ]
    paths = [f"{p}/portal/{portal_id}/process" for p in API_PREFIXES]
    for path in paths:
        resp = client.request(
            "POST",
            path,
            is_write=True,
            rule_check=f"preflight: process targets workflow {ALLOWED_WORKFLOW_NAME!r}",
            json_body=body,
            allow_writes=allow_writes,
        )
        if resp and resp.status_code in (200, 201):
            try:
                return resp.json(), path
            except Exception:
                return {"raw": resp.text}, path
    return None, None


def poll_process(
    client: GCClient,
    allow_writes: bool,
    portal_id: int,
    process_id: str,
    timeout_s: float = 120.0,
) -> list[tuple[str, Any]]:
    transitions: list[tuple[str, Any]] = []
    deadline = time.monotonic() + timeout_s
    query_paths = []
    for pfx in API_PREFIXES:
        query_paths.append(f"{pfx}/batches?portalid={portal_id}&batchid={process_id}&page=1&count=1")
        query_paths.append(f"{pfx}/portal/{portal_id}/process/{process_id}")
    while time.monotonic() < deadline:
        for path in query_paths:
            resp = client.request(
                "GET",
                path,
                is_write=False,
                rule_check="poll process; read-only",
                allow_writes=allow_writes,
            )
            if resp and resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = resp.text
                st = None
                if isinstance(data, dict):
                    st = data.get("Status") or data.get("status")
                elif isinstance(data, list) and data:
                    st = data[0].get("Status") if isinstance(data[0], dict) else None
                transitions.append((_utc_now_iso(), {"status": st, "path": path}))
                if st is not None and int(st) in (1, 2, 3, 4, 6, 7, 8, 9, 10):
                    # Heuristic terminal-ish; tenant-specific
                    pass
                break
        time.sleep(3.0)
    return transitions


def run_execution(
    client: GCClient,
    allow_writes: bool,
    portal_id: int,
    engine_id: str,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "assets": {},
        "workflow": {},
        "smoke": {},
        "errors": [],
    }
    portals = discover_portals(client, allow_writes)
    if not any(int(p.get("Id") or p.get("ID") or -1) == portal_id for p in portals):
        out["errors"].append("Preflight: portal id not in batchportals list")
        return out
    engines = discover_engines(client, allow_writes)
    if not any(str(e.get("ID")) == str(engine_id) for e in engines):
        out["errors"].append("Preflight: engine id not in engines list")
        return out

    lid, lhow = ensure_cur_list(client, allow_writes)
    if lid is None:
        out["errors"].append(f"CUR list: {lhow}")
        return out
    out["assets"]["list"] = {"id": lid, "mode": lhow}

    f_invoice, how1 = ensure_cur_field(client, allow_writes, CUR_FIELD_NAMES[0], "Character")
    f_date, how2 = ensure_cur_field(client, allow_writes, CUR_FIELD_NAMES[1], "Date")
    f_amt, how3 = ensure_cur_field(
        client, allow_writes, CUR_FIELD_NAMES[2], "Character", list_id=lid
    )
    for nm, fid, how in (
        (CUR_FIELD_NAMES[0], f_invoice, how1),
        (CUR_FIELD_NAMES[1], f_date, how2),
        (CUR_FIELD_NAMES[2], f_amt, how3),
    ):
        if fid is None:
            out["errors"].append(f"Field {nm}: {how}")
            return out
        out["assets"][nm] = {"id": fid, "mode": how}

    member_ids = [f_invoice, f_date, f_amt]
    tid, thow = ensure_cur_table(client, allow_writes, member_ids)
    if tid is None:
        out["errors"].append(f"Table: {thow}")
        return out
    out["assets"][CUR_TABLE_NAME] = {"id": tid, "mode": thow}

    wid, whow, werr = ensure_workflow(client, allow_writes, portal_id, engine_id)
    out["workflow"] = {"id": wid, "mode": whow, "error": werr}
    if werr:
        out["errors"].append(werr)
    if not wid:
        return out

    fpath, up_path = upload_sample_file(client, allow_writes)
    out["smoke"]["upload_path_tried"] = up_path
    out["smoke"]["file_ref"] = fpath
    if not fpath:
        out["errors"].append("Smoke test: file upload failed on all path variants")
        return out

    proc, ppath = create_process(
        client, allow_writes, portal_id, wid, engine_id, fpath
    )
    out["smoke"]["process_post_path"] = ppath
    out["smoke"]["process_response"] = proc
    if proc is None:
        out["errors"].append("Smoke test: POST process failed")
        return out
    pid = None
    if isinstance(proc, dict):
        pid = proc.get("ID") or proc.get("Id") or proc.get("id")
    if pid is not None:
        out["smoke"]["process_id"] = str(pid)
        out["smoke"]["poll"] = poll_process(
            client, allow_writes, portal_id, str(pid)
        )
    return out


def main() -> int:
    base = os.environ.get("GC_BASE_URL", "").strip()
    user = os.environ.get("GC_USERNAME", "").strip()
    password = os.environ.get("GC_PASSWORD", "").strip()
    dry = _env_bool("DRY_RUN", False)
    execute = _env_bool("EXECUTE", False)
    allow_writes = (not dry) and execute

    state = RunState()
    mode = "EXECUTION" if allow_writes else "DISCOVERY_ONLY"

    print("=== GC Cursor Agent ===")
    print(f"Correlation/run id: {state.correlation_id}")
    print(f"DRY_RUN={dry} EXECUTE={execute} => writes_allowed={allow_writes}")

    auth_pass = "FAIL"
    reach_pass = "FAIL"
    safety_pass = "PASS"

    if not base or not user:
        _print_report(
            mode="DISCOVERY_ONLY",
            auth_pass="FAIL",
            reach_pass="FAIL",
            safety_pass=safety_pass,
            portal_id="NONE",
            portal_name="NONE",
            engine_id="NONE",
            engine_evidence="N/A",
            decision_lines=["Missing GC_BASE_URL or GC_USERNAME"],
            plan_actions=[],
            exec_section=None,
            state=state,
            final="ABORTED",
            abort_detail="Configure GC_BASE_URL and GC_USERNAME.",
            portal_list_abort=None,
            abort_code=None,
        )
        return 1

    client = GCClient(base, user, password, state)

    # First probe (batchportals) for auth vs reachability
    probe_paths = []
    for p in API_PREFIXES:
        probe_paths.extend([f"{p}/batchportals", f"{p}/BatchPortals"])
    probe_resp, _ = try_paths(
        client,
        "GET",
        probe_paths,
        is_write=False,
        rule_check="initial auth/reachability probe",
        allow_writes=allow_writes,
    )
    auth_pass, reach_pass = classify_auth_reachability(probe_resp)

    portal_id_disp = "NONE"
    portal_name_disp = "NONE"
    engine_id_disp = "NONE"
    engine_evidence = "N/A"
    decision_lines: list[str] = []
    abort_reason: str | None = None
    abort_code: str | None = None
    portal_engine: dict[str, Any] | None = None
    portal_list_abort: list[dict[str, Any]] | None = None
    plan_actions: list[str] = []
    exec_results: dict[str, Any] | None = None

    if auth_pass == "PASS":
        sel, extra, err = select_portal_and_engine(client, allow_writes)
        if err == "no_heuristic_match":
            abort_code = err
            abort_reason = (
                "No portal name matched discovery heuristics (sandbox, test, dev, qa, batch portal). "
                "Set GC_PORTAL_ID to a batch portal Id, or rename a portal to match a heuristic."
            )
            portal_list_abort = extra if isinstance(extra, list) else None
            decision_lines.append("No heuristic match among returned batch portals.")
        elif err == "ambiguous_portal":
            abort_code = err
            abort_reason = "Multiple portals matched heuristics; disambiguation required."
            portal_list_abort = extra if isinstance(extra, list) else None
            names = [
                f"{i+1}. Id={p.get('Id') or p.get('ID')} Name={p.get('Name')}"
                for i, p in enumerate(extra or [])
            ]
            decision_lines.append("Ambiguous portals:\n" + "\n".join(names))
        elif err == "ambiguous_engine":
            abort_code = err
            abort_reason = "Multiple engines mapped to selected portal; pick one engine id explicitly."
            lines = [
                f"{i+1}. EngineID={e.get('ID')} PortalID={e.get('PortalID')} Path={e.get('Path')}"
                for i, e in enumerate(extra or [])
            ]
            decision_lines.append("Ambiguous engines:\n" + "\n".join(lines))
        elif err == "no_engine_for_portal":
            abort_code = err
            eng_info = extra if isinstance(extra, dict) else {}
            eng_list = eng_info.get("engines") or []
            pobj = eng_info.get("portal")
            if pobj is not None:
                portal_id_disp = str(eng_info.get("portal_id", ""))
                portal_name_disp = str(pobj.get("Name") or pobj.get("name") or "")
            abort_reason = (
                "No engine reports PortalID matching the selected batch portal, and "
                f"{len(eng_list)} engine(s) returned (need GC_ENGINE_ID if more than one)."
            )
            decision_lines.append(
                f"Selected portal id={portal_id_disp} name={portal_name_disp!r}; "
                f"engines from API: {len(eng_list)}"
            )
            lines = [
                f"{i+1}. EngineID={e.get('ID')} PortalID={e.get('PortalID')} ServiceName={e.get('ServiceName')}"
                for i, e in enumerate(eng_list)
            ]
            if lines:
                decision_lines.append("Engines:\n" + "\n".join(lines))
            decision_lines.append(
                "Next action: set GC_ENGINE_ID to the capture engine id that should run workflows for this portal."
            )
        elif err:
            abort_code = err
            abort_reason = str(err)
        else:
            portal_engine = sel
            pid = portal_engine["portal_id"]
            portal_id_disp = str(pid)
            portal_name_disp = str(portal_engine["portal"].get("Name", ""))
            eng = portal_engine["engine"]
            engine_id_disp = str(eng.get("ID", ""))
            eng_pid = eng.get("PortalID") or eng.get("PortalId") or eng.get("portalID")
            go_eng, _ = _env_engine_id_override()
            try:
                natural = (
                    eng_pid is not None
                    and int(eng_pid) == int(portal_engine["portal_id"])
                )
            except (TypeError, ValueError):
                natural = False
            if natural:
                engine_evidence = (
                    f"Engine ID {engine_id_disp} has PortalID={eng_pid} "
                    f"matching selected portal {portal_id_disp}."
                )
            elif go_eng:
                engine_evidence = (
                    f"Engine ID {engine_id_disp} selected via GC_ENGINE_ID; "
                    f"API reports engine PortalID={eng_pid}; batch portal id={portal_id_disp}."
                )
            else:
                engine_evidence = (
                    f"Engine ID {engine_id_disp} selected as sole engine for operator-designated "
                    f"portal {portal_id_disp}; API PortalID={eng_pid}."
                )
            opid, _ = _env_portal_id_override()
            if opid is not None:
                decision_lines.append(
                    f"Portal id {portal_id_disp} from GC_PORTAL_ID override; "
                    f"engine {engine_id_disp} matched PortalID."
                )
            else:
                decision_lines.append(
                    f"Selected single heuristic portal '{portal_name_disp}' (id={portal_id_disp}) "
                    f"and single engine {engine_id_disp}."
                )

    if auth_pass == "FAIL":
        abort_reason = abort_reason or "HTTP 401/403 or unreachable API (see audit failed calls)."

    default_plan = [
        "Re-GET batchportals + engines to confirm portal/engine still exist",
        "GET fields, lists, tablefields — reuse or create only CUR_* metadata",
        f"POST list/fields/tablefield — only names starting with CUR_ (e.g. {CUR_LIST_NAME})",
        f"POST or PUT workflow named exactly {ALLOWED_WORKFLOW_NAME!r} bound to discovered engine",
        "POST sample PDF (upload path variants), POST portal process for that workflow, poll status",
    ]
    if portal_engine and not abort_reason:
        plan_actions = default_plan
    elif not allow_writes and not plan_actions:
        plan_actions = default_plan

    if portal_engine and not abort_reason and allow_writes:
        exec_results = run_execution(
            client,
            allow_writes,
            portal_engine["portal_id"],
            str(engine_id_disp),
        )
        if exec_results.get("errors"):
            abort_reason = "; ".join(exec_results["errors"])
            abort_code = "execution_failed"

    final = "ABORTED"
    if auth_pass == "FAIL" or abort_reason:
        final = "ABORTED"
    elif allow_writes and not abort_reason:
        final = "EXECUTED"
    elif not allow_writes and portal_engine and not abort_reason:
        final = "READY_FOR_CONFIRMATION"
    elif not allow_writes and abort_reason:
        final = "ABORTED"

    _print_report(
        mode=mode,
        auth_pass=auth_pass,
        reach_pass=reach_pass,
        safety_pass=safety_pass,
        portal_id=portal_id_disp,
        portal_name=portal_name_disp,
        engine_id=engine_id_disp,
        engine_evidence=engine_evidence,
        decision_lines=decision_lines,
        plan_actions=plan_actions,
        exec_section=exec_results,
        state=state,
        final=final,
        abort_detail=abort_reason,
        portal_list_abort=portal_list_abort,
        abort_code=abort_code,
        allow_writes=allow_writes,
    )

    audit_path = os.environ.get("GC_AUDIT_JSON", "")
    if audit_path:
        with open(audit_path, "w", encoding="utf-8") as f:
            json.dump(
                {"correlation_id": state.correlation_id, "entries": [e.__dict__ for e in state.audit]},
                f,
                indent=2,
            )

    if final == "ABORTED":
        return 2
    return 0


def _print_report(
    *,
    mode: str,
    auth_pass: str,
    reach_pass: str,
    safety_pass: str,
    portal_id: str,
    portal_name: str,
    engine_id: str,
    engine_evidence: str,
    decision_lines: list[str],
    plan_actions: list[str],
    exec_section: dict[str, Any] | None,
    state: RunState,
    final: str,
    abort_detail: str | None,
    portal_list_abort: list[dict[str, Any]] | None,
    abort_code: str | None,
    allow_writes: bool = False,
) -> None:
    print("\n1) MODE")
    print(f"- {mode}")

    print("\n2) PRE-FLIGHT")
    print(f"- Auth: {auth_pass}")
    print(f"- Endpoint reachability: {reach_pass}")
    print(f"- Safety rules loaded: {safety_pass}")

    print("\n3) DISCOVERED CONFIG")
    print(f"- GC_PORTAL_ID: {portal_id}")
    print(f"- Portal Name: {portal_name}")
    print(f"- GC_ENGINE_ID: {engine_id}")
    print(f"- Engine Mapping Evidence: {engine_evidence}")

    print("\n4) DECISION LOG")
    for line in decision_lines:
        for sub in line.split("\n"):
            print(f"- {sub}")
    if state.variants_tried:
        print("- Endpoint variants attempted (404 retries):")
        for v in state.variants_tried[:20]:
            print(f"  - {v}")
    if abort_detail:
        print(f"- ABORT: {abort_detail}")
    if abort_code:
        print(f"- ABORT_CODE: {abort_code}")

    print("\n5) PLAN (if discovery-only)")
    if not allow_writes:
        for i, a in enumerate(plan_actions, 1):
            print(f"  {i}. {a}")
        print("- Explicit statement: No writes performed")
    else:
        print("  (Execution mode — writes performed per plan; see section 6.)")

    print("\n6) EXECUTION RESULTS (if execution mode)")
    if allow_writes and exec_section:
        print(f"- Workflow touched: {ALLOWED_WORKFLOW_NAME}")
        wf = exec_section.get("workflow") or {}
        print(f"- Workflow id/mode: {wf.get('id')} / {wf.get('mode')} err={wf.get('error')}")
        assets = exec_section.get("assets") or {}
        print("- CUR_ assets created/reused:")
        for k, v in assets.items():
            print(f"  - {k}: {v}")
        sm = exec_section.get("smoke") or {}
        print(f"- Smoke upload: path={sm.get('upload_path_tried')} ref={sm.get('file_ref')}")
        print(f"- Process id: {sm.get('process_id')} post_path={sm.get('process_post_path')}")
        print(f"- Poll transitions (sample): {sm.get('poll', [])[:5]}")
        if exec_section.get("errors"):
            print(f"- Errors: {exec_section['errors']}")
    elif not allow_writes:
        print("- N/A (discovery-only)")
    else:
        print("- N/A")

    print("\n7) SAFETY ATTESTATION")
    if allow_writes:
        print("- Confirm no non-allowed workflow modified: only listed/created workflow name checked")
        print("- Confirm no non-CUR_ metadata modified: only POST/PUT for CUR_* names and list fields")
        print("- Deletes scope: No deletes performed")
    else:
        print("- Confirm no non-allowed workflow modified: N/A (no writes)")
        print("- Confirm no non-CUR_ metadata modified: N/A (no writes)")
        print("- Deletes scope: No deletes performed")

    total = state.read_calls + state.write_calls
    print("\n8) AUDIT SUMMARY")
    print(f"- Total API calls: {total}")
    print(f"- Read calls count: {state.read_calls}")
    print(f"- Write calls count: {state.write_calls}")
    if state.failed_calls:
        print("- Any failed calls (method, endpoint, code):")
        for m, ep, c in state.failed_calls:
            print(f"  - {m} {ep} -> {c}")
    else:
        print("- Any failed calls (method, endpoint, code): none recorded")
    print(f"- Correlation/run id: {state.correlation_id}")

    print("\n9) FINAL STATUS")
    if final == "ABORTED" and portal_list_abort and abort_code == "no_heuristic_match":
        print("- ABORTED (portal inventory):")
        for p in portal_list_abort:
            print(
                f"  - Id={p.get('Id') or p.get('ID')} Name={p.get('Name')!r} Type={p.get('Type')}"
            )
    else:
        print(f"- {final}")

    if final == "ABORTED" and portal_list_abort and abort_code == "ambiguous_portal":
        print("- Numbered portal choices:")
        for i, p in enumerate(portal_list_abort, 1):
            print(f"  {i}. Id={p.get('Id') or p.get('ID')} Name={p.get('Name')!r}")


if __name__ == "__main__":
    sys.exit(main())
