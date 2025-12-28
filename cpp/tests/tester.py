#!/usr/bin/env python3
"""
tester_process.py

Грузит N документов через POST /v1/process (do_index=true, do_search=false),
потом делает запрос (do_search=true) и печатает modules/sources/plagiarism.

По умолчанию начинает с чистого состояния (wipe_all), чтобы не было upsert по source_id.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_text(base_tokens: List[str], i: int) -> str:
    # backend-нормализованный текст: только пробелы, без табов/переводов строк
    # добавляем вариативность, но сохраняем общий "скелет" для матчей
    tokens = base_tokens + [f"doc{i}", f"v{i%97}"]
    return " ".join(tokens)


def post_json(url: str, payload: Dict[str, Any], timeout: int = 300) -> Dict[str, Any]:
    r = requests.post(url, json=payload, timeout=timeout)
    try:
        r.raise_for_status()
    except Exception as e:
        # показать тело ошибки
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}") from e
    return r.json()


def wipe_all(host: str) -> Dict[str, Any]:
    url = host.rstrip("/") + "/v1/admin/wipe_all"
    r = requests.post(url, params={"confirm": "WIPE_ALL"}, timeout=120)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}


def expected_docs_for_level(target_level: int, fanout: int) -> int:
    # L2 требует fanout документов, L3 fanout^2, L4 fanout^3
    if target_level < 2:
        return 0
    return fanout ** (target_level - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="http://192.168.75.70:8088")
    ap.add_argument("--org", type=int, default=28)
    ap.add_argument("--start-id", type=int, default=101, help="document_id (source_id) start")
    ap.add_argument("--count", type=int, default=0, help="how many docs to index (if 0 -> from --target-level)")
    ap.add_argument("--fanout", type=int, default=3, help="fanout used for compaction tests (3 for fast test)")
    ap.add_argument("--target-level", type=int, default=3, help="desired level to appear (2..4). count auto = fanout^(level-1)")
    ap.add_argument("--wipe", action="store_true", help="wipe_all before run")
    ap.add_argument("--sleep", type=float, default=0.0, help="sleep between requests (seconds)")
    ap.add_argument("--topk", type=int, default=20)
    ap.add_argument("--query-id", type=str, default="999", help="document_id for query request")
    ap.add_argument("--dump-response", action="store_true", help="print full JSON response for final search")
    args = ap.parse_args()

    host = args.host.rstrip("/")
    process_url = host + "/v1/process"

    if args.wipe:
        w = wipe_all(host)
        print("WIPE_ALL:", json.dumps(w, ensure_ascii=False))

    # base tokens: >= 9 токенов гарантированно
    base_tokens = (
        "alpha beta gamma delta epsilon one two three four five six seven eight nine ten "
        "eleven twelve thirteen fourteen fifteen"
    ).split()

    count = args.count
    if count <= 0:
        count = expected_docs_for_level(args.target_level, args.fanout)
        if count <= 0:
            count = args.fanout  # fallback
    print(f"Indexing {count} docs into org={args.org} (fanout={args.fanout}).")

    # 1) index loop
    for k in range(count):
        source_id = str(args.start_id + k)
        title = f"Doc {source_id}"
        author = f"Author {source_id}"
        created_at = iso_utc_now()
        text = make_text(base_tokens, args.start_id + k)

        payload = {
            "document_id": source_id,
            "title": title,
            "author": author,
            "created_at": created_at,
            "text": text,
            "file_name": f"doc_{source_id}.txt",
            "enable_ocr": False,
            "organization_id": args.org,
            "do_index": True,
            "do_search": False,
        }

        resp = post_json(process_url, payload)
        # resp обычно: {"status":"indexed", ...} или если у тебя иначе — просто печатаем кратко
        status = resp.get("status", "ok")
        print(f"[{k+1}/{count}] indexed source_id={source_id} status={status}")

        if args.sleep > 0:
            time.sleep(args.sleep)

    # 2) final search request
    query_text = " ".join(base_tokens[:18])  # гарантированно >= 9 токенов

    qpayload = {
        "document_id": args.query_id,
        "title": "Query",
        "author": "",
        "created_at": iso_utc_now(),
        "text": query_text,
        "file_name": "query.txt",
        "enable_ocr": False,
        "organization_id": args.org,
        "do_index": False,
        "do_search": True,
    }

    out = post_json(process_url, qpayload)

    # summary
    modules = out.get("modules", [])
    sources = out.get("sources", [])
    plag = out.get("plagiarism_percentage", None)

    print("\n=== SEARCH RESULT SUMMARY ===")
    print("document_id:", out.get("document_id"))
    print("status:", out.get("status"))
    print("processed_at:", out.get("processed_at"))
    print("plagiarism_percentage:", plag)
    print("sources:", len(sources))
    print("modules:", len(modules))

    if modules:
        print("\nModules (segment roots):")
        for m in modules:
            print(" -", m.get("module_name"))

    if sources:
        print("\nTop sources:")
        for s in sources[:min(10, len(sources))]:
            print(f" - id={s.get('id')} source_id={s.get('source_id')} name={s.get('name')} author={s.get('author')}")

    if args.dump_response:
        print("\n=== FULL RESPONSE ===")
        print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
