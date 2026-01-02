#!/usr/bin/env python3
import os
import time
import uuid
import random
import asyncio
from collections import defaultdict

import httpx

PROCESS_URL = os.getenv("PROCESS_URL", "http://192.168.75.70:8088/v1/process")
ORG_ID = int(os.getenv("ORG_ID", "28"))

N = int(os.getenv("N", "2000"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "50"))
TIMEOUT = float(os.getenv("TIMEOUT", "180"))
RETRIES = int(os.getenv("RETRIES", "4"))
PRINT_EVERY = int(os.getenv("PRINT_EVERY", "50"))

# монитор сегментов (опционально)
SEGMENTS_URL = os.getenv("SEGMENTS_URL", f"http://192.168.75.70:8088/v1/segment_docs?org_id={ORG_ID}")
MONITOR_INTERVAL = float(os.getenv("MONITOR_INTERVAL", "10"))
POST_MONITOR_SECONDS = float(os.getenv("POST_MONITOR_SECONDS", "60"))  # 0 = не мониторить после

BASE_TEXT = (
    "бірінші бөлім өлкетану ордасы рысжан қазына 4 рысжан 6 қазына ілиясова музейлердің жағдайы "
    "кағажу кала берер ме деген күдыфыф ыфыфы ыфыфы ыфыфы лар рухы мен хаяқымыздын қазына мұрасы "
    "өлмес ешпес деген үміт те жок емес жиырма бірінші ғасыр табалдырығында өзім калірлейтін музейімді "
    "келешек ұрпақтың қолына аманат етіп тапсырғалы отырмын аманатқа қиянат жасап көрмеген бабаларымның кағидасын"
)
MIN_LEN = len(BASE_TEXT)
WORDS = BASE_TEXT.split()
CYR = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"

def gen_text(i: int) -> str:
    # детерминированно: одинаково воспроизводится для i
    rng = random.Random(1337 + i)
    parts = [BASE_TEXT]

    # делаем текст >= BASE_TEXT по длине (часто чуть больше, чтобы разнообразить)
    target = max(MIN_LEN, MIN_LEN + rng.randint(0, MIN_LEN))

    while len("\n\n".join(parts)) < target:
        k = rng.randint(60, 160)
        sampled = rng.choices(WORDS, k=k)

        # лёгкая мутация токенов
        for j in range(len(sampled)):
            if rng.random() < 0.06:
                sampled[j] = sampled[j] + rng.choice([".", ",", "!", "?", "—"])

        parts.append(" ".join(sampled))

        # шумовая “строка”
        if rng.random() < 0.25:
            parts.append("".join(rng.choices(CYR, k=rng.randint(12, 28))))

    # добавим уникальный хвост, чтобы точно не было одинаковых документов
    parts.append(f"id={i} u={uuid.uuid4().hex}")
    text = "\n\n".join(parts)
    if len(text) < MIN_LEN:
        text += " " + BASE_TEXT
    return text

def make_payload(i: int) -> dict:
    doc_id = f"load_{i:04d}_{uuid.uuid4().hex[:12]}"
    return {
        "organization_id": ORG_ID,
        "document_id": doc_id,
        "file_name": f"load_{i:04d}.satxt",
        "title": f"POEM load {i:04d}",
        "author": "LoadTest",
        "created_at": "2025-12-28T00:00:00Z",
        "text": gen_text(i),
        "do_index": True,
        "do_search": False,
    }

def count_levels_from_segments(segments):
    c = defaultdict(int)
    for s in segments:
        if s.startswith("l1/"): c["l1"] += 1
        elif s.startswith("l2/"): c["l2"] += 1
        elif s.startswith("l3/"): c["l3"] += 1
        elif s.startswith("l4/"): c["l4"] += 1
        elif s.startswith("l5/"): c["l5"] += 1
        else: c["other"] += 1
    return dict(c)

async def monitor_segments(stop_evt: asyncio.Event):
    if not SEGMENTS_URL:
        return
    async with httpx.AsyncClient(timeout=30) as client:
        while not stop_evt.is_set():
            try:
                r = await client.get(SEGMENTS_URL)
                js = r.json()
                # ожидаем формат: {"segments":[...]} (когда org_id задан)
                segs = js.get("segments", [])
                counts = count_levels_from_segments(segs)
                total = len(segs)
                print(f"[segments] total={total} counts={counts}")
            except Exception as e:
                print(f"[segments] ERROR {e}")
            try:
                await asyncio.wait_for(stop_evt.wait(), timeout=MONITOR_INTERVAL)
            except asyncio.TimeoutError:
                pass

async def post_one(client: httpx.AsyncClient, sem: asyncio.Semaphore, i: int):
    payload = make_payload(i)
    text_len = len(payload["text"])
    assert text_len >= MIN_LEN

    async with sem:
        t0 = time.perf_counter()
        last_err = None
        for attempt in range(RETRIES):
            try:
                r = await client.post(PROCESS_URL, json=payload)
                code = r.status_code

                # ретраи на временные проблемы
                if code in (408, 429, 500, 502, 503, 504):
                    backoff = min(2.0 ** attempt, 8.0) + random.random() * 0.25
                    await asyncio.sleep(backoff)
                    continue

                dt = time.perf_counter() - t0
                return i, code, dt, text_len, (r.text[:300] if code >= 300 else "")
            except Exception as e:
                last_err = str(e)
                backoff = min(2.0 ** attempt, 8.0) + random.random() * 0.25
                await asyncio.sleep(backoff)

        dt = time.perf_counter() - t0
        return i, -1, dt, text_len, (last_err or "unknown error")

async def main():
    print(f"PROCESS_URL={PROCESS_URL}")
    print(f"N={N} CONCURRENCY={CONCURRENCY} ORG_ID={ORG_ID} MIN_LEN={MIN_LEN}")
    if SEGMENTS_URL:
        print(f"SEGMENTS_URL={SEGMENTS_URL}")

    sem = asyncio.Semaphore(CONCURRENCY)
    stop_evt = asyncio.Event()

    mon_task = asyncio.create_task(monitor_segments(stop_evt)) if SEGMENTS_URL else None

    limits = httpx.Limits(max_connections=CONCURRENCY * 2, max_keepalive_connections=CONCURRENCY)
    timeout = httpx.Timeout(TIMEOUT)

    ok = 0
    fail = 0
    t_start = time.perf_counter()
    lat_ok = []

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        tasks = [asyncio.create_task(post_one(client, sem, i)) for i in range(N)]

        done = 0
        for fut in asyncio.as_completed(tasks):
            i, code, dt, text_len, err = await fut
            done += 1

            if 200 <= code < 300:
                ok += 1
                lat_ok.append(dt)
            else:
                fail += 1
                # ошибки печатаем сразу
                print(f"[{i:04d}] FAIL code={code} dt={dt:.3f}s text_len={text_len} err={err}")

            if done % PRINT_EVERY == 0 or done == N:
                elapsed = time.perf_counter() - t_start
                rps = done / elapsed if elapsed > 0 else 0.0
                if lat_ok:
                    s = sorted(lat_ok)
                    p50 = s[int(0.50 * (len(s) - 1))]
                    p95 = s[int(0.95 * (len(s) - 1))]
                else:
                    p50 = p95 = 0.0
                print(f"[progress] done={done}/{N} ok={ok} fail={fail} rps={rps:.2f} p50={p50:.3f}s p95={p95:.3f}s")

    # ещё немного помониторим сегменты после залива (если надо)
    if mon_task:
        if POST_MONITOR_SECONDS > 0:
            try:
                await asyncio.sleep(POST_MONITOR_SECONDS)
            except Exception:
                pass
        stop_evt.set()
        try:
            await mon_task
        except Exception:
            pass

    total_dt = time.perf_counter() - t_start
    print(f"DONE ok={ok} fail={fail} total_time={total_dt:.2f}s")

if __name__ == "__main__":
    asyncio.run(main())
