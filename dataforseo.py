#!/usr/bin/env python3
import csv, json, os, random, sys, time, configparser, argparse
import asyncio, aiohttp, aiosqlite
from collections import deque
from datetime import datetime, timezone
from base64 import b64encode
import sqlite3  # only for catching OperationalError if needed
import uuid     # for simulated sv task IDs

# ----------------------------
# Config / constants
# ----------------------------

DEFAULT_CONFIG_PATH = "config.ini"

# DataForSEO rate limits (per minute)
MAX_TASK_POST_CALLS_PER_MINUTE = 2000     # /task_post (2000 calls/min, 100 tasks/call)
MAX_TASKS_READY_CALLS_PER_MINUTE = 20     # /tasks_ready (20 calls/min)
MAX_TASK_GET_CALLS_PER_MINUTE = 2000      # /task_get/{id}

TASK_POST_BATCH_SIZE = 100                # max tasks per POST (API limit)
TASKS_READY_MAX_PER_CALL = 1000           # max tasks returned per /tasks_ready call (informational)

TASK_GET_CONCURRENCY = 25                 # how many /task_get calls to run concurrently

# Simulator: ensure at least this many queries exist (CSV + fake)
SIMULATOR_FAKE_QUERY_COUNT = 5000

# Sandbox dummy task ID that returns a full sample SERP
SANDBOX_DUMMY_TASK_ID = "00000000-0000-0000-0000-000000000000"

# Backoff settings
SIM_BASE_POLL_INTERVAL = 5     # seconds
REAL_BASE_POLL_INTERVAL = 10   # seconds
MAX_POLL_INTERVAL = 3600       # maximum sleep interval (60 minutes) - exponential backoff caps here
MAX_POLL_COUNT = 300           # maximum number of polls before giving up


# ----------------------------
# Async rate limiter
# ----------------------------

class AsyncRateLimiter:
    def __init__(self, max_calls, period_seconds, name: str = ""):
        self.max_calls = max_calls
        self.period = period_seconds
        self.calls = deque()
        self._lock = asyncio.Lock()
        self.name = name  # optional label for logging

    async def wait(self):
        async with self._lock:
            now = time.time()
            # Drop calls older than the window
            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()

            if len(self.calls) >= self.max_calls:
                sleep_for = self.period - (now - self.calls[0]) + 0.01
                if sleep_for > 0:
                    label = self.name or "rate limiter"
                    print(
                        f"\r[RATE-LIMIT] {label}: hit {self.max_calls} calls/{self.period:.0f}s, "
                        f"sleeping {sleep_for:.2f}s."
                    )
                    await asyncio.sleep(sleep_for)

                # Clean up again after sleeping
                now = time.time()
                while self.calls and now - self.calls[0] > self.period:
                    self.calls.popleft()

            self.calls.append(time.time())


# ----------------------------
# DataForSEO Async RestClient
# ----------------------------

class RestClient:
    def __init__(self, username, password, domain="api.dataforseo.com"):
        self.username = username
        self.password = password
        self.domain = domain
        self._session = None

    async def __aenter__(self):
        auth_bytes = f"{self.username}:{self.password}".encode("ascii")
        self._auth_header = b64encode(auth_bytes).decode("ascii")
        self._session = aiohttp.ClientSession(
            base_url=f"https://{self.domain}",
            headers={
                "Authorization": f"Basic {self._auth_header}",
                "Content-Type": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session is not None:
            await self._session.close()

    async def request(self, path, method, data=None):
        if self._session is None:
            raise RuntimeError("RestClient must be used as an async context manager")

        if isinstance(data, (dict, list)):
            body = json.dumps(data)
        else:
            body = data

        # Log first request to verify endpoint (debug)
        if not hasattr(self, '_endpoint_logged'):
            full_url = f"https://{self.domain}{path}"
            print(f"[DEBUG] First API call: {method} {full_url}")
            self._endpoint_logged = True

        async with self._session.request(method, path, data=body) as resp:
            text = await resp.text()
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = {"status_code": 0, "status_message": "Invalid JSON", "raw": text}
            return parsed

    async def get(self, path):
        return await self.request(path, "GET")

    async def post(self, path, data):
        return await self.request(path, "POST", data)


# ----------------------------
# DB setup/helpers (async)
# ----------------------------

async def init_db(db_path):
    conn = await aiosqlite.connect(db_path)
    await conn.execute("PRAGMA foreign_keys = ON;")
    await conn.execute("PRAGMA journal_mode = WAL;")
    await conn.execute("PRAGMA synchronous = NORMAL;")

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL UNIQUE,
            language_code TEXT,
            location_name TEXT,
            se_domain TEXT,
            device TEXT
        )
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            created_at TEXT
        )
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER NOT NULL,
            task_id TEXT NOT NULL UNIQUE,
            status TEXT,
            created_at TEXT,
            updated_at TEXT,
            job_id INTEGER,
            finished_at TEXT,
            FOREIGN KEY(query_id) REFERENCES queries(id),
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        )
        """
    )

    # Schema upgrades for older DBs
    try:
        await conn.execute("ALTER TABLE tasks ADD COLUMN job_id INTEGER")
    except Exception:
        pass
    try:
        await conn.execute("ALTER TABLE tasks ADD COLUMN finished_at TEXT")
    except Exception:
        pass
    # NEW: upgrades for queries
    try:
        await conn.execute("ALTER TABLE queries ADD COLUMN language_code TEXT")
    except Exception:
        pass
    try:
        await conn.execute("ALTER TABLE queries ADD COLUMN location_name TEXT")
    except Exception:
        pass
    try:
        await conn.execute("ALTER TABLE queries ADD COLUMN se_domain TEXT")
    except Exception:
        pass
    try:
        await conn.execute("ALTER TABLE queries ADD COLUMN device TEXT")
    except Exception:
        pass

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            status_code INTEGER,
            status_message TEXT,
            result_json TEXT,
            created_at TEXT
        )
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sv_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER NOT NULL,
            task_id TEXT NOT NULL UNIQUE,
            status TEXT,
            created_at TEXT,
            updated_at TEXT,
            job_id INTEGER,
            FOREIGN KEY(query_id) REFERENCES queries(id),
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        )
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sv_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            status_code INTEGER,
            status_message TEXT,
            result_json TEXT,
            created_at TEXT
        )
        """
    )

    await conn.commit()
    return conn

async def export_view_to_csv(conn, view_name: str, csv_path: str):
    """
    Export a database view to a CSV file.
    
    Args:
        conn: Database connection
        view_name: Name of the view to export
        csv_path: Path to the output CSV file
    """
    try:
        async with conn.execute(f"SELECT * FROM {view_name}") as cur:
            rows = await cur.fetchall()
            if not rows:
                print(f"[EXPORT] View '{view_name}' is empty; skipping CSV export.")
                return
            
            # Get column names
            column_names = [description[0] for description in cur.description]
            
            # Write to CSV
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(column_names)
                writer.writerows(rows)
            
            print(f"[EXPORT] Exported {len(rows)} rows from '{view_name}' to '{csv_path}'")
    except Exception as e:
        print(f"[EXPORT] Error exporting '{view_name}' to '{csv_path}': {e}", file=sys.stderr)


async def create_views(conn):
    """
    Create or replace SQL views used for reporting/search analysis.
    Safe to run every time; uses DROP VIEW IF EXISTS.
    """
    await conn.execute("DROP VIEW IF EXISTS organic_query_ranks")
    await conn.execute("""
    CREATE VIEW organic_query_ranks AS
    WITH latest_results AS (
        -- Get only the most recent result per task_id to avoid duplicates
        SELECT r1.task_id, r1.result_json, r1.created_at
        FROM results r1
        INNER JOIN (
            SELECT task_id, MAX(created_at) as max_created_at
            FROM results
            GROUP BY task_id
        ) r2 ON r1.task_id = r2.task_id AND r1.created_at = r2.max_created_at
    ),
    serp_raw AS (
        SELECT
            q.id          AS query_id,
            q.keyword     AS query,
            q.language_code   AS language_code,
            q.location_name   AS location_name,
            q.se_domain       AS se_domain,
            q.device          AS device,
            lr.result_json     AS serp_json
        FROM latest_results lr
        JOIN tasks   t ON t.task_id = lr.task_id
        JOIN queries q ON q.id      = t.query_id
    ),
    serp_items AS (
        SELECT
            s.query_id,
            s.query,
            s.language_code,
            s.location_name,
            s.se_domain,
            s.device,
            json_extract(je.value, '$.url')           AS ranked_url,
            json_extract(je.value, '$.rank_group')    AS rank,
            json_extract(je.value, '$.rank_absolute') AS absolute_rank
        FROM serp_raw s
        JOIN json_each(s.serp_json, '$.result[0].items') AS je
    ),
    sv_data AS (
        SELECT
            q.keyword AS query_keyword,
            json_extract(svr.result_json, '$.result[0].search_volume') AS search_volume
        FROM sv_results svr
        JOIN sv_tasks svt ON svt.task_id = svr.task_id
        JOIN queries q ON q.id = svt.query_id
        WHERE svr.result_json IS NOT NULL
    )
    SELECT DISTINCT
        si.query,
        COALESCE(sv.search_volume, 0) AS search_volume,
        si.ranked_url,
        si.rank,
        si.absolute_rank,
        si.language_code,
        si.location_name,
        si.se_domain,
        si.device
    FROM serp_items si
    LEFT JOIN sv_data sv ON sv.query_keyword = si.query
    WHERE si.ranked_url IS NOT NULL
    ORDER BY si.query, si.absolute_rank;
    """)

    await conn.execute("DROP VIEW IF EXISTS map_query_ranks")
    await conn.execute("""
    CREATE VIEW map_query_ranks AS
    WITH latest_results AS (
        -- Get only the most recent result per task_id to avoid duplicates
        SELECT r1.task_id, r1.result_json, r1.created_at
        FROM results r1
        INNER JOIN (
            SELECT task_id, MAX(created_at) as max_created_at
            FROM results
            GROUP BY task_id
        ) r2 ON r1.task_id = r2.task_id AND r1.created_at = r2.max_created_at
    ),
    serp_raw AS (
        SELECT
            q.id          AS query_id,
            q.keyword     AS query,
            q.language_code   AS language_code,
            q.location_name   AS location_name,
            q.se_domain       AS se_domain,
            q.device          AS device,
            lr.result_json     AS serp_json
        FROM latest_results lr
        JOIN tasks   t ON t.task_id = lr.task_id
        JOIN queries q ON q.id      = t.query_id
    ),
    map_items AS (
        SELECT
            s.query_id,
            s.query,
            s.language_code,
            s.location_name,
            s.se_domain,
            s.device,
            json_extract(je.value, '$.url')           AS ranked_url,
            json_extract(je.value, '$.title')         AS title,
            json_extract(je.value, '$.domain')        AS domain,
            json_extract(je.value, '$.phone')         AS phone,
            json_extract(je.value, '$.rank_group')    AS rank,
            json_extract(je.value, '$.rank_absolute') AS absolute_rank,
            json_extract(je.value, '$.rating.value')   AS rating_value,
            json_extract(je.value, '$.rating.votes_count') AS rating_votes
        FROM serp_raw s
        JOIN json_each(s.serp_json, '$.result[0].items') AS je
        WHERE json_extract(je.value, '$.type') = 'local_pack'
    ),
    sv_data AS (
        SELECT
            q.keyword AS query_keyword,
            json_extract(svr.result_json, '$.result[0].search_volume') AS search_volume
        FROM sv_results svr
        JOIN sv_tasks svt ON svt.task_id = svr.task_id
        JOIN queries q ON q.id = svt.query_id
        WHERE svr.result_json IS NOT NULL
    )
    SELECT DISTINCT
        mi.query,
        COALESCE(sv.search_volume, 0) AS search_volume,
        mi.ranked_url,
        mi.title,
        mi.domain,
        mi.phone,
        mi.rank,
        mi.absolute_rank,
        mi.rating_value,
        mi.rating_votes,
        mi.language_code,
        mi.location_name,
        mi.se_domain,
        mi.device
    FROM map_items mi
    LEFT JOIN sv_data sv ON sv.query_keyword = mi.query
    WHERE mi.ranked_url IS NOT NULL
    ORDER BY mi.query, mi.absolute_rank;
    """)

    await conn.commit()
    print("[DB] Views created/updated.")

async def create_job(conn, tag=None):
    if tag is None:
        tag = f"run-{datetime.now(timezone.utc).isoformat()}"
    now = datetime.now(timezone.utc).isoformat()
    await conn.execute(
        "INSERT INTO jobs (tag, created_at) VALUES (?, ?)",
        (tag, now),
    )
    await conn.commit()
    async with conn.execute("SELECT last_insert_rowid()") as cur:
        (job_id,) = await cur.fetchone()
    print(f"[JOB] Created job {job_id} (tag={tag})")
    return job_id


async def ensure_min_queries_for_simulator(conn, min_count):
    """
    Ensure there are at least min_count queries in the DB.
    If there are fewer, top up with fake queries.
    """
    async with conn.execute("SELECT COUNT(*) FROM queries") as cur:
        (current_count,) = await cur.fetchone()

    if current_count >= min_count:
        print(f"[SIM] Queries already present: {current_count} (>= {min_count}), skipping top-up.")
        return

    to_add = min_count - current_count
    print(f"[SIM] Topping up with {to_add} fake queries to reach {min_count} total.")

    base = current_count
    for i in range(to_add):
        kw = f"fake query {base + i:06d}"
        await conn.execute(
            "INSERT OR IGNORE INTO queries(keyword) VALUES (?)",
            (kw,),
        )

    await conn.commit()
    print("[SIM] Top-up complete.")


async def count_queries(conn) -> int:
    async with conn.execute("SELECT COUNT(*) FROM queries") as cur:
        (cnt,) = await cur.fetchone()
    return cnt


async def import_queries_from_csv(
    conn,
    csv_path,
    simulator=False,
    fake_count=SIMULATOR_FAKE_QUERY_COUNT,
):
    """
    Imports queries from CSV.

    CSV is expected to have a header row. Supported column names:
      - keyword (or: query, search_term)  [required]
      - language_code                     [optional]
      - location_name                     [optional]
      - se_domain                         [optional]
      - device                            [optional: desktop|mobile]

    - If simulator is True and CSV is missing: continue and just top up with fake queries.
    - If simulator is True and CSV exists: import CSV, then top up with fake queries.
    - If simulator is False and CSV is missing: exit with error.
    """
    if os.path.exists(csv_path):
        print(f"[IMPORT] Loading queries from CSV: {csv_path}")
        imported = 0
        # Use utf-8-sig to automatically strip BOM if present
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                print("[IMPORT] CSV has no header/columns", file=sys.stderr)
                sys.exit(1)

            # Normalise header names
            fieldnames = [fn.strip().lower() for fn in reader.fieldnames]

            def get_field(row, *names):
                for name in names:
                    if name in row and row[name] is not None:
                        val = row[name].strip()
                        if val != "":
                            return val
                return None

            for raw_row in reader:
                # make a case-insensitive dict
                row = {k.strip().lower(): (v or "").strip() for k, v in raw_row.items()}

                keyword = get_field(row, "keyword", "query", "search_term")
                if not keyword:
                    continue

                # skip any keyword that begins with "#" (after stripping whitespace)
                if keyword.lstrip().startswith("#"):
                    # effectively treat this row as a comment
                    continue

                language_code = get_field(row, "language_code", "language")
                location_name = get_field(row, "location_name", "location")
                se_domain = get_field(row, "se_domain", "domain")
                device = get_field(row, "device")

                # Normalise device
                if device:
                    d = device.lower()
                    if d not in ("desktop", "mobile"):
                        print(
                            f"[IMPORT] Warning: unknown device '{device}' for keyword '{keyword}', ignoring."
                        )
                        device = None
                    else:
                        device = d

                # Insert or update
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO queries
                        (keyword, language_code, location_name, se_domain, device)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (keyword, language_code, location_name, se_domain, device),
                )

                # If keyword already existed, update its metadata
                await conn.execute(
                    """
                    UPDATE queries
                    SET
                        language_code = COALESCE(?, language_code),
                        location_name = COALESCE(?, location_name),
                        se_domain     = COALESCE(?, se_domain),
                        device        = COALESCE(?, device)
                    WHERE keyword = ?
                    """,
                    (language_code, location_name, se_domain, device, keyword),
                )

                imported += 1

        await conn.commit()
        print(f"[IMPORT] Imported/updated {imported} queries from CSV.")
    else:
        if simulator:
            print(
                f"[IMPORT] CSV not found at {csv_path}; simulator mode: will generate fake queries."
            )
        else:
            print(f"[IMPORT] CSV file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)

    if simulator:
        await ensure_min_queries_for_simulator(conn, fake_count)

    total = await count_queries(conn)
    print(f"[IMPORT] Total queries in DB: {total}")


async def get_queries_without_task(conn):
    sql = """
        SELECT
            q.id,
            q.keyword,
            q.language_code,
            q.location_name,
            q.se_domain,
            q.device
        FROM queries q
        LEFT JOIN tasks t ON t.query_id = q.id
        WHERE t.id IS NULL
    """
    async with conn.execute(sql) as cur:
        rows = await cur.fetchall()
    return rows


async def get_queries_without_sv_task(conn):
    """
    Return queries that don't yet have a search volume task.
    """
    sql = """
        SELECT
            q.id,
            q.keyword,
            q.language_code,
            q.location_name
        FROM queries q
        LEFT JOIN sv_tasks t ON t.query_id = q.id
        WHERE t.id IS NULL
    """
    async with conn.execute(sql) as cur:
        rows = await cur.fetchall()
    return rows


async def mark_sv_task_in_db(conn, query_id, task_id, status, job_id):
    now = datetime.now(timezone.utc).isoformat()
    await conn.execute(
        """
        INSERT INTO sv_tasks (query_id, task_id, status, created_at, updated_at, job_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (query_id, task_id, status, now, now, job_id),
    )


async def mark_sv_task_status(conn, task_id, status):
    now = datetime.now(timezone.utc).isoformat()
    await conn.execute(
        """
        UPDATE sv_tasks
        SET status = ?, updated_at = ?
        WHERE task_id = ?
        """,
        (status, now, task_id),
    )


async def insert_sv_result(conn, task_id, status_code, status_message, result_json):
    """
    Insert a search volume result for a task. If a result already exists for this task_id,
    delete the old one(s) first to prevent duplicates (keeps only the latest result per task).
    """
    now = datetime.now(timezone.utc).isoformat()
    # Delete any existing results for this task_id to prevent duplicates
    await conn.execute("DELETE FROM sv_results WHERE task_id = ?", (task_id,))
    # Insert the new result
    await conn.execute(
        """
        INSERT INTO sv_results (task_id, status_code, status_message, result_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (task_id, status_code, status_message, result_json, now),
    )


async def count_local_sv_pending_tasks(conn) -> int:
    """
    Count SV tasks that are pending (not completed and not in error state).
    Error statuses like created:40501 are excluded as they won't be processed.
    """
    async with conn.execute(
        """
        SELECT COUNT(*) FROM sv_tasks 
        WHERE (status IS NULL OR status NOT LIKE 'completed:%')
        AND (status IS NULL OR status LIKE 'created:2%' OR status LIKE 'ready%')
        """
    ) as cur:
        (cnt,) = await cur.fetchone()
    return cnt


async def get_local_sv_pending_task_ids(conn, limit=None):
    """
    Get SV task IDs that are pending (not completed and not in error state).
    Error statuses like created:40501 are excluded as they won't be processed.
    """
    sql = """
        SELECT task_id
        FROM sv_tasks
        WHERE (status IS NULL OR status NOT LIKE 'completed:%')
        AND (status IS NULL OR status NOT LIKE 'failed:%')
        AND (status IS NULL OR status LIKE 'created:2%' OR status LIKE 'ready%')
        ORDER BY id
    """
    params = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)
    async with conn.execute(sql, params) as cur:
        rows = await cur.fetchall()
    return [r[0] for r in rows]


async def mark_task_in_db(conn, query_id, task_id, status, job_id):
    now = datetime.now(timezone.utc).isoformat()
    await conn.execute(
        """
        INSERT INTO tasks (query_id, task_id, status, created_at, updated_at, job_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (query_id, task_id, status, now, now, job_id),
    )


async def mark_tasks_ready(conn, task_ids):
    """
    Mark a set of tasks as 'ready' before we fire task_get on them.
    """
    if not task_ids:
        return
    now = datetime.now(timezone.utc).isoformat()
    await conn.executemany(
        """
        UPDATE tasks
        SET status = ?, updated_at = ?
        WHERE task_id = ? AND (status IS NULL OR status NOT LIKE 'completed:%')
        """,
        [("ready", now, tid) for tid in task_ids],
    )
    await conn.commit()


async def mark_task_status(conn, task_id, status):
    now = datetime.now(timezone.utc).isoformat()
    if status.startswith("completed:"):
        await conn.execute(
            """
            UPDATE tasks
            SET status = ?, updated_at = ?, finished_at = ?
            WHERE task_id = ?
            """,
            (status, now, now, task_id),
        )
    else:
        await conn.execute(
            """
            UPDATE tasks
            SET status = ?, updated_at = ?
            WHERE task_id = ?
            """,
            (status, now, task_id),
        )


async def insert_result(conn, task_id, status_code, status_message, result_json):
    """
    Insert a result for a task. If a result already exists for this task_id,
    delete the old one(s) first to prevent duplicates (keeps only the latest result per task).
    """
    now = datetime.now(timezone.utc).isoformat()
    # Delete any existing results for this task_id to prevent duplicates
    await conn.execute("DELETE FROM results WHERE task_id = ?", (task_id,))
    # Insert the new result
    await conn.execute(
        """
        INSERT INTO results (task_id, status_code, status_message, result_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (task_id, status_code, status_message, result_json, now),
    )


async def count_local_pending_tasks(conn) -> int:
    """
    Count tasks that are pending (not completed and not in error state).
    Error statuses like created:40501 are excluded as they won't be processed.
    """
    async with conn.execute(
        """
        SELECT COUNT(*) FROM tasks 
        WHERE (status IS NULL OR status NOT LIKE 'completed:%')
        AND (status IS NULL OR status NOT LIKE 'failed:%')
        AND (status IS NULL OR status LIKE 'created:2%' OR status LIKE 'ready%')
        """
    ) as cur:
        (cnt,) = await cur.fetchone()
    return cnt


async def get_local_pending_task_ids(conn, limit=None):
    """
    Get task IDs that are pending (not completed, not failed, and not in error state).
    Error statuses like created:40501 or failed:40501 are excluded as they won't be processed.
    """
    sql = """
        SELECT task_id FROM tasks 
        WHERE (status IS NULL OR status NOT LIKE 'completed:%')
        AND (status IS NULL OR status NOT LIKE 'failed:%')
        AND (status IS NULL OR status LIKE 'created:2%' OR status LIKE 'ready%')
        ORDER BY id
    """
    params = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)
    async with conn.execute(sql, params) as cur:
        rows = await cur.fetchall()
    return [r[0] for r in rows]


# ----------------------------
# API logic (async)
# ----------------------------

async def submit_tasks_for_pending_queries(
    conn,
    client: RestClient,
    language_code,
    location_name,
    se_domain,
    task_post_rate_limiter: AsyncRateLimiter,
    job_id: int,
):
    """
    Reads all queries without an associated task, batches them into groups of <=100,
    submits to DataForSEO, and stores task IDs in DB linked to the current job.

    Per-query overrides:
      - language_code
      - location_name
      - se_domain
      - device (desktop|mobile, optional; default desktop if omitted)
    """
    pending = await get_queries_without_task(conn)
    total_pending = len(pending)
    print(f"[SUBMIT] Queries without tasks: {total_pending}")

    if total_pending == 0:
        print("[SUBMIT] Nothing new to submit.")
        return 0

    total_submitted = 0
    batch_count = 0

    for i in range(0, total_pending, TASK_POST_BATCH_SIZE):
        batch = pending[i: i + TASK_POST_BATCH_SIZE]
        post_data = {}

        for _, (query_id, keyword, q_lang, q_loc, q_domain, q_device) in enumerate(batch):
            # fall back to global config if per-query value is missing
            lang = q_lang or language_code
            loc = q_loc or location_name
            dom = q_domain or se_domain

            task = {
                "keyword": keyword,
                "language_code": lang,
                "location_name": loc,
                "se_domain": dom,
            }

            # optional device
            if q_device:
                # assume already validated on import: desktop|mobile
                task["device"] = q_device

            post_data[len(post_data)] = task

        await task_post_rate_limiter.wait()
        batch_count += 1
        response = await client.post("/v3/serp/google/organic/task_post", post_data)

        if response.get("status_code") != 20000:
            print(
                f"[SUBMIT] Error from task_post: {response.get('status_code')} {response.get('status_message')}",
                file=sys.stderr,
            )
            continue

        tasks_resp = response.get("tasks", [])
        if not tasks_resp:
            print("[SUBMIT] No tasks returned in response", file=sys.stderr)
            continue

        for (query_id, *_rest), task_item in zip(batch, tasks_resp):
            task_id = task_item.get("id")
            status_code = task_item.get("status_code")
            # Check if task creation failed (error status codes >= 40000)
            if status_code and status_code >= 40000:
                status = f"failed:{status_code}"
                print(
                    f"[SUBMIT] Task creation failed for query_id={query_id}: "
                    f"{status_code} - {task_item.get('status_message', 'Unknown error')}",
                    file=sys.stderr,
                )
            else:
                status = f"created:{status_code}"
            await mark_task_in_db(conn, query_id, task_id, status, job_id)
            total_submitted += 1

        await conn.commit()
    print(f"[SUBMIT] Submitted {total_submitted} task(s) in {batch_count} batch(es) for job {job_id}.")
    return total_submitted


async def submit_sv_tasks_for_pending_queries(
    conn,
    client: RestClient,
    language_code_default,
    location_name_default,
    task_post_rate_limiter: AsyncRateLimiter,
    job_id: int,
):
    """
    Creates search volume tasks for queries without sv_tasks.

    For each query:
      - keywords = [keyword]
      - language_code = per-query language_code or global default
      - location_name = per-query location_name or global default

    Endpoint:
      POST /v3/keywords_data/google_ads/search_volume/task_post
    """
    pending = await get_queries_without_sv_task(conn)
    total_pending = len(pending)
    print(f"[SV-SUBMIT] Queries without search volume tasks: {total_pending}")

    if total_pending == 0:
        print("[SV-SUBMIT] Nothing new to submit.")
        return 0

    total_submitted = 0
    batch_count = 0

    for i in range(0, total_pending, TASK_POST_BATCH_SIZE):
        batch = pending[i: i + TASK_POST_BATCH_SIZE]
        post_data = {}

        for _, (query_id, keyword, q_lang, q_loc) in enumerate(batch):
            lang = q_lang or language_code_default

            task = {
                "keywords": [keyword],
            }
            if lang:
                task["language_code"] = lang
            # Note: Search volume API does not accept location_name parameter
            # Location filtering is not available for search volume tasks

            post_data[len(post_data)] = task

        await task_post_rate_limiter.wait()
        batch_count += 1
        response = await client.post(
            "/v3/keywords_data/google_ads/search_volume/task_post", post_data
        )

        if response.get("status_code") != 20000:
            print(
                f"[SV-SUBMIT] Error from SV task_post: {response.get('status_code')} {response.get('status_message')}",
                file=sys.stderr,
            )
            continue

        tasks_resp = response.get("tasks", []) or []
        if not tasks_resp:
            print("[SV-SUBMIT] No tasks returned in SV response", file=sys.stderr)
            continue

        for (query_id, *_rest), task_item in zip(batch, tasks_resp):
            task_id = task_item.get("id")
            status_code = task_item.get("status_code")
            # Check if task creation failed (error status codes >= 40000)
            if status_code and status_code >= 40000:
                status = f"failed:{status_code}"
                print(
                    f"[SV-SUBMIT] SV task creation failed for query_id={query_id}: "
                    f"{status_code} - {task_item.get('status_message', 'Unknown error')}",
                    file=sys.stderr,
                )
            else:
                status = f"created:{status_code}"
            await mark_sv_task_in_db(conn, query_id, task_id, status, job_id)
            total_submitted += 1

        await conn.commit()

    print(
        f"[SV-SUBMIT] Submitted {total_submitted} search volume task(s) in {batch_count} batch(es) for job {job_id}."
    )
    return total_submitted


# -------- REAL SERP FETCH --------

async def _handle_task_get_real(
    conn,
    client: RestClient,
    task_id: str,
    task_get_rate_limiter: AsyncRateLimiter,
):
    """
    Real /task_get handler:
      - Calls the live API.
      - Stores the result for this task_id.
    """
    # Ensure local task exists
    async with conn.execute(
        "SELECT id FROM tasks WHERE task_id = ?",
        (task_id,),
    ) as cur:
        row = await cur.fetchone()

    if row is None:
        # Quiet skip; can be old DB cruft
        return

    await task_get_rate_limiter.wait()

    path = f"/v3/serp/google/organic/task_get/advanced/{task_id}"
    resp = await client.get(path)

    status_code = resp.get("status_code")
    status_message = resp.get("status_message")

    # Handle error statuses (like 40501 - Task Not Found)
    if status_code and status_code >= 40000:
        # Error status - mark as failed and store error result
        print(
            f"[FETCH] Task {task_id} returned error: {status_code} {status_message}. "
            f"Marking as failed."
        )
        raw_json = json.dumps(resp)
        await insert_result(conn, task_id, status_code, status_message, raw_json)
        await mark_task_status(conn, task_id, f"failed:{status_code}")
        await conn.commit()
        return

    task_items = resp.get("tasks", [])
    if not task_items:
        print(
            f"[FETCH] No task items in task_get for task_id={task_id}. "
            f"DFS status: {status_code} {status_message}",
            file=sys.stderr,
        )
        return

    task_obj = task_items[0]
    raw_json = json.dumps(task_obj)

    await insert_result(conn, task_id, status_code, status_message, raw_json)
    await mark_task_status(conn, task_id, f"completed:{status_code}")
    await conn.commit()


# -------- SERP SIMULATOR --------

async def _fetch_sandbox_sample_task_obj(
    client: RestClient,
    task_get_rate_limiter: AsyncRateLimiter,
):
    """
    Fetch the dummy sandbox SERP once and cache its task_obj.
    """
    await task_get_rate_limiter.wait()
    path = f"/v3/serp/google/organic/task_get/advanced/{SANDBOX_DUMMY_TASK_ID}"
    resp = await client.get(path)

    status_code = resp.get("status_code")
    status_message = resp.get("status_message")
    if status_code != 20000:
        raise RuntimeError(
            f"Failed to fetch sandbox sample task: {status_code} {status_message}"
        )

    task_items = resp.get("tasks", []) or []
    if not task_items:
        raise RuntimeError("Sandbox sample task_get returned no tasks.")

    return task_items[0]


async def _store_simulated_result(
    conn,
    local_task_id: str,
    task_obj_template: dict,
    task_get_rate_limiter: AsyncRateLimiter,
):
    """
    Simulator:
      - Pretend we called /task_get for this task.
      - Use the cached sandbox sample task_obj.
      - Honour the task_get rate limiter.
    """
    # Ensure local task exists
    async with conn.execute(
        "SELECT id FROM tasks WHERE task_id = ?",
        (local_task_id,),
    ) as cur:
        row = await cur.fetchone()
    if row is None:
        return

    # Count this as a logical call for rate-limiting purposes
    await task_get_rate_limiter.wait()

    # You could customise the task_obj here per keyword if you wanted.
    # For now, we just store the template as-is.
    raw_json = json.dumps(task_obj_template)
    status_code = 20000
    status_message = "OK (simulated)"

    await insert_result(conn, local_task_id, status_code, status_message, raw_json)
    await mark_task_status(conn, local_task_id, f"completed:{status_code}")
    await conn.commit()


async def fetch_results_simulator(
    conn,
    client: RestClient,
    tasks_ready_rate_limiter: AsyncRateLimiter,
    task_get_rate_limiter: AsyncRateLimiter,
    base_poll_interval=SIM_BASE_POLL_INTERVAL,
    max_poll_interval=MAX_POLL_INTERVAL,
):
    """
    Simulator mode:

      1. Poll /tasks_ready within its rate limit.
      2. Use the COUNT (up to 1000) as the number of local tasks to mark ready.
         - We take that many local pending tasks from our DB (first N).
         - Mark those tasks as 'ready' in the DB.
      3. For those ready tasks, we simulate /task_get:
         - We fetch the sandbox sample SERP *once* (cached).
         - For each task, we:
             * run task_get_rate_limiter.wait() to mimic 1 call,
             * store the cached sample JSON,
             * mark the task completed.
      4. If no tasks are ready, we increase the standoff delay (exponential backoff)
         up to a max interval.

      This mimics real behaviour and pressure on the rate limiter, but
      only hits the real sandbox task_get endpoint once per run.
    """
    pending_ids = await get_local_pending_task_ids(conn)
    total_pending = len(pending_ids)
    if total_pending == 0:
        print("[FETCH-SIM] No local pending tasks; skipping fetch.")
        return

    # SIM: split tasks into "early" and "late" to mimic tasks that only
    # show up in tasks_ready after a few polls.
    random.shuffle(pending_ids)
    cut = int(total_pending * 0.8)  # 80% early, 20% late
    processing_ids = pending_ids[:cut]
    late_ids = pending_ids[cut:]

    print(
        f"[FETCH-SIM] Starting simulated fetch for {total_pending} local task(s) "
        f"({len(processing_ids)} early, {len(late_ids)} late)."
    )

    sem = asyncio.Semaphore(TASK_GET_CONCURRENCY)
    index = 0  # index into processing_ids (only early at start)
    idle_polls = 0
    poll_num = 0
    sample_task_obj = None

    # We loop while there are still tasks in processing_ids or late_ids waiting to be released.
    while index < len(processing_ids) or late_ids:
        await tasks_ready_rate_limiter.wait()
        poll_num += 1

        # SIM: after a few polls, make late tasks eligible
        if poll_num == 3 and late_ids:
            print("\n[FETCH-SIM] -------------------------------------------------------")
            print("[FETCH-SIM] ⚠️  LATE-ARRIVING TASK EVENT")
            print(f"[FETCH-SIM] Released {len(late_ids)} delayed task(s) into processing.")
            print("[FETCH-SIM] -------------------------------------------------------\n")
            processing_ids.extend(late_ids)
            late_ids = []

        ready = await client.get("/v3/serp/google/organic/tasks_ready")

        if ready.get("status_code") != 20000:
            print(
                f"[FETCH-SIM] Error from tasks_ready: {ready.get('status_code')} {ready.get('status_message')}",
                file=sys.stderr,
            )
            break

        ready_tasks = ready.get("tasks", []) or []
        ready_count = random.randint(800, 1000)

        if ready_count == 0:
            idle_polls += 1
            delay = min(base_poll_interval * (2 ** (idle_polls - 1)), max_poll_interval)
            print(f"[FETCH-SIM] Poll #{poll_num}: no ready tasks (idle={idle_polls}); sleeping {delay:.1f}s.")
            await asyncio.sleep(delay)
            continue

        idle_polls = 0  # we saw something ready

        # Determine how many local tasks we'll mark as ready
        remaining = len(processing_ids) - index
        to_process = min(ready_count, remaining, TASKS_READY_MAX_PER_CALL)
        batch_ids = processing_ids[index: index + to_process]
        index += to_process

        print(
            f"[FETCH-SIM] Poll #{poll_num}: tasks_ready={ready_count}, "
            f"marking {to_process} local task(s) ready; "
            f"progress={index}/{len(processing_ids)}."
        )

        # Mark these tasks as ready in DB
        await mark_tasks_ready(conn, batch_ids)

        # Fetch sandbox sample task_obj once
        if sample_task_obj is None:
            print("[FETCH-SIM] Fetching sandbox sample SERP for simulation...")
            sample_task_obj = await _fetch_sandbox_sample_task_obj(
                client, task_get_rate_limiter
            )
            print("[FETCH-SIM] Sandbox sample SERP cached for simulated task_get calls.")

        async def worker(local_task_id: str):
            async with sem:
                await _store_simulated_result(
                    conn,
                    local_task_id,
                    sample_task_obj,
                    task_get_rate_limiter,
                )

        await asyncio.gather(*(worker(tid) for tid in batch_ids))

    print("[FETCH-SIM] Simulated fetch complete.")


# -------- REAL SERP FETCH LOOP --------

async def fetch_results_real(
    conn,
    client: RestClient,
    tasks_ready_rate_limiter: AsyncRateLimiter,
    task_get_rate_limiter: AsyncRateLimiter,
    base_poll_interval=REAL_BASE_POLL_INTERVAL,
    max_poll_interval=MAX_POLL_INTERVAL,
):
    """
    Real mode:

      1. Poll /tasks_ready within its rate limit.
      2. For each returned task ID:
           - If it exists in our DB, mark it as 'ready'.
           - Those ready tasks are then fetched via /task_get/regular/{id} asynchronously.
           - Unknown (foreign) IDs are ignored.
      3. If no known tasks are ready for a while, we increase standoff delay
         (exponential backoff) up to a max interval and eventually stop.
    """
    pending = await count_local_pending_tasks(conn)
    if pending == 0:
        print("[FETCH] No local pending tasks; skipping real fetch.")
        return

    print(f"[FETCH] Starting real fetch for {pending} local task(s).")

    sem = asyncio.Semaphore(TASK_GET_CONCURRENCY)
    idle_polls = 0
    poll_num = 0

    while True:
        pending = await count_local_pending_tasks(conn)
        if pending == 0:
            print("[FETCH] All local tasks completed; stopping fetch loop.")
            break

        # Safety check: prevent infinite loops
        if poll_num >= MAX_POLL_COUNT:
            print(
                f"[FETCH] Reached maximum poll count ({MAX_POLL_COUNT}). "
                f"Stopping fetch loop. {pending} task(s) still pending."
            )
            break

        await tasks_ready_rate_limiter.wait()
        poll_num += 1
        ready = await client.get("/v3/serp/google/organic/tasks_ready")

        if ready.get("status_code") != 20000:
            print(
                f"[FETCH] Error from tasks_ready: {ready.get('status_code')} {ready.get('status_message')}",
                file=sys.stderr,
            )
            break

        ready_tasks = ready.get("tasks", []) or []
        if not ready_tasks:
            idle_polls += 1
            # Exponential backoff: 10s, 20s, 40s, 80s, 160s, 320s, 640s, capped at MAX_POLL_INTERVAL
            delay = min(base_poll_interval * (2 ** (idle_polls - 1)), max_poll_interval)
            print(f"[FETCH] Poll #{poll_num}: no ready tasks (idle={idle_polls}); sleeping {delay:.1f}s.")
            await asyncio.sleep(delay)
            continue

        # Split into known vs unknown
        # Note: /tasks_ready returns wrapper tasks, actual task IDs are in result[0].id
        # When no tasks are ready, result can be null instead of []
        known_ids = []
        unknown_ids = 0
        for t in ready_tasks:
            # Extract actual task IDs from result array (handle null case)
            result_items = t.get("result") or []
            if not result_items:
                continue
            for result_item in result_items:
                task_id = result_item.get("id")
                if not task_id:
                    continue
                async with conn.execute(
                    "SELECT 1 FROM tasks WHERE task_id = ?",
                    (task_id,),
                ) as cur:
                    row = await cur.fetchone()
                if row:
                    known_ids.append(task_id)
                else:
                    unknown_ids += 1

        if not known_ids:
            idle_polls += 1
            # FALLBACK DISABLED: After 3 idle polls, try fetching pending tasks directly
            # (sometimes /tasks_ready doesn't show ready tasks immediately)
            # if idle_polls >= 3:
            #     print(
            #         f"[FETCH] Poll #{poll_num}: /tasks_ready returned no known tasks after {idle_polls} polls. "
            #         f"Trying direct fetch of {pending} pending task(s)..."
            #     )
            #     # Get pending task IDs and try fetching them directly
            #     pending_ids = await get_local_pending_task_ids(conn, limit=10)
            #     print(f"[FETCH] Found {len(pending_ids)} pending task(s) to fetch directly")
            #     if pending_ids:
            #         # Mark as ready and fetch directly
            #         await mark_tasks_ready(conn, pending_ids)
            #         print(f"[FETCH] Fetching {len(pending_ids)} task(s) directly...")
            #         async def worker(task_id: str):
            #             async with sem:
            #                 await _handle_task_get_real(
            #                     conn,
            #                     client,
            #                     task_id,
            #                     task_get_rate_limiter,
            #                 )
            #         try:
            #             await asyncio.gather(*(worker(tid) for tid in pending_ids))
            #             print(f"[FETCH] Direct fetch completed for {len(pending_ids)} task(s)")
            #         except Exception as e:
            #             print(f"[FETCH] Error during direct fetch: {e}", file=sys.stderr)
            #             import traceback
            #             traceback.print_exc()
            #         # Reset idle counter since we made progress
            #         idle_polls = 0
            #         continue
            #     else:
            #         print("[FETCH] No pending tasks found for direct fetch")
            
            delay = min(base_poll_interval * (2 ** (idle_polls - 1)), max_poll_interval)
            print(
                f"[FETCH] Poll #{poll_num}: ready={len(ready_tasks)}, known=0, "
                f"foreign={unknown_ids} (idle={idle_polls}); sleeping {delay:.1f}s."
            )
            await asyncio.sleep(delay)
            continue

        idle_polls = 0
        print(
            f"[FETCH] Poll #{poll_num}: ready={len(ready_tasks)}, known={len(known_ids)}, "
            f"foreign={unknown_ids}, pending_before={pending}."
        )

        # Mark known tasks as ready
        await mark_tasks_ready(conn, known_ids)

        async def worker(task_id: str):
            async with sem:
                await _handle_task_get_real(
                    conn,
                    client,
                    task_id,
                    task_get_rate_limiter,
                )

        await asyncio.gather(*(worker(tid) for tid in known_ids))

    print("[FETCH] Real fetch complete.")


# -------- REAL SEARCH VOLUME FETCH --------

async def _handle_sv_task_get_real(
    conn,
    client: RestClient,
    task_id: str,
    task_get_rate_limiter: AsyncRateLimiter,
):
    """
    Fetches one search volume task result and stores it.
    Endpoint:
      GET /v3/keywords_data/google_ads/search_volume/task_get/{id}
    """
    # ensure local task exists
    async with conn.execute(
        "SELECT id FROM sv_tasks WHERE task_id = ?",
        (task_id,),
    ) as cur:
        row = await cur.fetchone()
    if row is None:
        return

    await task_get_rate_limiter.wait()

    path = f"/v3/keywords_data/google_ads/search_volume/task_get/{task_id}"
    resp = await client.get(path)

    status_code = resp.get("status_code")
    status_message = resp.get("status_message")

    # Handle error statuses (like 40501 - Task Not Found)
    if status_code and status_code >= 40000:
        # Error status - mark as failed and store error result
        print(
            f"[SV-FETCH] SV Task {task_id} returned error: {status_code} {status_message}. "
            f"Marking as failed."
        )
        raw_json = json.dumps(resp)
        await insert_sv_result(conn, task_id, status_code, status_message, raw_json)
        await mark_sv_task_status(conn, task_id, f"failed:{status_code}")
        await conn.commit()
        return

    task_items = resp.get("tasks", []) or []
    if not task_items:
        print(
            f"[SV-FETCH] No task items in SV task_get for task_id={task_id}. "
            f"DFS status: {status_code} {status_message}",
            file=sys.stderr,
        )
        return

    task_obj = task_items[0]
    raw_json = json.dumps(task_obj)

    await insert_sv_result(conn, task_id, status_code, status_message, raw_json)
    await mark_sv_task_status(conn, task_id, f"completed:{status_code}")
    await conn.commit()


async def fetch_sv_results_real(
    conn,
    client: RestClient,
    tasks_ready_rate_limiter: AsyncRateLimiter,
    task_get_rate_limiter: AsyncRateLimiter,
    base_poll_interval=REAL_BASE_POLL_INTERVAL,
    max_poll_interval=MAX_POLL_INTERVAL,
):
    """
    Real mode search volume fetch:
      - Poll /v3/keywords_data/google_ads/search_volume/tasks_ready
      - For known task IDs, call task_get and store results
    """
    pending = await count_local_sv_pending_tasks(conn)
    if pending == 0:
        print("[SV-FETCH] No local SV pending tasks; skipping fetch.")
        return

    print(f"[SV-FETCH] Starting SV fetch for {pending} local task(s).")

    sem = asyncio.Semaphore(TASK_GET_CONCURRENCY)
    idle_polls = 0
    poll_num = 0

    while True:
        pending = await count_local_sv_pending_tasks(conn)
        if pending == 0:
            print("[SV-FETCH] All SV tasks completed; stopping fetch loop.")
            break

        # Safety check: prevent infinite loops
        if poll_num >= MAX_POLL_COUNT:
            print(
                f"[SV-FETCH] Reached maximum poll count ({MAX_POLL_COUNT}). "
                f"Stopping fetch loop. {pending} SV task(s) still pending."
            )
            break

        await tasks_ready_rate_limiter.wait()
        poll_num += 1
        ready = await client.get(
            "/v3/keywords_data/google_ads/search_volume/tasks_ready"
        )

        if ready.get("status_code") != 20000:
            print(
                f"[SV-FETCH] Error from SV tasks_ready: {ready.get('status_code')} {ready.get('status_message')}",
                file=sys.stderr,
            )
            break

        ready_tasks = ready.get("tasks", []) or []
        if not ready_tasks:
            idle_polls += 1
            delay = min(
                base_poll_interval * (2 ** (idle_polls - 1)), max_poll_interval
            )
            print(
                f"[SV-FETCH] Poll #{poll_num}: no ready SV tasks (idle={idle_polls}); sleeping {delay:.1f}s."
            )
            await asyncio.sleep(delay)
            continue

        # split into known vs unknown
        # Note: /tasks_ready returns wrapper tasks, actual task IDs are in result[0].id
        # When no tasks are ready, result can be null instead of []
        known_ids = []
        unknown_ids = 0
        for t in ready_tasks:
            # Extract actual task IDs from result array (handle null case)
            result_items = t.get("result") or []
            if not result_items:
                continue
            for result_item in result_items:
                t_id = result_item.get("id")
                if not t_id:
                    continue
                async with conn.execute(
                    "SELECT 1 FROM sv_tasks WHERE task_id = ?",
                    (t_id,),
                ) as cur:
                    row = await cur.fetchone()
                if row:
                    known_ids.append(t_id)
                else:
                    unknown_ids += 1

        if not known_ids:
            idle_polls += 1
            # FALLBACK DISABLED: After 3 idle polls, try fetching pending SV tasks directly
            # (sometimes /tasks_ready doesn't show ready tasks immediately)
            # if idle_polls >= 3:
            #     print(
            #         f"[SV-FETCH] Poll #{poll_num}: /tasks_ready returned no known SV tasks after {idle_polls} polls. "
            #         f"Trying direct fetch of {pending} pending SV task(s)..."
            #     )
            #     # Get pending SV task IDs and try fetching them directly
            #     pending_ids = await get_local_sv_pending_task_ids(conn, limit=10)
            #     print(f"[SV-FETCH] Found {len(pending_ids)} pending SV task(s) to fetch directly")
            #     if pending_ids:
            #         print(f"[SV-FETCH] Fetching {len(pending_ids)} SV task(s) directly...")
            #         async def worker(tid: str):
            #             async with sem:
            #                 await _handle_sv_task_get_real(
            #                     conn,
            #                     client,
            #                     tid,
            #                     task_get_rate_limiter,
            #                 )
            #         try:
            #             await asyncio.gather(*(worker(tid) for tid in pending_ids))
            #             print(f"[SV-FETCH] Direct fetch completed for {len(pending_ids)} SV task(s)")
            #         except Exception as e:
            #             print(f"[SV-FETCH] Error during direct fetch: {e}", file=sys.stderr)
            #             import traceback
            #             traceback.print_exc()
            #         # Reset idle counter since we made progress
            #         idle_polls = 0
            #         continue
            #     else:
            #         print("[SV-FETCH] No pending SV tasks found for direct fetch")
            
            delay = min(
                base_poll_interval * (2 ** (idle_polls - 1)), max_poll_interval
            )
            print(
                f"[SV-FETCH] Poll #{poll_num}: ready={len(ready_tasks)}, known=0, "
                f"foreign={unknown_ids} (idle={idle_polls}); sleeping {delay:.1f}s."
            )
            await asyncio.sleep(delay)
            continue

        idle_polls = 0
        print(
            f"[SV-FETCH] Poll #{poll_num}: ready={len(ready_tasks)}, known={len(known_ids)}, "
            f"foreign={unknown_ids}, pending_before={pending}."
        )

        async def worker(tid: str):
            async with sem:
                await _handle_sv_task_get_real(
                    conn,
                    client,
                    tid,
                    task_get_rate_limiter,
                )

        await asyncio.gather(*(worker(tid) for tid in known_ids))

    print("[SV-FETCH] Real SV fetch complete.")


# -------- SEARCH VOLUME SIMULATOR --------

async def submit_sv_tasks_simulator(conn, job_id: int):
    """
    Simulator: create fake sv_tasks with UUIDs, no external API calls.
    """
    pending = await get_queries_without_sv_task(conn)
    total_pending = len(pending)
    print(f"[SV-SIM-SUBMIT] Queries without SV tasks: {total_pending}")

    if total_pending == 0:
        print("[SV-SIM-SUBMIT] Nothing new to submit.")
        return 0

    for (query_id, keyword, q_lang, q_loc) in pending:
        task_id = str(uuid.uuid4())
        status = "created:20000"
        await mark_sv_task_in_db(conn, query_id, task_id, status, job_id)

    await conn.commit()
    print(f"[SV-SIM-SUBMIT] Created {total_pending} simulated SV task(s).")
    return total_pending


async def _store_sv_simulated_result(
    conn,
    task_id: str,
    task_get_rate_limiter: AsyncRateLimiter,
):
    """
    Simulator: generate a fake but realistic search volume result for one SV task.
    """
    # Join sv_tasks -> queries to get keyword etc.
    async with conn.execute(
        """
        SELECT q.keyword, q.language_code, q.location_name
        FROM sv_tasks s
        JOIN queries q ON q.id = s.query_id
        WHERE s.task_id = ?
        """,
        (task_id,),
    ) as cur:
        row = await cur.fetchone()

    if row is None:
        return

    keyword, language_code, location_name = row

    await task_get_rate_limiter.wait()

    # Generate some plausible random numbers for simulation
    base_sv = random.randint(10, 100000)
    monthly = []
    # 12 fake months
    for m in range(12):
        # fluctuate around base_sv
        val = max(0, int(base_sv * (0.6 + 0.4 * random.random())))
        # Just generate dummy month strings; exact dates don't matter for sim
        month_str = f"2023-{(m % 12) + 1:02d}-01"
        monthly.append({"month": month_str, "search_volume": val})

    task_obj = {
        "id": task_id,
        "status_code": 20000,
        "status_message": "OK (simulated)",
        "time": "0.001",
        "cost": 0.0,
        "result_count": 1,
        "path": [
            "v3",
            "keywords_data",
            "google_ads",
            "search_volume",
            "task_get",
            task_id,
        ],
        "data": {
            "keywords": [keyword],
            "language_code": language_code,
            "location_name": location_name,
        },
        "result": [
            {
                "keyword": keyword,
                "search_volume": base_sv,
                "competition_index": round(random.random(), 2),
                "monthly_searches": monthly,
            }
        ],
    }

    raw_json = json.dumps(task_obj)
    status_code = 20000
    status_message = "OK (simulated)"

    await insert_sv_result(conn, task_id, status_code, status_message, raw_json)
    await mark_sv_task_status(conn, task_id, f"completed:{status_code}")
    await conn.commit()


async def fetch_sv_results_simulator(
    conn,
    task_get_rate_limiter: AsyncRateLimiter,
):
    """
    Simulator: resolve all pending SV tasks locally with fake data.
    No external API calls.
    """
    pending_ids = await get_local_sv_pending_task_ids(conn)
    total_pending = len(pending_ids)
    if total_pending == 0:
        print("[SV-SIM-FETCH] No local SV pending tasks; skipping.")
        return

    print(f"[SV-SIM-FETCH] Generating simulated SV results for {total_pending} task(s).")

    sem = asyncio.Semaphore(TASK_GET_CONCURRENCY)

    async def worker(tid: str):
        async with sem:
            await _store_sv_simulated_result(conn, tid, task_get_rate_limiter)

    await asyncio.gather(*(worker(tid) for tid in pending_ids))

    print("[SV-SIM-FETCH] Simulated SV fetch complete.")


# ----------------------------
# Config loader (sync)
# ----------------------------

def load_config(path):
    config = configparser.ConfigParser()
    read_files = config.read(path)
    if not read_files:
        print(f"[CONFIG] Config file not found or unreadable: {path}", file=sys.stderr)
        sys.exit(1)

    username = config.get("api", "username", fallback=None)
    password = config.get("api", "password", fallback=None)
    if not username or not password:
        print("[CONFIG] api.username and api.password must be set in config.ini", file=sys.stderr)
        sys.exit(1)

    simulator = config.getboolean("general", "simulator", fallback=False)

    language_code = config.get("api", "language_code", fallback="EN")
    location_name = config.get(
        "api", "location_name", fallback="New York,New York,United States"
    )
    se_domain = config.get("api", "se_domain", fallback="google.com")

    csv_path = config.get("files", "csv_path", fallback="queries.csv")
    db_path = config.get("files", "db_path", fallback="serp_tasks.db")

    return {
        "username": username,
        "password": password,
        "language_code": language_code,
        "location_name": location_name,
        "se_domain": se_domain,
        "csv_path": csv_path,
        "db_path": db_path,
        "simulator": simulator,
    }


# ----------------------------
# Main
# ----------------------------

async def main_async(args):
    cfg = load_config(args.config)
    if args.csv:
        cfg["csv_path"] = args.csv
    if args.db:
        cfg["db_path"] = args.db

    domain = "sandbox.dataforseo.com" if cfg["simulator"] else "api.dataforseo.com"
    
    print("=== DataForSEO Bulk SERP Loader ===")
    print("Using settings:")
    print(f"  CSV           : {cfg['csv_path']}")
    print(f"  DB            : {cfg['db_path']}")
    print(f"  language_code : {cfg['language_code']}")
    print(f"  location_name : {cfg['location_name']}")
    print(f"  se_domain     : {cfg['se_domain']}")
    print(f"  simulator     : {cfg['simulator']}")
    print(f"  mode          : {args.mode}")
    print(f"  API Endpoint  : https://{domain} {'(SANDBOX)' if cfg['simulator'] else '(PRODUCTION)'}")
    print("Rate limits (per minute):")
    print(f"  POST /task_post      : {MAX_TASK_POST_CALLS_PER_MINUTE} calls, {TASK_POST_BATCH_SIZE} tasks/call max")
    print(f"  GET  /tasks_ready    : {MAX_TASKS_READY_CALLS_PER_MINUTE} calls")
    print(f"  GET  /task_get/{{id}} : {MAX_TASK_GET_CALLS_PER_MINUTE} calls")

    conn = await init_db(cfg["db_path"])
    await create_views(conn)

    # IMPORT
    if args.mode in ("all", "import"):
        await import_queries_from_csv(conn, cfg["csv_path"], simulator=cfg["simulator"])
    else:
        print("[MAIN] Skipping import step (mode != import/all).")

    if args.mode == "import":
        await conn.close()
        print("[MAIN] Done (import only).")
        return

    async with RestClient(cfg["username"], cfg["password"], domain=domain) as client:

        task_post_rl = AsyncRateLimiter(
            MAX_TASK_POST_CALLS_PER_MINUTE, 60, name="POST /task_post"
        )
        tasks_ready_rl = AsyncRateLimiter(
            MAX_TASKS_READY_CALLS_PER_MINUTE, 60, name="GET /tasks_ready"
        )
        task_get_rl = AsyncRateLimiter(
            MAX_TASK_GET_CALLS_PER_MINUTE, 60, name="GET /task_get"
        )

        # ----- SUBMIT PHASE -----
        total_submitted_queries = 0
        if args.mode in ("all", "submit"):
            # SERP job
            serp_job_id = await create_job(conn)
            serp_count = await submit_tasks_for_pending_queries(
                conn,
                client,
                cfg["language_code"],
                cfg["location_name"],
                cfg["se_domain"],
                task_post_rl,
                serp_job_id,
            )
            total_submitted_queries += serp_count or 0

            # Search volume jobs
            if args.sv:
                sv_job_id = await create_job(conn, tag="search-volume")
                if cfg["simulator"]:
                    sv_count = await submit_sv_tasks_simulator(conn, sv_job_id)
                else:
                    sv_count = await submit_sv_tasks_for_pending_queries(
                        conn,
                        client,
                        cfg["language_code"],
                        cfg["location_name"],
                        task_post_rl,
                        sv_job_id,
                    )
                total_submitted_queries += sv_count or 0
        else:
            print("[MAIN] Skipping submit step (mode != submit/all).")

        if args.mode == "submit":
            await conn.close()
            print("[MAIN] Done (submit only).")
            return

        # ----- FETCH PHASE -----
        # If we submitted a small number of queries (< 500), wait 3 minutes before fetching
        # to give the API more time to process them
        if total_submitted_queries > 0 and total_submitted_queries < 500:
            wait_seconds = 180  # 3 minutes
            print(
                f"[MAIN] Submitted {total_submitted_queries} query/queries (< 500). "
                f"Waiting {wait_seconds} seconds before starting fetch to allow API processing time..."
            )
            await asyncio.sleep(wait_seconds)
            print("[MAIN] Wait complete, starting fetch phase.")
        if args.mode in ("all", "fetch"):
            # SERP fetch
            if cfg["simulator"]:
                await fetch_results_simulator(
                    conn, client, tasks_ready_rl, task_get_rl
                )
            else:
                await fetch_results_real(
                    conn, client, tasks_ready_rl, task_get_rl
                )

            # Search volume fetch
            if args.sv:
                if cfg["simulator"]:
                    await fetch_sv_results_simulator(conn, task_get_rl)
                else:
                    await fetch_sv_results_real(
                        conn, client, tasks_ready_rl, task_get_rl
                    )
        else:
            print("[MAIN] Skipping fetch step (mode != fetch/all).")

    # ----- EXPORT VIEWS TO CSV -----
    print("\n[EXPORT] Exporting views to CSV files...")
    base_name = os.path.splitext(cfg["db_path"])[0]  # e.g., "serp_tasks" from "serp_tasks.db"
    await export_view_to_csv(conn, "organic_query_ranks", f"{base_name}_organic_ranks.csv")
    await export_view_to_csv(conn, "map_query_ranks", f"{base_name}_map_ranks.csv")
    
    await conn.close()
    print("[MAIN] Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Bulk DataForSEO SERP task loader with SQLite storage (async)"
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to config.ini (default: config.ini)",
    )
    parser.add_argument(
        "--csv",
        help="Optional CSV path override; if not provided, uses [files] csv_path from config.ini",
    )
    parser.add_argument(
        "--db",
        help="Optional SQLite DB path override; if not provided, uses [files] db_path from config.ini",
    )
    parser.add_argument(
        "--sv",
        action="store_true",
        default=True,
        help="Enable Search Volume tasks (enabled by default)",
    )
    parser.add_argument(
        "--mode",
        choices=["all", "import", "submit", "fetch"],
        default="all",
        help=(
            "Pipeline mode:\n"
            "  all    = import/top-up queries, submit tasks for queries without tasks, then fetch results\n"
            "  import = only import/top-up queries\n"
            "  submit = only submit tasks for queries without tasks (creates a new job)\n"
            "  fetch  = only fetch results for pending tasks\n"
        ),
    )

    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
