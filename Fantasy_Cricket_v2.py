#!/usr/bin/env python3
"""
Fantasy_Cricket_v2.py — IPL Scorecard Fetcher & DB Updater
===========================================================
USAGE (triggered by GitHub Actions with a match link):
  python Fantasy_Cricket_v2.py fetch  '{"match_url":"https://...","mid":5}'
  python Fantasy_Cricket_v2.py status '{"mid":5}'

WORKFLOW:
  1. Admin enters a match link on build-best-team.php admin tab
  2. PHP calls GitHub Actions API (workflow_dispatch) with the match URL + mid
  3. This script runs in GitHub Actions, fetches the scorecard, updates ipl_players in SQL

INSTALL DEPS:
  pip install mysql-connector-python requests beautifulsoup4

GITHUB SECRETS NEEDED:
  DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT (optional, default 3306)
"""

import sys
import json
import os
import datetime

# ── Configuration — loaded from GitHub Secrets / environment ─────────────────
# Set these as GitHub Repo Secrets (Settings → Secrets → Actions)
# or export them in your local shell before running.
# DB credentials come from your config.php on the server — mirror them here as Secrets.
DB_CONFIG = {
    "host":     os.environ["DB_HOST"],      # GitHub Secret: DB_HOST
    "user":     os.environ["DB_USER"],      # GitHub Secret: DB_USER
    "password": os.environ["DB_PASS"],      # GitHub Secret: DB_PASS
    "database": os.environ["DB_NAME"],      # GitHub Secret: DB_NAME
    "port":     int(os.environ.get("DB_PORT", 3306)),
}

# ── Imports ───────────────────────────────────────────────────────────────────
try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False
    print("[WARN] pip install mysql-connector-python", file=sys.stderr)

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    print("[WARN] pip install requests", file=sys.stderr)


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    if not HAS_MYSQL:
        raise RuntimeError("mysql-connector-python not installed.")
    return mysql.connector.connect(**DB_CONFIG)

def db_query(sql, params=(), fetchone=False):
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    res  = cur.fetchone() if fetchone else cur.fetchall()
    cur.close(); conn.close()
    return res

def db_execute(sql, params=()):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    lid = cur.lastrowid
    cur.close(); conn.close()
    return lid

def ensure_tables():
    conn = get_conn()
    cur  = conn.cursor()

    # Sync log
    cur.execute("""
        CREATE TABLE IF NOT EXISTS `fantasy_sync_log` (
            `id`           INT AUTO_INCREMENT PRIMARY KEY,
            `match_id`     INT NOT NULL,
            `team_name`    VARCHAR(50) NOT NULL,
            `last_sync_at` DATETIME NOT NULL,
            `status`       VARCHAR(30) DEFAULT 'done',
            `match_url`    TEXT,
            `note`         TEXT,
            UNIQUE KEY `uk_match_team` (`match_id`, `team_name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # Preview table — holds fetched scorecard data for admin to review before appending
    cur.execute("""
        CREATE TABLE IF NOT EXISTS `fantasy_preview` (
            `id`              INT AUTO_INCREMENT PRIMARY KEY,
            `match_id`        INT NOT NULL,
            `match_url`       TEXT,
            `player_name`     VARCHAR(150) NOT NULL,
            `team`            VARCHAR(50),
            `role`            VARCHAR(20),
            `credits`         DECIMAL(4,1),
            `sel_percent`     DECIMAL(5,2),
            `last_5_pts`      INT DEFAULT 0,
            `venue_pts`       INT DEFAULT 0,
            `is_playing_today` TINYINT(1) DEFAULT 0,
            `raw_data`        TEXT COMMENT 'JSON of scraped raw fields, for reference',
            `fetched_at`      DATETIME DEFAULT CURRENT_TIMESTAMP,
            KEY `idx_mid` (`match_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    cur.close(); conn.close()


# ── DB update helper (called after scorecard is parsed) ──────────────────────
def update_player_in_db(player_name, last_5_pts=None, sel_percent=None,
                        venue_pts=None, is_playing=None):
    """
    Update ipl_players with fresh scorecard data.
    Pass only the fields you want to update (others stay unchanged).

    Call this from your own scorecard-parsing code below.
    """
    updates = []
    vals    = []
    if last_5_pts   is not None: updates.append("last_5_pts=%s");       vals.append(int(last_5_pts))
    if sel_percent  is not None: updates.append("sel_percent=%s");      vals.append(float(sel_percent))
    if venue_pts    is not None: updates.append("venue_pts=%s");        vals.append(int(venue_pts))
    if is_playing   is not None: updates.append("is_playing_today=%s"); vals.append(int(is_playing))

    if not updates:
        return False

    updates.append("updated_at=NOW()")
    vals.append(player_name)

    db_execute(
        f"UPDATE ipl_players SET {', '.join(updates)} WHERE popular_name=%s OR short_name=%s OR player_name=%s",
        tuple(vals + [player_name, player_name])  # match any name column
    )
    return True


def save_preview_player(match_id, match_url, player_name, team='', role='BAT',
                         credits=8.0, sel_percent=0.0, last_5_pts=0,
                         venue_pts=0, is_playing=0, raw_data=None):
    """
    Save a single player's fetched scorecard data to the preview table.
    Call this from your fetch_scorecard() for each player you parse.
    """
    import json as _json
    raw = _json.dumps(raw_data) if raw_data else '{}'
    db_execute("""
        INSERT INTO fantasy_preview
            (match_id, match_url, player_name, team, role, credits, sel_percent,
             last_5_pts, venue_pts, is_playing_today, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (match_id, match_url, player_name, team, role,
          credits, sel_percent, last_5_pts, venue_pts, is_playing, raw))


def clear_preview(match_id):
    """Clear old preview rows for this match before a fresh fetch."""
    db_execute("DELETE FROM fantasy_preview WHERE match_id=%s", (match_id,))


def log_sync(match_id, team_name, match_url='', status='done', note=''):
    db_execute("""
        INSERT INTO fantasy_sync_log (match_id, team_name, last_sync_at, status, match_url, note)
        VALUES (%s, %s, NOW(), %s, %s, %s)
        ON DUPLICATE KEY UPDATE last_sync_at=NOW(), status=%s, match_url=%s, note=%s
    """, (match_id, team_name, status, match_url, note, status, match_url, note))


# ═════════════════════════════════════════════════════════════════════════════
# ★ YOUR SCORECARD PARSING CODE GOES HERE ★
# Receive match_url, fetch the scorecard, and call save_preview_player() for
# each player you find. The data will appear in the admin panel for review.
# ═════════════════════════════════════════════════════════════════════════════
def fetch_scorecard(match_url, mid, team_a, team_b):
    """
    TODO: Add your scorecard-fetching + parsing logic here.

    For each player in the scorecard, call:
        save_preview_player(
            match_id    = mid,
            match_url   = match_url,
            player_name = "Virat Kohli",
            team        = "RCB",
            role        = "BAT",          # BAT / BOWL / ALL / WK
            credits     = 10.5,
            sel_percent = 74.5,
            last_5_pts  = 320,
            venue_pts   = 88,
            is_playing  = 1,
            raw_data    = {"runs":72, "balls":48, ...}   # optional extras
        )

    Return a dict summary at the end.
    """
    # ── EXAMPLE (replace with your real scraper) ──────────────────────────────
    # resp = requests.get(match_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
    # soup = BeautifulSoup(resp.text, 'html.parser')
    # ... parse players ...
    # save_preview_player(mid, match_url, player_name, ...)

    print(f"[INFO] Fetching: {match_url}", file=sys.stderr)
    print(f"[INFO] Match {mid}: {team_a} vs {team_b}", file=sys.stderr)
    print("[WARN] fetch_scorecard() is a placeholder — add your scraping logic.", file=sys.stderr)

    return {"status": "placeholder", "players_saved": 0,
            "message": "Add your scraping logic to fetch_scorecard() in Fantasy_Cricket_v2.py"}
    # ─────────────────────────────────────────────────────────────────────────


# ── Main action: fetch ────────────────────────────────────────────────────────
def action_fetch(payload):
    ensure_tables()
    mid       = int(payload.get("mid", 0))
    match_url = payload.get("match_url", "").strip()
    team_a    = payload.get("team_a", "")
    team_b    = payload.get("team_b", "")

    if not match_url:
        return {"status": "error", "message": "match_url is required"}

    # Clear old preview for this match
    clear_preview(mid)

    for team in [team_a, team_b]:
        if team:
            log_sync(mid, team, match_url, "running", "Fetch started")

    try:
        result = fetch_scorecard(match_url, mid, team_a, team_b)
        n = result.get("players_saved", 0)
        for team in [team_a, team_b]:
            if team:
                log_sync(mid, team, match_url, "preview_ready", f"{n} players in preview")
        return {"action": "fetch", "mid": mid, "match_url": match_url, **result}

    except Exception as e:
        for team in [team_a, team_b]:
            if team:
                log_sync(mid, team, match_url, "error", str(e))
        return {"action": "fetch", "status": "error", "message": str(e)}


# ── Main action: status ───────────────────────────────────────────────────────
def action_status(payload):
    try:
        ensure_tables()
    except Exception:
        pass
    mid = int(payload.get("mid", 0))
    rows = db_query(
        "SELECT team_name, last_sync_at, status, match_url, note FROM fantasy_sync_log WHERE match_id=%s",
        (mid,)
    )
    return {"action": "status", "mid": mid, "logs": rows}


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python Fantasy_Cricket_v2.py <fetch|status> '<json>'"}))
        sys.exit(1)

    action  = sys.argv[1].lower()
    payload = {}
    if len(sys.argv) >= 3:
        try:
            payload = json.loads(sys.argv[2])
        except json.JSONDecodeError as e:
            print(json.dumps({"error": f"Invalid JSON: {e}"}))
            sys.exit(1)

    try:
        result = action_fetch(payload) if action == "fetch" else \
                 action_status(payload) if action == "status" else \
                 {"error": f"Unknown action '{action}'. Valid: fetch, status"}
    except Exception as e:
        result = {"error": str(e), "action": action}

    print(json.dumps(result, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
