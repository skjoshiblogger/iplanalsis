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
def _fantasy_pts_bat(runs, balls, fours, sixes, dismissed, wk=False):
    """Estimate batting fantasy points (Dream11 T20 scoring)."""
    pts = runs                          # 1 pt per run
    pts += fours * 1                    # 1 extra per 4
    pts += sixes * 2                    # 2 extra per 6
    if runs >= 100: pts += 16
    elif runs >= 50: pts += 8
    elif runs >= 25: pts += 4
    if dismissed and runs == 0: pts -= 2  # duck
    # Strike rate bonus/penalty for 10+ balls
    if balls >= 10:
        sr = (runs / balls) * 100
        if sr > 170:   pts += 6
        elif sr > 150: pts += 4
        elif sr > 130: pts += 2
        elif sr < 50:  pts -= 6
        elif sr < 60:  pts -= 4
        elif sr < 70:  pts -= 2
    return max(0, pts)

def _fantasy_pts_bowl(wickets, overs, runs_given, maidens):
    """Estimate bowling fantasy points (Dream11 T20 scoring)."""
    pts = wickets * 25
    if wickets >= 5:   pts += 16
    elif wickets >= 4: pts += 8
    elif wickets >= 3: pts += 4
    pts += maidens * 12
    # Economy bonus/penalty for 2+ overs
    if overs >= 2:
        economy = runs_given / overs if overs else 0
        if economy < 5:    pts += 6
        elif economy < 6:  pts += 4
        elif economy < 7:  pts += 2
        elif economy > 11: pts -= 6
        elif economy > 10: pts -= 4
        elif economy > 9:  pts -= 2
    return max(0, pts)

def _safe_int(val, default=0):
    try: return int(str(val).strip().replace('-','0') or default)
    except: return default

def _safe_float(val, default=0.0):
    try: return float(str(val).strip().replace('-','0') or default)
    except: return default

def fetch_scorecard(match_url, mid, team_a, team_b):
    """
    Parse ESPN Cricinfo full-scorecard page and save players to fantasy_preview.
    Works with URLs like:
      https://www.espncricinfo.com/series/.../full-scorecard
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("[ERROR] pip install beautifulsoup4", file=sys.stderr)
        return {"status": "error", "players_saved": 0,
                "message": "beautifulsoup4 not installed. Run: pip install beautifulsoup4"}

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }

    print(f"[INFO] Fetching: {match_url}", file=sys.stderr)
    try:
        resp = requests.get(match_url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        return {"status": "error", "players_saved": 0, "message": f"HTTP error: {e}"}

    soup = BeautifulSoup(resp.text, 'html.parser')

    # ── Detect innings tables ─────────────────────────────────────────────────
    # Cricinfo wraps each innings in a div. Team name appears above each table.
    # We collect all batting rows + bowling rows and figure out teams from context.

    players_data = {}   # player_name -> dict of accumulated stats

    def get_team_name(heading_el):
        """Extract team name from heading element above scorecard table."""
        txt = heading_el.get_text(strip=True) if heading_el else ''
        # "Royal Challengers Bengaluru Innings" → strip "Innings"
        return txt.replace('Innings', '').replace('innings', '').strip()

    # Find all scorecard table containers
    # Cricinfo uses "ds-table" or similar pattern; also look for role="table"
    all_tables = soup.find_all('table')

    innings_blocks = []
    # Each innings has a batting table followed by a bowling table
    # We detect them by looking for header rows with "Batter" or "Bowler"
    for tbl in all_tables:
        headers_row = tbl.find('tr')
        if not headers_row:
            continue
        col_texts = [th.get_text(strip=True).lower() for th in headers_row.find_all(['th','td'])]
        col_str   = ' '.join(col_texts)

        if 'batter' in col_str or ('r' in col_texts and 'b' in col_texts and '4s' in col_texts):
            # Batting table — find nearest team heading
            team_hint = ''
            prev = tbl.find_previous(['h2','h3','h4','div'])
            if prev:
                team_hint = get_team_name(prev)

            rows = tbl.find_all('tr')[1:]   # skip header
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 6:
                    continue
                name_cell = cells[0].get_text(strip=True)
                if not name_cell or name_cell.lower() in ('extras','total','did not bat','fall of wickets'):
                    continue
                # Clean name: "Virat Kohli†" → "Virat Kohli"
                name = name_cell.replace('†','').replace('(c)','').replace('(wk)','').strip()
                is_wk = '†' in name_cell or 'stumped' in cells[1].get_text().lower()

                # Dismissal info tells us if played
                dismissal = cells[1].get_text(strip=True).lower() if len(cells) > 1 else ''
                did_not_bat = 'did not bat' in dismissal or dismissal == ''
                dismissed   = dismissal not in ('not out', 'did not bat', '')

                runs    = _safe_int(cells[2].get_text())
                balls   = _safe_int(cells[3].get_text())
                fours   = _safe_int(cells[4].get_text())
                sixes   = _safe_int(cells[5].get_text())

                if name not in players_data:
                    players_data[name] = {'team': team_hint, 'bat_pts': 0, 'bowl_pts': 0,
                                          'is_wk': False, 'bowled': False, 'is_playing': 0,
                                          'raw': {}}
                players_data[name]['bat_pts'] += _fantasy_pts_bat(runs, balls, fours, sixes, dismissed, is_wk)
                players_data[name]['is_wk']   = players_data[name]['is_wk'] or is_wk
                players_data[name]['is_playing'] = 0 if did_not_bat else 1
                players_data[name]['raw'].update({'runs': runs, 'balls': balls, '4s': fours, '6s': sixes})
                if not players_data[name]['team'] and team_hint:
                    players_data[name]['team'] = team_hint

        elif 'bowler' in col_str or ('w' in col_texts and 'econ' in col_texts):
            # Bowling table
            team_hint = ''
            prev = tbl.find_previous(['h2','h3','h4','div'])
            if prev:
                # Bowling table belongs to the fielding/bowling team — opposite of batting header
                team_hint = get_team_name(prev)

            rows = tbl.find_all('tr')[1:]
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue
                name = cells[0].get_text(strip=True).replace('†','').replace('(c)','').strip()
                if not name or name.lower() in ('extras','total','did not bat'):
                    continue

                overs    = _safe_float(cells[1].get_text())
                maidens  = _safe_int(cells[2].get_text())
                runs_g   = _safe_int(cells[3].get_text())
                wickets  = _safe_int(cells[4].get_text())

                if name not in players_data:
                    players_data[name] = {'team': team_hint, 'bat_pts': 0, 'bowl_pts': 0,
                                          'is_wk': False, 'bowled': False, 'is_playing': 0,
                                          'raw': {}}
                bowl_pts = _fantasy_pts_bowl(wickets, overs, runs_g, maidens)
                players_data[name]['bowl_pts']   += bowl_pts
                players_data[name]['bowled']      = True
                players_data[name]['is_playing']  = 1
                players_data[name]['raw'].update({'wickets': wickets, 'overs': overs,
                                                  'runs_conceded': runs_g, 'economy': round(runs_g/overs, 2) if overs else 0})
                if not players_data[name]['team'] and team_hint:
                    players_data[name]['team'] = team_hint

    if not players_data:
        return {"status": "error", "players_saved": 0,
                "message": "Could not parse any players from the scorecard page. "
                           "Cricinfo may have changed their HTML, or the page required JS rendering."}

    # ── Determine role and credits, save to DB ────────────────────────────────
    saved = 0
    for name, p in players_data.items():
        bat_pts  = p['bat_pts']
        bowl_pts = p['bowl_pts']
        total    = bat_pts + bowl_pts

        # Role detection
        if p['is_wk']:
            role = 'WK'
        elif p['bowled'] and bat_pts > 20:
            role = 'ALL'
        elif p['bowled']:
            role = 'BOWL'
        else:
            role = 'BAT'

        # Estimated credits based on total pts (rough: 8–10.5 range)
        if total >= 60:   cred = 10.0
        elif total >= 40: cred = 9.5
        elif total >= 25: cred = 9.0
        elif total >= 10: cred = 8.5
        else:             cred = 8.0
        if p['is_wk']:    cred = min(cred + 0.5, 10.5)

        save_preview_player(
            match_id    = mid,
            match_url   = match_url,
            player_name = name,
            team        = p['team'],
            role        = role,
            credits     = cred,
            sel_percent = 0.0,      # not on scorecard page
            last_5_pts  = int(total),
            venue_pts   = int(total),   # same match = venue pts proxy
            is_playing  = p['is_playing'],
            raw_data    = p['raw'],
        )
        saved += 1
        print(f"[SAVED] {name} ({role}) pts={total}", file=sys.stderr)

    return {"status": "ok", "players_saved": saved,
            "message": f"Parsed and saved {saved} players from Cricinfo scorecard."}


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
