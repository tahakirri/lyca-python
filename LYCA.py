import streamlit as st
import sqlite3
import hashlib
from datetime import datetime, time, timedelta
import os
import re
from PIL import Image
import io
import pandas as pd
import json
import pytz

# Ensure 'data' directory exists before any DB connection
os.makedirs("data", exist_ok=True)

# --- DB connection function ---
def get_db_connection():
    conn = sqlite3.connect("data/requests.db")
    return conn

# --- Ensure DB migration for break_templates column ---
def ensure_break_templates_column():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        if "break_templates" not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN break_templates TEXT")
            conn.commit()
    except Exception as e:
        # Log the error but don't crash the app
        print(f"Warning: Could not ensure break_templates column: {e}")
    finally:
        conn.close()

def apply_swap_on_date(agent_a_id, agent_b_id, date_ymd):
    """Swap the schedules of two agents on a single date across roster tables.
    If the date exists in roster_next schedule, swap there; otherwise swap in roster.
    This exchanges whatever is stored for that date (e.g., 'OFF' vs shift time)."""
    conn = sqlite3.connect("data/requests.db")
    try:
        cur = conn.cursor()

        def load_sched(table, aid):
            cur.execute(f"SELECT schedule FROM {table} WHERE agent_id = ?", (aid,))
            r = cur.fetchone()
            try:
                return (json.loads(r[0]) if r and r[0] else {}), bool(r)
            except Exception:
                return ({}, bool(r))

        def save_sched(table, aid, sched_map, exists_row):
            js = json.dumps(sched_map, ensure_ascii=False)
            if exists_row:
                cur.execute(f"UPDATE {table} SET schedule = ? WHERE agent_id = ?", (js, aid))
            else:
                # Minimal insert when missing
                cur.execute(
                    f"INSERT OR REPLACE INTO {table} (agent_id, name, department, shift, schedule, process, upload_date)\n                     VALUES (?, COALESCE((SELECT name FROM {table} WHERE agent_id = ?), ''),\n                             COALESCE((SELECT department FROM {table} WHERE agent_id = ?), ''),\n                             COALESCE((SELECT shift FROM {table} WHERE agent_id = ?), ''), ?,\n                             COALESCE((SELECT process FROM {table} WHERE agent_id = ?), ''), CURRENT_TIMESTAMP)",
                    (aid, aid, aid, aid, js, aid)
                )

        # Prefer roster_next if either agent has the date there
        sched_a_next, a_next_exists = load_sched('roster_next', agent_a_id)
        sched_b_next, b_next_exists = load_sched('roster_next', agent_b_id)
        if (date_ymd in sched_a_next) or (date_ymd in sched_b_next):
            val_a = sched_a_next.get(date_ymd, '')
            val_b = sched_b_next.get(date_ymd, '')
            sched_a_next[date_ymd], sched_b_next[date_ymd] = val_b, val_a
            save_sched('roster_next', agent_a_id, sched_a_next, a_next_exists)
            save_sched('roster_next', agent_b_id, sched_b_next, b_next_exists)
            conn.commit()
            return True

        # Otherwise operate on current roster
        sched_a, a_exists = load_sched('roster', agent_a_id)
        sched_b, b_exists = load_sched('roster', agent_b_id)
        val_a = sched_a.get(date_ymd, '')
        val_b = sched_b.get(date_ymd, '')
        sched_a[date_ymd], sched_b[date_ymd] = val_b, val_a
        save_sched('roster', agent_a_id, sched_a, a_exists)
        save_sched('roster', agent_b_id, sched_b, b_exists)
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()

def apply_cross_swap(requester_id, target_id, requester_date_ymd, target_date_ymd):
    """Swap two agents' schedules on two dates (each date swapped within itself).
    Example: requester has Work on Mon and OFF on Fri; target has OFF on Mon and Work on Fri.
    After swap: requester OFF Mon, Work Fri; target Work Mon, OFF Fri."""
    ok1 = apply_swap_on_date(requester_id, target_id, requester_date_ymd)
    ok2 = apply_swap_on_date(requester_id, target_id, target_date_ymd)
    return ok1 and ok2

# --- Migration to ensure cross-date columns exist in swap_requests ---
def ensure_swap_cross_date_columns():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(swap_requests)")
        cols = [row[1] for row in cursor.fetchall()]
        if "requester_date" not in cols:
            try:
                cursor.execute("ALTER TABLE swap_requests ADD COLUMN requester_date DATE")
                conn.commit()
            except Exception:
                pass
        if "target_date" not in cols:
            try:
                cursor.execute("ALTER TABLE swap_requests ADD COLUMN target_date DATE")
                conn.commit()
            except Exception:
                pass
    finally:
        conn.close()

ensure_swap_cross_date_columns()

def get_swap_by_id(swap_id):
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, requester_id, target_id, date
            FROM swap_requests
            WHERE id = ?
        ''', (swap_id,))
        return cursor.fetchone()
    finally:
        conn.close()

# --- Helpers to render schedule columns ---
def build_14_day_columns():
    tz = pytz.timezone('Africa/Casablanca')
    start = pd.Timestamp.now(tz=tz).normalize()
    days = [(start + pd.Timedelta(days=i)).strftime('%Y-%m-%d') for i in range(14)]
    return days

def expand_schedule_columns(df):
    """Given a roster DataFrame with 'Schedule' JSON, add 14 day columns with YYYY-MM-DD headers."""
    if df.empty or 'Schedule' not in df.columns:
        return df
    days = build_14_day_columns()
    for d in days:
        df[d] = ''
    for idx, row in df.iterrows():
        try:
            js = json.loads(row['Schedule']) if isinstance(row['Schedule'], str) and row['Schedule'] else {}
        except Exception:
            js = {}
        for d in days:
            val = js.get(d, '')
            df.at[idx, d] = val
    return df

def load_schedule_map(agent_id, table_name):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT schedule FROM {table_name} WHERE agent_id = ?", (agent_id,))
        row = cursor.fetchone()
        if row and row[0]:
            try:
                return json.loads(row[0])
            except Exception:
                return {}
        return {}
    finally:
        conn.close()

def save_schedule_map(agent_id, table_name, schedule_map):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        serialized = json.dumps(schedule_map, ensure_ascii=False)
        cursor.execute(f"UPDATE {table_name} SET schedule = ? WHERE agent_id = ?", (serialized, agent_id))
        conn.commit()
    finally:
        conn.close()

def assign_schedule_value(schedule_map, date_key, value):
    if value:
        schedule_map[date_key] = value
    elif date_key in schedule_map:
        del schedule_map[date_key]

def swap_schedule_values(table_name, requester_id, target_id, date_key):
    requester_schedule = load_schedule_map(requester_id, table_name)
    target_schedule = load_schedule_map(target_id, table_name)
    req_value = requester_schedule.get(date_key, '')
    tgt_value = target_schedule.get(date_key, '')
    assign_schedule_value(requester_schedule, date_key, tgt_value)
    assign_schedule_value(target_schedule, date_key, req_value)
    save_schedule_map(requester_id, table_name, requester_schedule)
    save_schedule_map(target_id, table_name, target_schedule)
    return requester_schedule.get(date_key, ''), target_schedule.get(date_key, '')

def update_agent_shift(agent_id, shift_value):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE roster SET shift = ? WHERE agent_id = ?", (shift_value or '', agent_id))
        conn.commit()
    finally:
        conn.close()

def apply_swap_on_date(requester_id, target_id, date_key):
    today_key = pd.Timestamp.now(tz=pytz.timezone('Africa/Casablanca')).strftime('%Y-%m-%d')
    for table in ['roster_next', 'roster']:
        new_req, new_tgt = swap_schedule_values(table, requester_id, target_id, date_key)
        if table == 'roster' and date_key == today_key:
            update_agent_shift(requester_id, new_req)
            update_agent_shift(target_id, new_tgt)

def apply_cross_swap(requester_id, target_id, requester_date_key, target_date_key):
    """Swap both dates independently between the two agents.
    Effect: for requester_date_key, swap values between requester and target on that same date;
    for target_date_key, do the same. This prevents duplication (e.g., double OFF)."""
    apply_swap_on_date(requester_id, target_id, requester_date_key)
    apply_swap_on_date(requester_id, target_id, target_date_key)

def get_agent_schedules(agent_id):
    """Return (name, department, process, schedule_current_dict, schedule_next_dict)."""
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        # current roster
        cursor.execute('''
            SELECT name, department, process, schedule FROM roster WHERE agent_id = ?
        ''', (agent_id,))
        rcur = cursor.fetchone()
        # next roster
        cursor.execute('''
            SELECT name, department, process, schedule FROM roster_next WHERE agent_id = ?
        ''', (agent_id,))
        rnext = cursor.fetchone()
        name = (rcur and rcur[0]) or (rnext and rnext[0]) or None
        dept = (rcur and rcur[1]) or (rnext and rnext[1]) or None
        proc = (rcur and rcur[2]) or (rnext and rnext[2]) or None
        scur = {}
        snext = {}
        try:
            if rcur and rcur[3]:
                scur = json.loads(rcur[3])
        except Exception:
            scur = {}
        try:
            if rnext and rnext[3]:
                snext = json.loads(rnext[3])
        except Exception:
            snext = {}
        return name, dept, proc, scur, snext
    finally:
        conn.close()

def render_agent_two_week_roster(agent_id):
    name, dept, proc, scur, snext = get_agent_schedules(agent_id)
    tz = pytz.timezone('Africa/Casablanca')
    today = pd.Timestamp.now(tz=tz).normalize()
    # Combine schedules, preferring next over current
    combined = dict(scur)
    for k, v in snext.items():
        combined[k] = v
    # Parse all date keys
    parsed = []
    for k, v in combined.items():
        try:
            dt = pd.to_datetime(k).tz_localize(tz) if not hasattr(pd.to_datetime(k), 'tzinfo') else pd.to_datetime(k)
        except Exception:
            try:
                dt = pd.to_datetime(k).tz_localize(tz)
            except Exception:
                continue
        parsed.append((dt.normalize(), k, v))
    # Toggle to show all available dates or a 14-day view
    show_all = st.checkbox("Show full roster range", value=False, help="View all available dates from current and next rosters")
    if show_all:
        selection = sorted(parsed, key=lambda x: x[0])
        if not selection:
            selection = [(today + pd.Timedelta(days=i), (today + pd.Timedelta(days=i)).strftime('%Y-%m-%d'), '') for i in range(14)]
        days = [s[0].strftime('%Y-%m-%d') for s in selection]
        values = []
        for _, key, _ in selection:
            val = snext.get(key) if key in snext else scur.get(key, '')
            values.append(val)
        df = pd.DataFrame({'Date': days, 'Shift/Off': values})
    else:
        # Split into future (>= today) and past (< today)
        future = sorted([p for p in parsed if p[0] >= today], key=lambda x: x[0])
        past = sorted([p for p in parsed if p[0] < today], key=lambda x: x[0], reverse=True)
        # Take next 14 from future; if not enough, append from past to reach 14
        selection = future[:14]
        if len(selection) < 14:
            needed = 14 - len(selection)
            selection.extend(past[:needed])
            selection = sorted(selection, key=lambda x: x[0])
        # If still empty (no schedule at all), show 14 consecutive days starting today
        if not selection:
            selection = [(today + pd.Timedelta(days=i), (today + pd.Timedelta(days=i)).strftime('%Y-%m-%d'), '') for i in range(14)]
        # Build dataframe
        days = [s[0].strftime('%Y-%m-%d') for s in selection]
        values = []
        for _, key, _ in selection:
            val = snext.get(key) if key in snext else scur.get(key, '')
            values.append(val)
        df = pd.DataFrame({'Date': days, 'Shift/Off': values})
    if name or dept or proc:
        st.write(f"**Name:** {name or '-'} | **Process:** {proc or dept or '-'}")
    st.subheader("My Roster (Next 14 days)")
    st.dataframe(df, use_container_width=True)
def get_agent_id_for_username(username):
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        cols = [r[1] for r in cursor.fetchall()]
        if "agent_id" in cols:
            cursor.execute("SELECT agent_id FROM users WHERE username = ?", (username,))
            row = cursor.fetchone()
            if row and row[0]:
                return str(row[0])
        return username
    finally:
        conn.close()
ensure_break_templates_column()

# --- Backfill users.agent_id if missing ---
def backfill_user_agent_ids():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Ensure column exists before updating
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        if "agent_id" in columns:
            cursor.execute(
                "UPDATE users SET agent_id = username WHERE (agent_id IS NULL OR agent_id = '')"
            )
            conn.commit()
    except Exception as e:
        # Log but do not crash
        print(f"Warning: Could not backfill agent_id: {e}")
    finally:
        conn.close()

backfill_user_agent_ids()

# --- Ensure roster_next table ---
def ensure_roster_next_table():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS roster_next (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                name TEXT NOT NULL,
                department TEXT,
                shift TEXT,
                schedule TEXT,
                process TEXT,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(agent_id)
            )
            """
        )
        conn.commit()
    except Exception as e:
        print(f"Warning: Could not ensure roster_next table: {e}")
    finally:
        conn.close()

ensure_roster_next_table()

# --- Ensure DB migration for agent_id column ---
def ensure_agent_id_column():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        if "agent_id" not in columns:
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN agent_id TEXT UNIQUE")
                conn.commit()
            except sqlite3.OperationalError:
                # If adding with UNIQUE fails, try without UNIQUE
                cursor.execute("ALTER TABLE users ADD COLUMN agent_id TEXT")
                conn.commit()
    except Exception as e:
        # Log the error but don't crash the app
        print(f"Warning: Could not ensure agent_id column: {e}")
    finally:
        conn.close()

ensure_agent_id_column()

# --- Ensure DB migration for process column in roster table ---
def ensure_process_column():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(roster)")
        columns = [row[1] for row in cursor.fetchall()]
        if "process" not in columns:
            cursor.execute("ALTER TABLE roster ADD COLUMN process TEXT")
            conn.commit()
    except Exception as e:
        # Log the error but don't crash the app
        print(f"Warning: Could not ensure process column: {e}")
    finally:
        conn.close()

ensure_process_column()

# --- One-time sync: link users.agent_id from roster name if missing ---
def sync_users_agent_id_from_roster():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # Only update rows where users.agent_id is NULL or empty, using exact or case-insensitive name match
        cur.execute("PRAGMA table_info(users)")
        cols = [r[1] for r in cur.fetchall()]
        if "agent_id" in cols:
            cur.execute(
                """
                UPDATE users
                SET agent_id = (
                    SELECT r.agent_id FROM roster r
                    WHERE r.name = users.username COLLATE NOCASE
                    LIMIT 1
                )
                WHERE (agent_id IS NULL OR agent_id = '')
                AND EXISTS (
                    SELECT 1 FROM roster r2 WHERE r2.name = users.username COLLATE NOCASE
                )
                """
            )
            conn.commit()
    except Exception as e:
        print(f"Warning: Could not sync users.agent_id from roster: {e}")
    finally:
        conn.close()

sync_users_agent_id_from_roster()

def ensure_group_messages_reactions_column():
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(group_messages)")
        columns = [row[1] for row in cursor.fetchall()]
        if "reactions" not in columns:
            try:
                cursor.execute("ALTER TABLE group_messages ADD COLUMN reactions TEXT DEFAULT '{}' ")
                conn.commit()
            except Exception:
                pass
    finally:
        conn.close()

ensure_group_messages_reactions_column()

# --- Create roster and swap tables ---
def create_roster_tables():
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        
        # Roster table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS roster (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                name TEXT NOT NULL,
                department TEXT,
                shift TEXT,
                schedule TEXT,
                process TEXT,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(agent_id)
            )
        ''')
        
        # Swap requests table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS swap_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                requester_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                date DATE NOT NULL,
                requester_date DATE,
                target_date DATE,
                reason TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_by TEXT,
                approved_at TIMESTAMP
            )
        ''')
        
        conn.commit()
    finally:
        conn.close()

create_roster_tables()

def ensure_dropdown_options_table():
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        # Create dropdown_options table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dropdown_options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section TEXT NOT NULL,
                option_value TEXT NOT NULL,
                display_order INTEGER DEFAULT 0
            )
        """)
        
        # Check if we need to populate default values
        cursor.execute("SELECT COUNT(*) FROM dropdown_options")
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Insert default options for late_login
            late_login_defaults = [
                "Disconnected RC",
                "Frozen Ring",
                "PC ISSUE",
                "RC Extension issue",
                "Ring Central issue",
                "Windows issue"
            ]
            for idx, option in enumerate(late_login_defaults):
                cursor.execute(
                    "INSERT INTO dropdown_options (section, option_value, display_order) VALUES (?, ?, ?)",
                    ("late_login", option, idx)
                )
            
            # Insert default options for quality_issues
            quality_defaults = [
                "Audio Issue",
                "Call Drop From Rc",
                "Call Frozen",
                "CRM Issue",
                "Hold Frozen"
            ]
            for idx, option in enumerate(quality_defaults):
                cursor.execute(
                    "INSERT INTO dropdown_options (section, option_value, display_order) VALUES (?, ?, ?)",
                    ("quality_issues", option, idx)
                )
            
            # Insert default options for midshift_issues
            midshift_defaults = [
                "Extension issue",
                "Windows Issue",
                "PC Issue",
                "Disconnected RC",
                "Frozen Ring"
            ]
            for idx, option in enumerate(midshift_defaults):
                cursor.execute(
                    "INSERT INTO dropdown_options (section, option_value, display_order) VALUES (?, ?, ?)",
                    ("midshift_issues", option, idx)
                )
            
            conn.commit()
    finally:
        conn.close()

ensure_dropdown_options_table()

BREAK_LABELS = {
    "lunch": "Lunch Break",
    "early_tea": "Early Tea Break",
    "late_tea": "Late Tea Break"
}

def render_notification_permission_banner(banner_id="notification-permission-banner"):
    """Render a consistent browser notification permission prompt."""
    st.markdown(f"""
    <div id="{banner_id}" class="notification-permission-banner"></div>
    <style>
        .notification-banner-wrapper {{
            padding: 1rem;
            border-radius: 0.5rem;
            border: 1px dashed #334155;
            background: rgba(37, 99, 235, 0.08);
            margin-bottom: 1rem;
        }}
        .notification-banner-wrapper button {{
            margin-top: 0.5rem;
            padding: 0.4rem 0.8rem;
            background-color: #2563eb;
            color: white;
            border: none;
            border-radius: 0.25rem;
            cursor: pointer;
        }}
    </style>
    <script>
    (function() {{
        if (!('Notification' in window)) {{
            return;
        }}
        const container = document.getElementById('{banner_id}');
        if (!container) {{
            return;
        }}
        container.classList.add('notification-permission-banner');
        if (Notification.permission === 'default') {{
            container.innerHTML = `
                <div class="notification-banner-wrapper">
                    <p style="margin: 0;">Enable browser notifications to receive alerts for chat mentions, requests, and break reminders.</p>
                    <button onclick="window.requestLycaNotificationPermission()">Enable Notifications</button>
                </div>
            `;
        }} else {{
            container.innerHTML = '';
        }}
    }})();
    window.requestLycaNotificationPermission = window.requestLycaNotificationPermission || function() {{
        if (!('Notification' in window)) {{
            return;
        }}
        Notification.requestPermission().then(() => {{
            document.querySelectorAll('.notification-permission-banner').forEach(el => {{
                if (el) {{
                    el.innerHTML = '';
                }}
            }});
        }});
    }};
    </script>
    """, unsafe_allow_html=True)

# --------------------------
# Timezone Utility Functions
# --------------------------

def get_casablanca_time():
    """Get current time in Casablanca, Morocco timezone"""
    morocco_tz = pytz.timezone('Africa/Casablanca')
    return datetime.now(morocco_tz).strftime("%Y-%m-%d %H:%M:%S")

def convert_to_casablanca_date(date_str):
    """Convert a date string to Casablanca timezone"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        morocco_tz = pytz.timezone('Africa/Casablanca')
        return dt.date()  # Simplified since stored times are already in Casablanca time
    except:
        return None

def get_date_range_casablanca(date):
    """Get start and end of day in Casablanca time"""
    try:
        start = datetime.combine(date, time.min)
        end = datetime.combine(date, time.max)
        return start, end
    except Exception as e:
        st.error(f"Error processing date: {str(e)}")
        return None, None

# --------------------------
# Database Functions
# --------------------------

def get_db_connection():
    """Create and return a database connection."""
    os.makedirs("data", exist_ok=True)
    return sqlite3.connect("data/requests.db")

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def authenticate(username, password):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        hashed_password = hash_password(password)
        cursor.execute("SELECT role FROM users WHERE LOWER(username) = LOWER(?) AND password = ?", 
                      (username, hashed_password))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        conn.close()

def init_db():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                password TEXT,
                role TEXT CHECK(role IN ('agent', 'admin', 'qa')),
                group_name TEXT
            )
        """)
        # MIGRATION: Add group_name if not exists
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN group_name TEXT")
        except Exception:
            pass
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vip_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender TEXT,
                message TEXT,
                timestamp TEXT,
                mentions TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT,
                request_type TEXT,
                identifier TEXT,
                comment TEXT,
                timestamp TEXT,
                completed INTEGER DEFAULT 0,
                group_name TEXT
            )
        """)
        # MIGRATION: Add group_name if not exists
        try:
            cursor.execute("ALTER TABLE requests ADD COLUMN group_name TEXT")
        except Exception:
            pass
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mistakes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_leader TEXT,
                agent_name TEXT,
                ticket_id TEXT,
                error_description TEXT,
                timestamp TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS group_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender TEXT,
                message TEXT,
                timestamp TEXT,
                mentions TEXT,
                group_name TEXT,
                reactions TEXT DEFAULT '{}',
                is_deleted INTEGER DEFAULT 0,
                edited_at TEXT,
                edited_by TEXT,
                deleted_by TEXT,
                deleted_at TEXT,
                pinned INTEGER DEFAULT 0,
                pinned_by TEXT,
                pinned_at TEXT
            )
        """)
        # MIGRATION: Add group_name if not exists
        try:
            cursor.execute("ALTER TABLE group_messages ADD COLUMN group_name TEXT")
        except Exception:
            pass
        # MIGRATION: Add reactions column if not exists
        try:
            cursor.execute("ALTER TABLE group_messages ADD COLUMN reactions TEXT DEFAULT '{}' ")
        except Exception:
            pass
        for column_name, column_def in [
            ("is_deleted", "INTEGER DEFAULT 0"),
            ("edited_at", "TEXT"),
            ("edited_by", "TEXT"),
            ("deleted_by", "TEXT"),
            ("deleted_at", "TEXT"),
            ("pinned", "INTEGER DEFAULT 0"),
            ("pinned_by", "TEXT"),
            ("pinned_at", "TEXT")
        ]:
            try:
                cursor.execute(f"ALTER TABLE group_messages ADD COLUMN {column_name} {column_def}")
            except Exception:
                pass
        # HOLD TABLE: Add hold_tables table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hold_tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uploader TEXT,
                table_data TEXT,
                timestamp TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                id INTEGER PRIMARY KEY,
                killswitch_enabled INTEGER DEFAULT 0,
                chat_killswitch_enabled INTEGER DEFAULT 0,
                wfm_enabled INTEGER DEFAULT 1
            )
        """)

        # Migration: ensure wfm_enabled exists on older databases *before* inserting
        try:
            cursor.execute("PRAGMA table_info(system_settings)")
            cols = [row[1] for row in cursor.fetchall()]
            if "wfm_enabled" not in cols:
                cursor.execute("ALTER TABLE system_settings ADD COLUMN wfm_enabled INTEGER DEFAULT 1")
        except Exception:
            pass

        # Ensure there is always a row with id=1, including the wfm_enabled column
        cursor.execute(
            "INSERT OR IGNORE INTO system_settings (id, killswitch_enabled, chat_killswitch_enabled, wfm_enabled) "
            "VALUES (1, 0, 0, 1)"
        )

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS muted_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                muted_until TEXT,
                reason TEXT,
                muted_by TEXT,
                created_at TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_notification_settings (
                username TEXT PRIMARY KEY,
                chat_notifications INTEGER DEFAULT 1,
                request_notifications INTEGER DEFAULT 1,
                break_notifications INTEGER DEFAULT 1,
                FOREIGN KEY(username) REFERENCES users(username)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS request_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER,
                user TEXT,
                comment TEXT,
                timestamp TEXT,
                FOREIGN KEY(request_id) REFERENCES requests(id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hold_images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uploader TEXT,
                image_data BLOB,
                timestamp TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS late_logins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT,
                presence_time TEXT,
                login_time TEXT,
                reason TEXT,
                timestamp TEXT,
                status TEXT DEFAULT 'pending',
                approved_by TEXT,
                approved_at TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quality_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT,
                issue_type TEXT,
                timing TEXT,
                mobile_number TEXT,
                product TEXT,
                timestamp TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS midshift_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT,
                issue_type TEXT,
                start_time TEXT,
                end_time TEXT,
                timestamp TEXT
            )
        """)
        
        # Create default admin account
        cursor.execute("""
            INSERT OR IGNORE INTO users (username, password, role) 
            VALUES (?, ?, ?)
        """, ("taha kirri", hash_password("Cursed@99"), "admin"))
        
        # Create other admin accounts
        admin_accounts = [
            ("taha kirri", "Cursed@99"),
            ("admin", "p@ssWord995"),
            ("Malikay", "pass@25**"),
        ]
        
        for username, password in admin_accounts:
            cursor.execute("""
                INSERT OR IGNORE INTO users (username, password, role) 
                VALUES (?, ?, ?)
            """, (username, hash_password(password), "admin"))
        
        # Create agent accounts
        agents = [
            ("agent", "Agent@3356"),
        ]
        
        for agent_name, workspace_id in agents:
            cursor.execute("""
                INSERT OR IGNORE INTO users (username, password, role) 
                VALUES (?, ?, ?)
            """, (agent_name, hash_password(workspace_id), "agent"))

        cursor.execute("""
            INSERT OR IGNORE INTO user_notification_settings (username, chat_notifications, request_notifications, break_notifications)
            SELECT username, 1, 1, 1 FROM users
        """)
        
        conn.commit()
    finally:
        conn.close()

def is_killswitch_enabled():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT killswitch_enabled FROM system_settings WHERE id = 1")
        result = cursor.fetchone()
        return bool(result[0]) if result else False
    finally:
        conn.close()

def is_chat_killswitch_enabled():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT chat_killswitch_enabled FROM system_settings WHERE id = 1")
        result = cursor.fetchone()
        return bool(result[0]) if result else False
    finally:
        conn.close()

def toggle_killswitch(enable):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE system_settings SET killswitch_enabled = ? WHERE id = 1",
                      (1 if enable else 0,))
        conn.commit()
        return True
    finally:
        conn.close()

def toggle_chat_killswitch(enable):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE system_settings SET chat_killswitch_enabled = ? WHERE id = 1",
                      (1 if enable else 0,))
        conn.commit()
        return True
    finally:
        conn.close()

def is_wfm_enabled():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT wfm_enabled FROM system_settings WHERE id = 1")
        result = cursor.fetchone()
        return bool(result[0]) if result is not None else True
    finally:
        conn.close()

def toggle_wfm(enable):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE system_settings SET wfm_enabled = ? WHERE id = 1",
            (1 if enable else 0,)
        )
        conn.commit()
        return True
    finally:
        conn.close()

def add_request(agent_name, request_type, identifier, comment, group_name=None):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        timestamp = get_casablanca_time()
        if group_name is not None:
            cursor.execute("""
                INSERT INTO requests (agent_name, request_type, identifier, comment, timestamp, group_name) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (agent_name, request_type, identifier, comment, timestamp, group_name))
        else:
            cursor.execute("""
                INSERT INTO requests (agent_name, request_type, identifier, comment, timestamp) 
                VALUES (?, ?, ?, ?, ?)
            """, (agent_name, request_type, identifier, comment, timestamp))
        
        request_id = cursor.lastrowid
        
        cursor.execute("""
            INSERT INTO request_comments (request_id, user, comment, timestamp)
            VALUES (?, ?, ?, ?)
        """, (request_id, agent_name, f"Request created: {comment}", timestamp))
        
        conn.commit()
        return True
    finally:
        conn.close()

def get_requests():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM requests ORDER BY timestamp DESC")
        return cursor.fetchall()
    finally:
        conn.close()

def search_requests(query):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = f"%{query.lower()}%"
        cursor.execute("""
            SELECT * FROM requests 
            WHERE LOWER(agent_name) LIKE ? 
            OR LOWER(request_type) LIKE ? 
            OR LOWER(identifier) LIKE ? 
            OR LOWER(comment) LIKE ?
            ORDER BY timestamp DESC
        """, (query, query, query, query))
        return cursor.fetchall()
    finally:
        conn.close()

def update_request_status(request_id, completed):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE requests SET completed = ? WHERE id = ?",
                      (1 if completed else 0, request_id))
        conn.commit()
        return True
    finally:
        conn.close()

def add_request_comment(request_id, user, comment):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO request_comments (request_id, user, comment, timestamp)
            VALUES (?, ?, ?, ?)
        """, (request_id, user, comment, get_casablanca_time()))
        conn.commit()
        return True
    finally:
        conn.close()

def get_request_comments(request_id):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM request_comments 
            WHERE request_id = ?
            ORDER BY timestamp ASC
        """, (request_id,))
        return cursor.fetchall()
    finally:
        conn.close()

def add_mistake(team_leader, agent_name, ticket_id, error_description):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO mistakes (team_leader, agent_name, ticket_id, error_description, timestamp) 
            VALUES (?, ?, ?, ?, ?)
        """, (team_leader, agent_name, ticket_id, error_description, get_casablanca_time()))
        conn.commit()
        return True
    finally:
        conn.close()

def get_mistakes():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mistakes ORDER BY timestamp DESC")
        return cursor.fetchall()
    finally:
        conn.close()

def search_mistakes(query):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = f"%{query.lower()}%"
        cursor.execute("""
            SELECT * FROM mistakes 
            WHERE LOWER(agent_name) LIKE ? 
            OR LOWER(ticket_id) LIKE ? 
            OR LOWER(error_description) LIKE ?
            ORDER BY timestamp DESC
        """, (query, query, query))
        return cursor.fetchall()
    finally:
        conn.close()

def send_group_message(sender, message, group_name=None):
    if is_killswitch_enabled() or is_chat_killswitch_enabled():
        st.error("Chat is currently locked. Please contact the developer.")
        return False

    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        mentions = re.findall(r'@(\w+)', message)
        reactions_json = json.dumps({})
        if group_name is not None:
            cursor.execute("""
                INSERT INTO group_messages (sender, message, timestamp, mentions, group_name, reactions) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (sender, message, get_casablanca_time(), ','.join(mentions), group_name, reactions_json))
        else:
            cursor.execute("""
                INSERT INTO group_messages (sender, message, timestamp, mentions, reactions) 
                VALUES (?, ?, ?, ?, ?)
            """, (sender, message, get_casablanca_time(), ','.join(mentions), reactions_json))
        conn.commit()
        return True
    finally:
        conn.close()

def get_group_messages(group_name=None):
    # Harden: Never allow None, empty, or blank group_name to fetch all messages
    if group_name is None or str(group_name).strip() == "":
        return []
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM group_messages WHERE group_name = ? ORDER BY timestamp DESC LIMIT 50", (group_name,))
        rows = cursor.fetchall()
        messages = []
        for row in rows:
            msg = dict(zip([column[0] for column in cursor.description], row))
            # Parse reactions JSON
            if 'reactions' in msg and msg['reactions']:
                try:
                    msg['reactions'] = json.loads(msg['reactions'])
                except Exception:
                    msg['reactions'] = {}
            else:
                msg['reactions'] = {}
            messages.append(msg)
        return messages
    finally:
        conn.close()

def add_reaction_to_message(message_id, emoji, username):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT reactions FROM group_messages WHERE id = ?", (message_id,))
        row = cursor.fetchone()
        if not row:
            return False
        reactions = json.loads(row[0]) if row[0] else {}
        if emoji not in reactions:
            reactions[emoji] = []
        if username in reactions[emoji]:
            reactions[emoji].remove(username)  # Toggle off
            if not reactions[emoji]:
                del reactions[emoji]
        else:
            reactions[emoji].append(username)
        cursor.execute("UPDATE group_messages SET reactions = ? WHERE id = ?", (json.dumps(reactions), message_id))
        conn.commit()
        return True
    finally:
        conn.close()










def ensure_notification_settings_row(username):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO user_notification_settings (username, chat_notifications, request_notifications, break_notifications)
            VALUES (?, 1, 1, 1)
        """, (username,))
        conn.commit()
    finally:
        conn.close()

def get_user_notification_settings(username):
    ensure_notification_settings_row(username)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT chat_notifications, request_notifications, break_notifications
            FROM user_notification_settings
            WHERE username = ?
        """, (username,))
        row = cursor.fetchone()
        if not row:
            return {
                "chat_notifications": True,
                "request_notifications": True,
                "break_notifications": True
            }
        return {
            "chat_notifications": bool(row[0]),
            "request_notifications": bool(row[1]),
            "break_notifications": bool(row[2])
        }
    finally:
        conn.close()

def update_user_notification_settings(username, chat=None, request=None, breaks=None):
    ensure_notification_settings_row(username)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        current = get_user_notification_settings(username)
        chat_val = chat if chat is not None else current["chat_notifications"]
        request_val = request if request is not None else current["request_notifications"]
        break_val = breaks if breaks is not None else current["break_notifications"]
        cursor.execute("""
            UPDATE user_notification_settings
            SET chat_notifications = ?, request_notifications = ?, break_notifications = ?
            WHERE username = ?
        """, (1 if chat_val else 0, 1 if request_val else 0, 1 if break_val else 0, username))
        conn.commit()
        return True
    finally:
        conn.close()

def get_all_users(include_templates=False):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if include_templates:
            cursor.execute("SELECT id, username, role, group_name, break_templates FROM users")
        else:
            cursor.execute("SELECT id, username, role, group_name FROM users")
        return cursor.fetchall()
    finally:
        conn.close()

def add_user(username, password, role, group_name=None, break_templates=None, agent_id=None):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    # Ensure agent_id column exists so we can persist it when provided
    try:
        ensure_agent_id_column()
    except Exception:
        pass
    # Password complexity check (defense-in-depth)
    def is_password_complex(password):
        if len(password) < 8:
            return False
        if not re.search(r"[A-Z]", password):
            return False
        if not re.search(r"[a-z]", password):
            return False
        if not re.search(r"[0-9]", password):
            return False
        if not re.search(r"[^A-Za-z0-9]", password):
            return False
        return True
    if not is_password_complex(password):
        st.error("Password must be at least 8 characters, include uppercase, lowercase, digit, and special character.")
        return False
    import sqlite3
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check which columns exist in the users table
        cursor.execute("PRAGMA table_info(users)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        if group_name is not None:
            if break_templates is not None:
                break_templates_str = ','.join(break_templates) if isinstance(break_templates, list) else str(break_templates)
                if "agent_id" in existing_columns and agent_id:
                    cursor.execute("INSERT INTO users (username, password, role, group_name, break_templates, agent_id) VALUES (?, ?, ?, ?, ?, ?)",
                                   (username, hash_password(password), role, group_name, break_templates_str, agent_id))
                elif "break_templates" in existing_columns:
                    cursor.execute("INSERT INTO users (username, password, role, group_name, break_templates) VALUES (?, ?, ?, ?, ?)",
                                   (username, hash_password(password), role, group_name, break_templates_str))
                else:
                    cursor.execute("INSERT INTO users (username, password, role, group_name) VALUES (?, ?, ?, ?)",
                                   (username, hash_password(password), role, group_name))
            else:
                if "agent_id" in existing_columns and agent_id:
                    cursor.execute("INSERT INTO users (username, password, role, group_name, agent_id) VALUES (?, ?, ?, ?, ?)",
                                   (username, hash_password(password), role, group_name, agent_id))
                else:
                    cursor.execute("INSERT INTO users (username, password, role, group_name) VALUES (?, ?, ?, ?)",
                                   (username, hash_password(password), role, group_name))
        else:
            if break_templates is not None:
                break_templates_str = ','.join(break_templates) if isinstance(break_templates, list) else str(break_templates)
                if "agent_id" in existing_columns and agent_id:
                    cursor.execute("INSERT INTO users (username, password, role, break_templates, agent_id) VALUES (?, ?, ?, ?, ?)",
                                   (username, hash_password(password), role, break_templates_str, agent_id))
                elif "break_templates" in existing_columns:
                    cursor.execute("INSERT INTO users (username, password, role, break_templates) VALUES (?, ?, ?, ?)",
                                   (username, hash_password(password), role, break_templates_str))
                else:
                    cursor.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
                                   (username, hash_password(password), role))
            else:
                if "agent_id" in existing_columns and agent_id:
                    cursor.execute("INSERT INTO users (username, password, role, agent_id) VALUES (?, ?, ?, ?)",
                                   (username, hash_password(password), role, agent_id))
                else:
                    cursor.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
                                   (username, hash_password(password), role))
        
        conn.commit()
        ensure_notification_settings_row(username)
        return True
    except sqlite3.IntegrityError:
        return "exists"
    finally:
        conn.close()


def delete_user(user_id):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()

# --- Roster Management Functions ---
def upload_roster(roster_data, target='current'):
    """Upload roster data from CSV/Excel"""
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        table = 'roster_next' if str(target).lower() == 'next' else 'roster'
        inserted = 0
        cols = list(roster_data.columns)
        # Build a set of date-like headers to capture per-day roster values
        def is_date_like(col):
            try:
                # pandas may give datetime or string; handle both
                if isinstance(col, str):
                    pd.to_datetime(col)
                    return True
                if hasattr(col, 'strftime'):
                    return True
            except Exception:
                return False
            return False
        # Normalize a header into YYYY-MM-DD if possible
        def normalize_header_to_ymd(col):
            tz = pytz.timezone('Africa/Casablanca')
            today = pd.Timestamp.now(tz=tz)
            # Direct parse
            try:
                return pd.to_datetime(col).strftime('%Y-%m-%d')
            except Exception:
                pass
            # Strip weekday prefix like 'Mon 11/17' -> '11/17' and append year
            try:
                if isinstance(col, str) and ' ' in col:
                    part = col.split()[-1]
                else:
                    part = str(col)
                # If missing year, append current year
                if '/' in part and part.count('/') == 1:
                    part = f"{part}/{today.year}"
                return pd.to_datetime(part).strftime('%Y-%m-%d')
            except Exception:
                return str(col)
        # Prefer fixed Excel range D..Q (0-based indexes 3..16)
        date_cols = []
        if len(cols) >= 4:
            candidate = cols[3:17]
            # Keep the ones that look like dates; if none detect as dates, keep the positional slice anyway
            detected = [c for c in candidate if is_date_like(c)]
            date_cols = detected if detected else candidate
        else:
            # Fallback to autodetection if the sheet has fewer columns
            date_cols = [c for c in cols if is_date_like(c)]
        for _, row in roster_data.iterrows():
            # Robust mapping based on your structure A=ID, B=Process, C=Name with safe positional fallback
            def safe_get(row, key, default=""):
                return row[key] if key in row and pd.notna(row[key]) else default

            # Try header names first, then lettered columns, then positional fallback
            agent_id_val = safe_get(row, 'Agent_ID', safe_get(row, 'ID', safe_get(row, 'A', '')))
            if not agent_id_val and len(cols) > 0:
                agent_id_val = row.iloc[0]

            process_val = safe_get(row, 'Process', safe_get(row, 'B', ''))
            if not process_val and len(cols) > 1:
                process_val = row.iloc[1]

            name_val = safe_get(row, 'Name', safe_get(row, 'C', ''))
            if not name_val and len(cols) > 2:
                name_val = row.iloc[2]

            agent_id = str(agent_id_val).strip()
            process = str(process_val).strip()
            name = str(name_val).strip()

            # Department equals Process (column B) in your sheet
            department = process

            # Extract wide per-date values into a JSON schedule map
            schedule_map = {}
            for dc in date_cols:
                val = row[dc] if dc in row else ''
                if pd.notna(val) and str(val).strip() != '':
                    # normalize header to YYYY-MM-DD string
                    dkey = normalize_header_to_ymd(dc)
                    v = str(val).strip()
                    v_lower = v.lower()
                    if v_lower == 'off':
                        v = 'OFF'
                    elif v_lower == 'ul':
                        v = 'UL'
                    elif v_lower == 'pl':
                        v = 'PL'
                    elif v_lower == 'closed':
                        v = 'CLOSED'
                    schedule_map[dkey] = v

            # Today's shift (if present in schedule_map)
            today_key = pd.Timestamp.now(tz=pytz.timezone('Africa/Casablanca')).strftime('%Y-%m-%d')
            shift = schedule_map.get(today_key, '')
            schedule = json.dumps(schedule_map, ensure_ascii=False)

            if agent_id and name:
                cursor.execute(f'''
                    INSERT OR REPLACE INTO {table} (agent_id, name, department, shift, schedule, process, upload_date)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (agent_id, name, department, shift, schedule, process))
                inserted += 1

        conn.commit()
        # After upload, auto-sync users.agent_id where missing using case-insensitive name match
        try:
            cursor.execute("PRAGMA table_info(users)")
            ucols = [r[1] for r in cursor.fetchall()]
            if "agent_id" in ucols:
                cursor.execute(
                    """
                    UPDATE users
                    SET agent_id = (
                        SELECT r.agent_id FROM {table} r
                        WHERE r.name = users.username COLLATE NOCASE
                        LIMIT 1
                    )
                    WHERE (agent_id IS NULL OR agent_id = '')
                    AND EXISTS (
                        SELECT 1 FROM {table} r2 WHERE r2.name = users.username COLLATE NOCASE
                    )
                    """
                )
                conn.commit()
        except Exception:
            pass
        st.success(f"Roster rows processed: {inserted}")
        return True
    except Exception as e:
        st.error(f"Error uploading roster: {e}")
        return False
    finally:
        conn.close()

def get_roster():
    """Get all roster data"""
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        # Check if agent_id column exists in users table
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "agent_id" in columns:
            cursor.execute('''
                SELECT r.*, u.username 
                FROM roster r 
                LEFT JOIN users u ON r.agent_id = u.agent_id 
                ORDER BY r.agent_id
            ''')
        else:
            cursor.execute('''
                SELECT r.*, NULL as username
                FROM roster r 
                ORDER BY r.agent_id
            ''')
        return cursor.fetchall()
    except Exception as e:
        st.error(f"Error fetching roster: {e}")
        return []
    finally:
        conn.close()

def get_roster_by_table(table_name='roster'):
    """Generic roster fetcher for a table name ('roster' or 'roster_next')"""
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        if "agent_id" in columns:
            cursor.execute(f'''
                SELECT r.*, u.username 
                FROM {table_name} r 
                LEFT JOIN users u ON r.agent_id = u.agent_id 
                ORDER BY r.agent_id
            ''')
        else:
            cursor.execute(f'''
                SELECT r.*, NULL as username
                FROM {table_name} r 
                ORDER BY r.agent_id
            ''')
        return cursor.fetchall()
    except Exception as e:
        st.error(f"Error fetching {table_name.replace('_',' ')}: {e}")
        return []
    finally:
        conn.close()

def get_agent_by_id(agent_id):
    """Get agent details by agent_id"""
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.username, u.role, r.name, r.department, r.shift, r.schedule 
            FROM users u 
            LEFT JOIN roster r ON u.agent_id = r.agent_id 
            WHERE u.agent_id = ?
        ''', (agent_id,))
        row = cursor.fetchone()
        if not row or all(v is None for v in row[2:]):
            # Fallback: try to match by name if agent_id mapping isn't set/aligned
            cursor.execute('''
                SELECT u.username, u.role, r.name, r.department, r.shift, r.schedule 
                FROM users u 
                LEFT JOIN roster r ON LOWER(r.name) = LOWER(u.username)
                WHERE u.username = ?
            ''', (agent_id,))
            row = cursor.fetchone()
        return row
    finally:
        conn.close()

def get_schedule_for_date(agent_id, date_ymd):
    """Return the schedule string (e.g., '9:00-18:00', 'OFF', 'UL', 'PL', 'Closed') for an agent on a given YYYY-MM-DD.
    Checks roster_next first if that date exists there, otherwise falls back to roster.
    """
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        # next roster
        cursor.execute("SELECT schedule FROM roster_next WHERE agent_id = ?", (agent_id,))
        rnext = cursor.fetchone()
        if rnext and rnext[0]:
            try:
                js = json.loads(rnext[0])
                if date_ymd in js:
                    return str(js[date_ymd]).strip()
            except Exception:
                pass
        # current roster
        cursor.execute("SELECT schedule FROM roster WHERE agent_id = ?", (agent_id,))
        rcur = cursor.fetchone()
        if rcur and rcur[0]:
            try:
                js = json.loads(rcur[0])
                if date_ymd in js:
                    return str(js[date_ymd]).strip()
            except Exception:
                pass
        return ""
    finally:
        conn.close()

def create_swap_request(requester_id, target_id, date, reason, requester_date=None, target_date=None):
    """Create a swap request.
    Flow:
    - New request is created with status 'pending_agent' (waiting for target agent decision).
    - When target agent accepts, status becomes 'pending_admin'.
    - Admin can then approve, which applies the actual roster swap.
    """
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        
        # Check if both agents have the same process
        cursor.execute('''
            SELECT process FROM roster WHERE agent_id = ?
        ''', (requester_id,))
        requester_process = cursor.fetchone()
        
        cursor.execute('''
            SELECT process FROM roster WHERE agent_id = ?
        ''', (target_id,))
        target_process = cursor.fetchone()
        
        if requester_process and target_process:
            requester_process_val = requester_process[0] if requester_process[0] else ''
            target_process_val = target_process[0] if target_process[0] else ''
            
            if requester_process_val != target_process_val:
                st.error(f"Shift swap not allowed: Agents must have the same process. "
                        f"Requester process: '{requester_process_val or 'Not set'}', "
                        f"Target process: '{target_process_val or 'Not set'}'")
                return False
        else:
            st.warning("Process information not found for one or both agents. Please ensure the roster is complete.")
        
        # Enforce swap rules for special codes
        # Normalize dates
        def norm(d):
            try:
                return pd.to_datetime(d).strftime('%Y-%m-%d')
            except Exception:
                return str(d)
        r_date = norm(requester_date or date)
        t_date = norm(target_date or date)

        # Disallow swapping for today's date or any past date
        tz = pytz.timezone('Africa/Casablanca')
        today_key = pd.Timestamp.now(tz=tz).strftime('%Y-%m-%d')
        if r_date <= today_key or t_date <= today_key:
            st.error("Swap not allowed for today or past dates. Please choose future dates.")
            return False

        requester_day = get_schedule_for_date(requester_id, r_date)
        target_day = get_schedule_for_date(target_id, t_date)
        forbidden = {"UL", "Pl", "pl", "ul", "PL"}
        if (requester_day in forbidden) or (target_day in forbidden):
            st.error("Swap not allowed: UL or PL days cannot be swapped.")
            return False

        # Insert with cross-date columns if available
        cursor.execute("PRAGMA table_info(swap_requests)")
        cols = [row[1] for row in cursor.fetchall()]
        # Ensure we always start in an explicit agent-pending state
        if "requester_date" in cols and "target_date" in cols:
            cursor.execute('''
                INSERT INTO swap_requests (requester_id, target_id, date, requester_date, target_date, reason, status)
                VALUES (?, ?, ?, ?, ?, ?, 'pending_agent')
            ''', (requester_id, target_id, t_date, r_date, t_date, reason))
        else:
            cursor.execute('''
                INSERT INTO swap_requests (requester_id, target_id, date, reason, status)
                VALUES (?, ?, ?, ?, 'pending_agent')
            ''', (requester_id, target_id, t_date, reason))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error creating swap request: {e}")
        return False
    finally:
        conn.close()

def get_swap_requests(status='pending_agent'):
    """Get swap requests by status in a stable column order."""
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                sr.id,
                sr.requester_id,
                sr.target_id,
                sr.date,
                sr.requester_date,
                sr.target_date,
                sr.status,
                sr.reason,
                sr.created_at,
                sr.approved_by,
                sr.approved_at,
                COALESCE(r1.name, u1.username) as requester_name,
                COALESCE(r2.name, u2.username) as target_name
            FROM swap_requests sr
            LEFT JOIN roster r1 ON sr.requester_id = r1.agent_id
            LEFT JOIN users u1 ON u1.agent_id = sr.requester_id
            LEFT JOIN roster r2 ON sr.target_id = r2.agent_id
            LEFT JOIN users u2 ON u2.agent_id = sr.target_id
            WHERE sr.status = ?
            ORDER BY sr.created_at DESC
        ''', (status,))
        return cursor.fetchall()
    finally:
        conn.close()

def respond_to_swap_request_as_agent(swap_id, acting_agent_id, accept):
    """Handle the target agent's decision on a swap request.
    - Only the target agent can respond.
    - If accept=True and current status is 'pending_agent', move to 'pending_admin'.
    - If accept=False, mark as 'rejected'.
    """
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, requester_id, target_id, status
            FROM swap_requests
            WHERE id = ?
        ''', (swap_id,))
        row = cursor.fetchone()
        if not row:
            st.error("Swap request not found.")
            return False
        _, requester_id, target_id, status = row
        # Ensure only the target agent can act at this step
        if str(target_id) != str(acting_agent_id):
            st.error("You are not allowed to respond to this swap request.")
            return False
        if status != 'pending_agent':
            st.error("This swap request is no longer awaiting agent approval.")
            return False
        new_status = 'pending_admin' if accept else 'rejected'
        cursor.execute('''
            UPDATE swap_requests
            SET status = ?, approved_at = CASE WHEN ? = 'rejected' THEN CURRENT_TIMESTAMP ELSE approved_at END
            WHERE id = ?
        ''', (new_status, new_status, swap_id))
        conn.commit()
        return True
    finally:
        conn.close()

def approve_swap_request(swap_id, approved_by):
    """Approve a swap request as an admin.
    This should be called only after the target agent has accepted,
    i.e. when status is 'pending_admin'. On success, status becomes
    'approved' and the actual roster swap is applied.
    """
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        # Load current status to ensure we only approve when waiting for admin
        cursor.execute('''
            SELECT status FROM swap_requests WHERE id = ?
        ''', (swap_id,))
        row = cursor.fetchone()
        if not row:
            st.error("Swap request not found.")
            return False
        current_status = row[0]
        if current_status != 'pending_admin':
            st.error("Swap request must be accepted by the agent before admin approval.")
            return False

        swap_row = get_swap_by_id(swap_id)
        cursor.execute('''
            UPDATE swap_requests 
            SET status = 'approved', approved_by = ?, approved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (approved_by, swap_id))
        conn.commit()
        if swap_row:
            # fetch full row with possible cross dates
            cursor.execute("SELECT requester_id, target_id, date, requester_date, target_date FROM swap_requests WHERE id = ?", (swap_id,))
            r = cursor.fetchone()
            if r:
                req_id, tgt_id, base_date, r_date, t_date = r
                def norm(d):
                    try:
                        return pd.to_datetime(d).strftime('%Y-%m-%d')
                    except Exception:
                        return str(d)
                if r_date and t_date:
                    apply_cross_swap(req_id, tgt_id, norm(r_date), norm(t_date))
                elif base_date:
                    apply_swap_on_date(req_id, tgt_id, norm(base_date))
        return True
    except Exception:
        return False
    finally:
        conn.close()

def reject_swap_request(swap_id, approved_by):
    """Reject a swap request"""
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = sqlite3.connect("data/requests.db")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE swap_requests 
            SET status = 'rejected', approved_by = ?, approved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (approved_by, swap_id))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()
        
def reset_password(username, new_password):
    """Reset a user's password"""
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    # Password complexity check (defense-in-depth)
    def is_password_complex(password):
        if len(password) < 8:
            return False
        if not re.search(r"[A-Z]", password):
            return False
        if not re.search(r"[a-z]", password):
            return False
        if not re.search(r"[0-9]", password):
            return False
        if not re.search(r"[^A-Za-z0-9]", password):
            return False
        return True
    if not is_password_complex(new_password):
        st.error("Password must be at least 8 characters, include uppercase, lowercase, digit, and special character.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        hashed_password = hash_password(new_password)
        cursor.execute("UPDATE users SET password = ? WHERE username = ?", 
                     (hashed_password, username))
        conn.commit()
        return True
    finally:
        conn.close()

def add_hold_image(uploader, image_data):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO hold_images (uploader, image_data, timestamp) 
            VALUES (?, ?, ?)
        """, (uploader, image_data, get_casablanca_time()))
        conn.commit()
        return True
    finally:
        conn.close()

def get_hold_images():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hold_images ORDER BY timestamp DESC")
        return cursor.fetchall()
    finally:
        conn.close()

def clear_hold_images():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM hold_images")
        conn.commit()
        return True
    finally:
        conn.close()

def clear_all_requests():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM requests")
        cursor.execute("DELETE FROM request_comments")
        conn.commit()
        return True
    finally:
        conn.close()

def clear_all_mistakes():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM mistakes")
        conn.commit()
        return True
    finally:
        conn.close()

def clear_all_group_messages():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM group_messages")
        conn.commit()
        return True
    finally:
        conn.close()

def add_late_login(agent_name, presence_time, login_time, reason):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Ensure migration for late_logins approval fields
        try:
            cursor.execute("PRAGMA table_info(late_logins)")
            cols = [row[1] for row in cursor.fetchall()]
            if "status" not in cols:
                cursor.execute("ALTER TABLE late_logins ADD COLUMN status TEXT DEFAULT 'pending'")
            if "approved_by" not in cols:
                cursor.execute("ALTER TABLE late_logins ADD COLUMN approved_by TEXT")
            if "approved_at" not in cols:
                cursor.execute("ALTER TABLE late_logins ADD COLUMN approved_at TEXT")
        except Exception:
            pass

        cursor.execute("""
            INSERT INTO late_logins (agent_name, presence_time, login_time, reason, timestamp, status) 
            VALUES (?, ?, ?, ?, ?, 'pending')
        """, (agent_name, presence_time, login_time, reason, get_casablanca_time()))
        conn.commit()
        return True
    finally:
        conn.close()

def approve_late_login(entry_id, admin_username):
    """Mark a late login as approved by an admin."""
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE late_logins
            SET status = 'approved', approved_by = ?, approved_at = ?
            WHERE id = ?
        """, (admin_username, get_casablanca_time(), entry_id))
        conn.commit()
        return True
    finally:
        conn.close()

def reject_late_login(entry_id, admin_username):
    """Mark a late login as rejected by an admin."""
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE late_logins
            SET status = 'rejected', approved_by = ?, approved_at = ?
            WHERE id = ?
        """, (admin_username, get_casablanca_time(), entry_id))
        conn.commit()
        return True
    finally:
        conn.close()

def get_late_logins():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM late_logins ORDER BY timestamp DESC")
        return cursor.fetchall()
    finally:
        conn.close()

def add_quality_issue(agent_name, issue_type, timing, mobile_number, product):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO quality_issues (agent_name, issue_type, timing, mobile_number, product, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (agent_name, issue_type, timing, mobile_number, product, get_casablanca_time()))
        conn.commit()
        return True
    finally:
        conn.close()

def get_quality_issues():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM quality_issues ORDER BY timestamp DESC")
        return cursor.fetchall()
    except Exception as e:
        st.error(f"Error fetching quality issues: {str(e)}")
    finally:
        conn.close()

def add_midshift_issue(agent_name, issue_type, start_time, end_time):
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO midshift_issues (agent_name, issue_type, start_time, end_time, timestamp) 
            VALUES (?, ?, ?, ?, ?)
        """, (agent_name, issue_type, start_time, end_time, get_casablanca_time()))
        conn.commit()
        return True
    finally:
        conn.close()

def get_midshift_issues():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM midshift_issues ORDER BY timestamp DESC")
        return cursor.fetchall()
    except Exception as e:
        st.error(f"Error fetching mid-shift issues: {str(e)}")
    finally:
        conn.close()

def clear_late_logins():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM late_logins")
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error clearing late logins: {str(e)}")
    finally:
        conn.close()

def clear_quality_issues():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM quality_issues")
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error clearing quality issues: {str(e)}")
    finally:
        conn.close()

def clear_midshift_issues():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM midshift_issues")
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error clearing mid-shift issues: {str(e)}")
    finally:
        conn.close()

def get_dropdown_options(section):
    """Get dropdown options for a specific section"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT option_value FROM dropdown_options 
            WHERE section = ? 
            ORDER BY display_order, option_value
        """, (section,))
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

def add_dropdown_option(section, option_value):
    """Add a new dropdown option"""
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Get max display_order for this section
        cursor.execute("""
            SELECT MAX(display_order) FROM dropdown_options WHERE section = ?
        """, (section,))
        max_order = cursor.fetchone()[0]
        next_order = (max_order + 1) if max_order is not None else 0
        
        cursor.execute("""
            INSERT INTO dropdown_options (section, option_value, display_order) 
            VALUES (?, ?, ?)
        """, (section, option_value, next_order))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error adding dropdown option: {str(e)}")
        return False
    finally:
        conn.close()

def delete_dropdown_option(section, option_value):
    """Delete a dropdown option"""
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM dropdown_options 
            WHERE section = ? AND option_value = ?
        """, (section, option_value))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error deleting dropdown option: {str(e)}")
        return False
    finally:
        conn.close()

def get_all_dropdown_options_with_ids(section):
    """Get dropdown options with their IDs for a specific section"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, option_value, display_order FROM dropdown_options 
            WHERE section = ? 
            ORDER BY display_order, option_value
        """, (section,))
        return cursor.fetchall()
    finally:
        conn.close()

def send_vip_message(sender, message):
    """Send a message in the VIP-only chat"""
    if is_killswitch_enabled() or is_chat_killswitch_enabled():
        st.error("Chat is currently locked. Please contact the developer.")
        return False
    
    if not is_vip_user(sender) and sender.lower() != "taha kirri":
        st.error("Only VIP users can send messages in this chat.")
        return False
        
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        mentions = re.findall(r'@(\w+)', message)
        cursor.execute("""
            INSERT INTO vip_messages (sender, message, timestamp, mentions) 
            VALUES (?, ?, ?, ?)
        """, (sender, message, get_casablanca_time(), ','.join(mentions)))
        conn.commit()
        return True
    finally:
        conn.close()

def get_vip_messages():
    """Get messages from the VIP-only chat"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vip_messages ORDER BY timestamp DESC LIMIT 50")
        return cursor.fetchall()
    finally:
        conn.close()

# --------------------------
# Break Scheduling Functions (from first code)
# --------------------------

def init_break_session_state():
    if 'templates' not in st.session_state:
        st.session_state.templates = {}
    if 'current_template' not in st.session_state:
        st.session_state.current_template = None
    if 'agent_bookings' not in st.session_state:
        st.session_state.agent_bookings = {}
    if 'selected_date' not in st.session_state:
        st.session_state.selected_date = datetime.now().strftime('%Y-%m-%d')
    if 'timezone_offset' not in st.session_state:
        st.session_state.timezone_offset = 0  # GMT by default
    if 'break_limits' not in st.session_state:
        st.session_state.break_limits = {}
    if 'active_templates' not in st.session_state:
        st.session_state.active_templates = []
    
    # Load data from files if exists
    if os.path.exists('templates.json'):
        with open('templates.json', 'r') as f:
            st.session_state.templates = json.load(f)
    if os.path.exists('break_limits.json'):
        with open('break_limits.json', 'r') as f:
            st.session_state.break_limits = json.load(f)
    if os.path.exists('all_bookings.json'):
        with open('all_bookings.json', 'r') as f:
            st.session_state.agent_bookings = json.load(f)
    if os.path.exists('active_templates.json'):
        with open('active_templates.json', 'r') as f:
            st.session_state.active_templates = json.load(f)

def adjust_template_time(time_str, hours):
    """Adjust a single time string by adding/subtracting hours"""
    try:
        if not time_str.strip():
            return ""
        time_obj = datetime.strptime(time_str.strip(), "%H:%M")
        adjusted_time = (time_obj + timedelta(hours=hours)).time()
        return adjusted_time.strftime("%H:%M")
    except:
        return time_str

def bulk_update_template_times(hours):
    """Update all template times by adding/subtracting hours"""
    if 'templates' not in st.session_state:
        return False
    
    try:
        for template_name in st.session_state.templates:
            template = st.session_state.templates[template_name]
            
            # Update lunch breaks
            template["lunch_breaks"] = [
                adjust_template_time(t, hours) 
                for t in template["lunch_breaks"]
            ]
            
            # Update early tea breaks
            template["tea_breaks"]["early"] = [
                adjust_template_time(t, hours) 
                for t in template["tea_breaks"]["early"]
            ]
            
            # Update late tea breaks
            template["tea_breaks"]["late"] = [
                adjust_template_time(t, hours) 
                for t in template["tea_breaks"]["late"]
            ]
        
        save_break_data()
        return True
    except Exception as e:
        st.error(f"Error updating template times: {str(e)}")
        return False

def save_break_data():
    with open('templates.json', 'w') as f:
        json.dump(st.session_state.templates, f)
    with open('break_limits.json', 'w') as f:
        json.dump(st.session_state.break_limits, f)
    with open('all_bookings.json', 'w') as f:
        json.dump(st.session_state.agent_bookings, f)
    with open('active_templates.json', 'w') as f:
        json.dump(st.session_state.active_templates, f)

def adjust_time(time_str, offset):
    try:
        if not time_str.strip():
            return ""
        time_obj = datetime.strptime(time_str.strip(), "%H:%M")
        adjusted_time = (time_obj + timedelta(hours=offset)).time()
        return adjusted_time.strftime("%H:%M")
    except:
        return time_str

def adjust_template_times(template, offset):
    """Safely adjust template times with proper error handling"""
    try:
        if not template or not isinstance(template, dict):
            return {
                "lunch_breaks": [],
                "tea_breaks": {"early": [], "late": []}
            }
            
        adjusted_template = {
            "lunch_breaks": [adjust_time(t, offset) for t in template.get("lunch_breaks", [])],
            "tea_breaks": {
                "early": [adjust_time(t, offset) for t in template.get("tea_breaks", {}).get("early", [])],
                "late": [adjust_time(t, offset) for t in template.get("tea_breaks", {}).get("late", [])]
            }
        }
        return adjusted_template
    except Exception as e:
        st.error(f"Error adjusting template times: {str(e)}")
        return {
            "lunch_breaks": [],
            "tea_breaks": {"early": [], "late": []}
        }

def count_bookings(date, break_type, time_slot):
    count = 0
    if date in st.session_state.agent_bookings:
        for agent_id, breaks in st.session_state.agent_bookings[date].items():
            if break_type == "lunch" and "lunch" in breaks and isinstance(breaks["lunch"], dict) and breaks["lunch"].get("time") == time_slot:
                count += 1
            elif break_type == "early_tea" and "early_tea" in breaks and isinstance(breaks["early_tea"], dict) and breaks["early_tea"].get("time") == time_slot:
                count += 1
            elif break_type == "late_tea" and "late_tea" in breaks and isinstance(breaks["late_tea"], dict) and breaks["late_tea"].get("time") == time_slot:
                count += 1
    return count

def display_schedule(template):
    st.header("LM US ENG 3:00 PM shift")
    
    # Lunch breaks table
    st.markdown("### LUNCH BREAKS")
    lunch_df = pd.DataFrame({
        "DATE": [st.session_state.selected_date],
        **{time: [""] for time in template["lunch_breaks"]}
    })
    st.table(lunch_df)
    
    st.markdown("**KINDLY RESPECT THE RULES BELOW**")
    st.markdown("**Non Respect Of Break Rules = Incident**")
    st.markdown("---")
    
    # Tea breaks table
    st.markdown("### TEA BREAK")
    
    # Create two columns for tea breaks
    max_rows = max(len(template["tea_breaks"]["early"]), len(template["tea_breaks"]["late"]))
    tea_data = {
        "Early Tea Break": template["tea_breaks"]["early"] + [""] * (max_rows - len(template["tea_breaks"]["early"])),
        "Late Tea Break": template["tea_breaks"]["late"] + [""] * (max_rows - len(template["tea_breaks"]["late"]))
    }
    tea_df = pd.DataFrame(tea_data)
    st.table(tea_df)
    
    # Rules section
    st.markdown("""
    **NO BREAK IN THE LAST HOUR WILL BE AUTHORIZED**  
    **PS: ONLY 5 MINUTES BIO IS AUTHORIZED IN THE LAST HHOUR BETWEEN 23:00 TILL 23:30 AND NO BREAK AFTER 23:30 !!!**  
    **BREAKS SHOULD BE TAKEN AT THE NOTED TIME AND NEED TO BE CONFIRMED FROM RTA OR TEAM LEADERS**
    """)

def migrate_booking_data():
    if 'agent_bookings' in st.session_state:
        for date in st.session_state.agent_bookings:
            for agent in st.session_state.agent_bookings[date]:
                bookings = st.session_state.agent_bookings[date][agent]
                if "lunch" in bookings and isinstance(bookings["lunch"], str):
                    bookings["lunch"] = {
                        "time": bookings["lunch"],
                        "template": "Default Template",
                        "booked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                if "early_tea" in bookings and isinstance(bookings["early_tea"], str):
                    bookings["early_tea"] = {
                        "time": bookings["early_tea"],
                        "template": "Default Template",
                        "booked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                if "late_tea" in bookings and isinstance(bookings["late_tea"], str):
                    bookings["late_tea"] = {
                        "time": bookings["late_tea"],
                        "template": "Default Template",
                        "booked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
        
        save_break_data()

def clear_all_bookings():
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return False
    
    try:
        # Clear session state bookings
        st.session_state.agent_bookings = {}
        
        # Clear the bookings file
        if os.path.exists('all_bookings.json'):
            with open('all_bookings.json', 'w') as f:
                json.dump({}, f)
        
        # Save empty state to ensure it's propagated
        save_break_data()
        
        # Force session state refresh
        st.session_state.last_request_count = 0
        st.session_state.last_mistake_count = 0
        st.session_state.last_message_ids = []
        
        return True
    except Exception as e:
        st.error(f"Error clearing bookings: {str(e)}")
        return False

def admin_break_dashboard():
    st.title("Break Schedule Management")
    st.markdown("---")
    
    # Initialize templates if empty
    if 'templates' not in st.session_state:
        st.session_state.templates = {}
    
    # Create default template if no templates exist
    if not st.session_state.templates:
        default_template = {
            "lunch_breaks": ["19:30", "20:00", "20:30", "21:00", "21:30"],
            "tea_breaks": {
                "early": ["16:00", "16:15", "16:30", "16:45", "17:00", "17:15", "17:30"],
                "late": ["21:45", "22:00", "22:15", "22:30"]
            }
        }
        st.session_state.templates["Default Template"] = default_template
        st.session_state.current_template = "Default Template"
        if "Default Template" not in st.session_state.active_templates:
            st.session_state.active_templates.append("Default Template")
        save_break_data()
    
    # Template Activation Management
    # Inject CSS to fix white-on-white metric text
    st.markdown("""
    <style>
    /* Make st.metric values black and bold for visibility */
    div[data-testid="stMetricValue"] {
        color: black !important;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)
    st.subheader("🔄 Template Activation")
    st.info("Only activated templates will be available for agents to book breaks from.")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.write("### Active Templates")
        active_templates = st.session_state.active_templates
        template_list = list(st.session_state.templates.keys())
        
        for template in template_list:
            is_active = template in active_templates
            if st.checkbox(f"{template} {'✅' if is_active else ''}", 
                         value=is_active, 
                         key=f"active_{template}"):
                if template not in active_templates:
                    active_templates.append(template)
            else:
                if template in active_templates:
                    active_templates.remove(template)
        
        st.session_state.active_templates = active_templates
        save_break_data()
    
    with col2:
        st.write("### Statistics")
        st.metric("Total Templates", len(template_list))
        st.metric("Active Templates", len(active_templates))
    
    st.markdown("---")
    
    # Template Management
    st.subheader("Template Management")
    
    col1, col2 = st.columns(2)
    with col1:
        template_name = st.text_input("New Template Name:")
    with col2:
        if st.button("Create Template"):
            if template_name and template_name not in st.session_state.templates:
                st.session_state.templates[template_name] = {
                    "lunch_breaks": ["19:30", "20:00", "20:30", "21:00", "21:30"],
                    "tea_breaks": {
                        "early": ["16:00", "16:15", "16:30", "16:45", "17:00", "17:15", "17:30"],
                        "late": ["21:45", "22:00", "22:15", "22:30"]
                    }
                }
                save_break_data()
                st.success(f"Template '{template_name}' created!")
                st.rerun()
    
    # Template Selection and Editing
    selected_template = st.selectbox(
        "Select Template to Edit:",
        list(st.session_state.templates.keys())
    )
    
    if selected_template:
        template = st.session_state.templates[selected_template]
        
        # Time adjustment buttons
        st.subheader("Time Adjustment")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("➕ Add 1 Hour to All Times"):
                bulk_update_template_times(1)
                st.success("Added 1 hour to all break times")
                st.rerun()
        with col2:
            if st.button("➖ Subtract 1 Hour from All Times"):
                bulk_update_template_times(-1)
                st.success("Subtracted 1 hour from all break times")
                st.rerun()
        
        # Edit Lunch Breaks
        st.subheader("Edit Lunch Breaks")
        lunch_breaks = st.text_area(
            "Enter lunch break times (one per line):",
            "\n".join(template["lunch_breaks"]),
            height=150
        )
        
        # Edit Tea Breaks
        st.subheader("Edit Tea Breaks")
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("Early Tea Breaks")
            early_tea = st.text_area(
                "Enter early tea break times (one per line):",
                "\n".join(template["tea_breaks"]["early"]),
                height=200
            )
        
        with col2:
            st.write("Late Tea Breaks")
            late_tea = st.text_area(
                "Enter late tea break times (one per line):",
                "\n".join(template["tea_breaks"]["late"]),
                height=200
            )
        
        # Break Limits
        st.markdown("---")
        st.subheader("Break Limits")
        
        if selected_template not in st.session_state.break_limits:
            st.session_state.break_limits[selected_template] = {
                "lunch": {time: 5 for time in template["lunch_breaks"]},
                "early_tea": {time: 3 for time in template["tea_breaks"]["early"]},
                "late_tea": {time: 3 for time in template["tea_breaks"]["late"]}
            }
        
        limits = st.session_state.break_limits[selected_template]
        
        # Validate break times before rendering limits
        if not template["lunch_breaks"]:
            st.error("Please fill all the lunch break times before saving or editing limits.")
        else:
            st.write("Lunch Break Limits")
            cols = st.columns(len(template["lunch_breaks"]))
            for i, time in enumerate(template["lunch_breaks"]):
                with cols[i]:
                    limits["lunch"][time] = st.number_input(
                        f"Max at {time}",
                        min_value=1,
                        value=limits["lunch"].get(time, 5),
                        key=f"lunch_limit_{time}"
                    )
        
        if not template["tea_breaks"]["early"]:
            st.error("Please fill all the early tea break times before saving or editing limits.")
        else:
            st.write("Early Tea Break Limits")
            cols = st.columns(len(template["tea_breaks"]["early"]))
            for i, time in enumerate(template["tea_breaks"]["early"]):
                with cols[i]:
                    limits["early_tea"][time] = st.number_input(
                        f"Max at {time}",
                        min_value=1,
                        value=limits["early_tea"].get(time, 3),
                        key=f"early_tea_limit_{time}"
                    )
        
        if not template["tea_breaks"]["late"]:
            st.error("Please fill all the late tea break times before saving or editing limits.")
        else:
            st.write("Late Tea Break Limits")
            cols = st.columns(len(template["tea_breaks"]["late"]))
            for i, time in enumerate(template["tea_breaks"]["late"]):
                with cols[i]:
                    limits["late_tea"][time] = st.number_input(
                        f"Max at {time}",
                        min_value=1,
                        value=limits["late_tea"].get(time, 3),
                        key=f"late_tea_limit_{time}"
                    )
        
        # Consolidated save button
        if st.button("Save All Changes", type="primary"):
            template["lunch_breaks"] = [t.strip() for t in lunch_breaks.split("\n") if t.strip()]
            template["tea_breaks"]["early"] = [t.strip() for t in early_tea.split("\n") if t.strip()]
            template["tea_breaks"]["late"] = [t.strip() for t in late_tea.split("\n") if t.strip()]
            save_break_data()
            st.success("All changes saved successfully!")
            st.rerun()
        
        if st.button("Delete Template") and len(st.session_state.templates) > 1:
            del st.session_state.templates[selected_template]
            if selected_template in st.session_state.active_templates:
                st.session_state.active_templates.remove(selected_template)
            save_break_data()
            st.success(f"Template '{selected_template}' deleted!")
            st.rerun()
    
    # View Bookings with template information
    st.markdown("---")
    st.subheader("View All Bookings")
    
    dates = list(st.session_state.agent_bookings.keys())
    if dates:
        selected_date = st.selectbox("Select Date:", dates, index=len(dates)-1)
        
        # Add clear bookings button with proper confirmation
        if 'confirm_clear' not in st.session_state:
            st.session_state.confirm_clear = False
            
        col1, col2 = st.columns([1, 3])
        with col1:
            if not st.session_state.confirm_clear:
                if st.button("Clear All Bookings"):
                    st.session_state.confirm_clear = True
            
        if st.session_state.confirm_clear:
            st.warning("⚠️ Are you sure you want to clear all bookings? This cannot be undone!")
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("Yes, Clear All"):
                    if clear_all_bookings():
                        st.success("All bookings have been cleared!")
                        st.session_state.confirm_clear = False
                        st.rerun()
            with col2:
                if st.button("Cancel"):
                    st.session_state.confirm_clear = False
                    st.rerun()
        
        if selected_date in st.session_state.agent_bookings:
            bookings_data = []
            for agent, breaks in st.session_state.agent_bookings[selected_date].items():
                # Get template name from any break type (they should all be the same)
                template_name = None
                for break_type in ['lunch', 'early_tea', 'late_tea']:
                    if break_type in breaks and isinstance(breaks[break_type], dict):
                        template_name = breaks[break_type].get('template', 'Unknown')
                        break
                
                # Find a single 'booked_at' value for this agent's booking
                booked_at = None
                for btype in ['lunch', 'early_tea', 'late_tea']:
                    if btype in breaks and isinstance(breaks[btype], dict):
                        booked_at = breaks[btype].get('booked_at', None)
                        if booked_at:
                            break
                booking = {
                    "Agent": agent,
                    "Template": template_name or "Unknown",
                    "Lunch": breaks.get("lunch", {}).get("time", "-") if isinstance(breaks.get("lunch"), dict) else breaks.get("lunch", "-"),
                    "Early Tea": breaks.get("early_tea", {}).get("time", "-") if isinstance(breaks.get("early_tea"), dict) else breaks.get("early_tea", "-"),
                    "Late Tea": breaks.get("late_tea", {}).get("time", "-") if isinstance(breaks.get("late_tea"), dict) else breaks.get("late_tea", "-"),
                    "Booked At": booked_at or "-"
                }
                bookings_data.append(booking)
            
            if bookings_data:
                df = pd.DataFrame(bookings_data)
                st.dataframe(df)
                
                # Export option
                if st.button("Export to CSV"):
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Download CSV",
                        csv,
                        f"break_bookings_{selected_date}.csv",
                        "text/csv"
                    )
            else:
                st.info("No bookings found for this date")
    else:
        st.info("No bookings available")

def time_to_minutes(time_str):
    """Convert time string (HH:MM) to minutes since midnight"""
    try:
        hours, minutes = map(int, time_str.split(':'))
        return hours * 60 + minutes
    except:
        return None

def times_overlap(time1, time2, duration_minutes=15):
    """Check if two time slots overlap, assuming each break is duration_minutes long"""
    t1 = time_to_minutes(time1)
    t2 = time_to_minutes(time2)
    
    if t1 is None or t2 is None:
        return False
        
    # Check if the breaks overlap
    return abs(t1 - t2) < duration_minutes

def check_break_conflicts(selected_breaks):
    """Check for conflicts between selected breaks"""
    times = []
    
    # Collect all selected break times
    if selected_breaks.get("lunch"):
        times.append(("lunch", selected_breaks["lunch"]))
    if selected_breaks.get("early_tea"):
        times.append(("early_tea", selected_breaks["early_tea"]))
    if selected_breaks.get("late_tea"):
        times.append(("late_tea", selected_breaks["late_tea"]))
    
    # Check each pair of breaks for overlap
    for i in range(len(times)):
        for j in range(i + 1, len(times)):
            break1_type, break1_time = times[i]
            break2_type, break2_time = times[j]
            
            if times_overlap(break1_time, break2_time, 30 if "lunch" in (break1_type, break2_type) else 15):
                return f"Conflict detected between {break1_type.replace('_', ' ')} ({break1_time}) and {break2_type.replace('_', ' ')} ({break2_time})"
    
    return None

def refresh_break_data():
    """Refresh break data from the database files"""
    try:
        if os.path.exists('all_bookings.json'):
            with open('all_bookings.json', 'r') as f:
                # Only update if the file has content
                content = f.read()
                if content.strip():
                    st.session_state.agent_bookings = json.loads(content)
    except Exception as e:
        st.error(f"Error refreshing break data: {str(e)}")

def inject_break_notifications(bookings, agent_id, current_date, server_time_iso):
    """Client-side break reminders 5 minutes before a scheduled break."""
    import streamlit.components.v1 as components
    import json
    break_times = []
    break_info = {}
    for break_type in ['lunch', 'early_tea', 'late_tea']:
        if break_type in bookings and isinstance(bookings[break_type], dict) and 'time' in bookings[break_type]:
            t = bookings[break_type]['time']
            break_times.append(t)
            break_info[t] = {
                'type': break_type,
                'template': bookings[break_type].get('template', '')
            }

    if 'break_info' not in st.session_state:
        st.session_state.break_info = break_info

    js_code = f'''
    <script>
    const breakTimes = {json.dumps(break_times)};
    const breakInfo = {json.dumps(break_info)};
    const serverTimeISO = "{server_time_iso}";
    const notificationKeyPrefix = 'notified_break_';
    const agentId = "{agent_id}";
    const currentDate = "{current_date}";

    function saveNotificationState(time, notified) {{
        const key = `${{notificationKeyPrefix}}${{currentDate}}_${{agentId}}_${{time}}`;
        if (notified) {{
            sessionStorage.setItem(key, 'true');
        }} else {{
            sessionStorage.removeItem(key);
        }}
    }}

    function wasNotified(time) {{
        const key = `${{notificationKeyPrefix}}${{currentDate}}_${{agentId}}_${{time}}`;
        return sessionStorage.getItem(key) === 'true';
    }}

    function checkAndNotifyBreaks() {{
        const now = new Date(serverTimeISO);
        const today = now.toISOString().split('T')[0];

        breakTimes.forEach(time => {{
            const [hours, minutes] = time.split(':');
            const breakTime = new Date(now.getFullYear(), now.getMonth(), now.getDate(),
                                     parseInt(hours), parseInt(minutes), 0);

            const diff = breakTime.getTime() - now.getTime();
            const minutesUntilBreak = Math.floor(diff / (1000 * 60));

            if (minutesUntilBreak >= 0 && minutesUntilBreak <= 5 && !wasNotified(time)) {{
                const breakType = breakInfo[time] ? breakInfo[time].type : 'break';
                let breakDisplayName = 'Break';
                if (breakType === 'lunch') {{
                    breakDisplayName = 'Lunch Break';
                }} else if (breakType === 'early_tea') {{
                    breakDisplayName = 'Early Tea Break';
                }} else if (breakType === 'late_tea') {{
                    breakDisplayName = 'Late Tea Break';
                }}

                if (Notification.permission === 'granted') {{
                    new Notification(breakDisplayName + ' Reminder', {{
                        body: 'Your ' + breakDisplayName + ' starts in ' + minutesUntilBreak + ' minutes at ' + time + '.',
                        icon: 'https://www.lycamobile.ma/wp-content/uploads/2020/10/favicon.png'
                    }});
                    saveNotificationState(time, true);
                }} else if (Notification.permission !== 'denied') {{
                    Notification.requestPermission().then(perm => {{
                        if (perm === 'granted') {{
                            new Notification(breakDisplayName + ' Reminder', {{
                                body: 'Your ' + breakDisplayName + ' starts in ' + minutesUntilBreak + ' minutes at ' + time + '.',
                                icon: 'https://www.lycamobile.ma/wp-content/uploads/2020/10/favicon.png'
                            }});
                            saveNotificationState(time, true);
                        }}
                    }});
                }} else {{
                    console.log('Break reminder: ' + breakDisplayName + ' at ' + time + ' (in ' + minutesUntilBreak + ' minutes)');
                }}
            }}
        }});
    }}

    if (!window.breakNotificationInterval) {{
        checkAndNotifyBreaks();
        window.breakNotificationInterval = setInterval(() => {{
            checkAndNotifyBreaks();
        }}, 60000);
    }}
    </script>
    '''
    components.html(js_code, height=0)

def agent_break_dashboard():
    # Initialize session state if not exists
    if 'selected_template_name' not in st.session_state:
        st.session_state.selected_template_name = None
    if 'temp_bookings' not in st.session_state:
        st.session_state.temp_bookings = {}
    if 'booking_confirmed' not in st.session_state:
        st.session_state.booking_confirmed = False
    if 'agent_bookings' not in st.session_state:
        st.session_state.agent_bookings = {}
    
    # Always refresh data from database at the start
    refresh_break_data()
    
    
    # If no template selected, we will later restrict the choice to the
    # agent's assigned templates (from users.break_templates). First,
    # check if they already have bookings for today and auto-select that
    # template when present.
    if st.session_state.selected_template_name is None:
        agent_id_lookup = get_agent_id_for_username(st.session_state.username)
        current_date_lookup = datetime.now().strftime('%Y-%m-%d')

        has_existing_booking = (
            current_date_lookup in st.session_state.agent_bookings and
            agent_id_lookup in st.session_state.agent_bookings[current_date_lookup]
        )

        if has_existing_booking:
            st.session_state.booking_confirmed = True
            st.session_state.selected_template_name = next(
                (b.get('template', 'Default Template')
                 for b in st.session_state.agent_bookings[current_date_lookup][agent_id_lookup].values()
                 if isinstance(b, dict) and 'template' in b),
                'Default Template'
            )
            st.rerun()
    agent_id = st.session_state.username
    morocco_tz = pytz.timezone('Africa/Casablanca')
    now_casa = datetime.now(morocco_tz)
    server_time_iso = now_casa.isoformat()
    casa_date = now_casa.strftime('%Y-%m-%d')
    current_date = casa_date  # Use Casablanca date for all booking logic

    # Only apply auto-clear for agents (not admin/qa)
    user_role = st.session_state.get('role', 'agent')
    if user_role == 'agent':
        # Track last clear per agent
        if 'last_booking_clear_per_agent' not in st.session_state:
            st.session_state.last_booking_clear_per_agent = {}
        last_clear = st.session_state.last_booking_clear_per_agent.get(agent_id)
        # Clear after 05:00 AM (Casablanca time) each day
        if (now_casa.hour > 5 or (now_casa.hour == 5 and now_casa.minute >= 0)):
            if last_clear != casa_date:
                # Clear only this agent's bookings for today
                if current_date in st.session_state.agent_bookings:
                    st.session_state.agent_bookings[current_date].pop(agent_id, None)
                st.session_state.last_booking_clear_per_agent[agent_id] = casa_date
                save_break_data()

    # Check if agent already has confirmed bookings
    has_confirmed_bookings = (
        current_date in st.session_state.agent_bookings and 
        agent_id in st.session_state.agent_bookings[current_date]
    )
    
    if has_confirmed_bookings:
        st.success("Your breaks have been confirmed for today")
        st.subheader("Your Confirmed Breaks")
        bookings = st.session_state.agent_bookings[current_date][agent_id]
        template_name = None
        for break_type in ['lunch', 'early_tea', 'late_tea']:
            if break_type in bookings and isinstance(bookings[break_type], dict):
                template_name = bookings[break_type].get('template')
                break
        
        if template_name:
            st.info(f"Template: **{template_name}**")
        
        # Find a single 'booked_at' value to display (first found among breaks)
        booked_at = None
        for break_type in ['lunch', 'early_tea', 'late_tea']:
            if break_type in bookings and isinstance(bookings[break_type], dict):
                booked_at = bookings[break_type].get('booked_at', None)
                if booked_at:
                    break
        if booked_at:
            st.caption(f"Booked at: {booked_at}")
        for break_type, display_name in [
            ("lunch", "Lunch Break"),
            ("early_tea", "Early Tea Break"),
            ("late_tea", "Late Tea Break")
        ]:
            if break_type in bookings:
                if isinstance(bookings[break_type], dict):
                    st.write(f"**{display_name}:** {bookings[break_type]['time']}")
                else:
                    st.write(f"**{display_name}:** {bookings[break_type]}")

        if st.session_state.get("notification_settings", {}).get("break_notifications", True):
            inject_break_notifications(bookings, agent_id, current_date, server_time_iso)
        return
    
    # Determine agent's assigned templates
    agent_templates = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Defensive: Check if break_templates column exists
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        if "break_templates" in columns:
            cursor.execute("SELECT break_templates FROM users WHERE username = ?", (agent_id,))
            row = cursor.fetchone()
            if row and row[0]:
                agent_templates = [t.strip() for t in row[0].split(',') if t.strip()]
    except Exception:
        agent_templates = []
    finally:
        try:
            conn.close()
        except:
            pass

    # Step 1: Template Selection
    if not st.session_state.selected_template_name:
        st.subheader("Step 1: Select Break Schedule")
        # Only show templates the agent is assigned to
        available_templates = [t for t in st.session_state.active_templates if t in agent_templates] if agent_templates else []
        if not available_templates or not agent_templates:
            st.error("You are not assigned to any break schedule. Please contact your administrator.")
            return  # Absolutely enforce early return
        if len(available_templates) == 1:
            # Only one template, auto-select
            st.session_state.selected_template_name = available_templates[0]
            st.rerun()
        else:
            selected_template = st.selectbox(
                "Choose your break schedule:",
                available_templates,
                index=None,
                placeholder="Select a template..."
            )
            if selected_template:
                if st.button("Continue to Break Selection"):
                    st.session_state.selected_template_name = selected_template
                    st.rerun()
            return  # Absolutely enforce early return

    
    # Step 2: Break Selection
    if st.session_state.selected_template_name not in st.session_state.templates:
        st.error("Your assigned break schedule is not available. Please contact your administrator.")
        return
    template = st.session_state.templates[st.session_state.selected_template_name]
    
    st.subheader("Step 2: Select Your Breaks")
    st.info(f"Selected Template: **{st.session_state.selected_template_name}**")
    
    if st.button("Change Template"):
        st.session_state.selected_template_name = None
        st.session_state.temp_bookings = {}
        st.rerun()
    
    # Break selection
    with st.form("break_selection_form"):
        st.write("**Lunch Break** (30 minutes)")
        lunch_options = []
        for slot in template["lunch_breaks"]:
            count = count_bookings(current_date, "lunch", slot)
            limit = st.session_state.break_limits.get(st.session_state.selected_template_name, {}).get("lunch", {}).get(slot, 5)
            available = max(0, limit - count)
            label = f"{slot} ({available} free to book)"
            lunch_options.append((label, slot))
        lunch_labels = ["No selection"] + [label for label, _ in lunch_options]
        lunch_values = [""] + [value for _, value in lunch_options]
        lunch_time = st.selectbox(
            "Select Lunch Break",
            lunch_labels,
            format_func=lambda x: x,
            index=0 if not lunch_labels else None
        )
        # Map label back to value
        lunch_time = lunch_values[lunch_labels.index(lunch_time)] if lunch_time in lunch_labels else ""

        
        st.write("**Early Tea Break** (15 minutes)")
        early_tea_options = []
        for slot in template["tea_breaks"]["early"]:
            count = count_bookings(current_date, "early_tea", slot)
            limit = st.session_state.break_limits.get(st.session_state.selected_template_name, {}).get("early_tea", {}).get(slot, 3)
            available = max(0, limit - count)
            label = f"{slot} ({available} free to book)"
            early_tea_options.append((label, slot))
        early_tea_labels = ["No selection"] + [label for label, _ in early_tea_options]
        early_tea_values = [""] + [value for _, value in early_tea_options]
        early_tea = st.selectbox(
            "Select Early Tea Break",
            early_tea_labels,
            format_func=lambda x: x,
            index=0 if not early_tea_labels else None
        )
        early_tea = early_tea_values[early_tea_labels.index(early_tea)] if early_tea in early_tea_labels else ""

        
        st.write("**Late Tea Break** (15 minutes)")
        late_tea_options = []
        for slot in template["tea_breaks"]["late"]:
            count = count_bookings(current_date, "late_tea", slot)
            limit = st.session_state.break_limits.get(st.session_state.selected_template_name, {}).get("late_tea", {}).get(slot, 3)
            available = max(0, limit - count)
            label = f"{slot} ({available} free to book)"
            late_tea_options.append((label, slot))
        late_tea_labels = ["No selection"] + [label for label, _ in late_tea_options]
        late_tea_values = [""] + [value for _, value in late_tea_options]
        late_tea = st.selectbox(
            "Select Late Tea Break",
            late_tea_labels,
            format_func=lambda x: x,
            index=0 if not late_tea_labels else None
        )
        late_tea = late_tea_values[late_tea_labels.index(late_tea)] if late_tea in late_tea_labels else ""

        
        # Validate and confirm
        if st.form_submit_button("Confirm Breaks"):
            if not (lunch_time and early_tea and late_tea):
                st.error("Please select all three breaks before confirming.")
                return
            
            # Check for time conflicts
            selected_breaks = {
                "lunch": lunch_time if lunch_time else None,
                "early_tea": early_tea if early_tea else None,
                "late_tea": late_tea if late_tea else None
            }
            
            conflict = check_break_conflicts(selected_breaks)
            if conflict:
                st.error(conflict)
                return
            
            # Check limits for each selected break
            can_book = True
            if lunch_time:
                count = sum(1 for bookings in st.session_state.agent_bookings.get(current_date, {}).values()
                           if isinstance(bookings.get("lunch"), dict) and bookings["lunch"]["time"] == lunch_time)
                limit = st.session_state.break_limits.get(st.session_state.selected_template_name, {}).get("lunch", {}).get(lunch_time, 5)
                if count >= limit:
                    st.error(f"Lunch break at {lunch_time} is full.")
                    can_book = False
            
            if early_tea:
                count = sum(1 for bookings in st.session_state.agent_bookings.get(current_date, {}).values()
                           if isinstance(bookings.get("early_tea"), dict) and bookings["early_tea"]["time"] == early_tea)
                limit = st.session_state.break_limits.get(st.session_state.selected_template_name, {}).get("early_tea", {}).get(early_tea, 3)
                if count >= limit:
                    st.error(f"Early tea break at {early_tea} is full.")
                    can_book = False
            
            if late_tea:
                count = sum(1 for bookings in st.session_state.agent_bookings.get(current_date, {}).values()
                           if isinstance(bookings.get("late_tea"), dict) and bookings["late_tea"]["time"] == late_tea)
                limit = st.session_state.break_limits.get(st.session_state.selected_template_name, {}).get("late_tea", {}).get(late_tea, 3)
                if count >= limit:
                    st.error(f"Late tea break at {late_tea} is full.")
                    can_book = False
            
            if can_book:
                # Save the bookings
                if current_date not in st.session_state.agent_bookings:
                    st.session_state.agent_bookings[current_date] = {}
                
                bookings = {}
                if lunch_time:
                    bookings["lunch"] = {
                        "time": lunch_time,
                        "template": st.session_state.selected_template_name,
                        "booked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                if early_tea:
                    bookings["early_tea"] = {
                        "time": early_tea,
                        "template": st.session_state.selected_template_name,
                        "booked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                if late_tea:
                    bookings["late_tea"] = {
                        "time": late_tea,
                        "template": st.session_state.selected_template_name,
                        "booked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                
                st.session_state.agent_bookings[current_date][agent_id] = bookings
                if save_break_data():
                    st.success("Your breaks have been confirmed!")
                    # Force a rerun to ensure state is consistent
                    st.rerun()
                else:
                    st.error("Click Again to Confirm.")

    # All swap / roster / WFM management UIs are intentionally handled by
    # the dedicated WFM dashboards (wfm_agent_dashboard / wfm_admin_dashboard)
    # and are not shown in the simple Breaks page.

# Full Admin WFM Dashboard UI
def wfm_admin_dashboard():
    st.title("👥 Workforce Management (Admin)")
    if is_killswitch_enabled():
        st.error("System is currently locked. Please contact the developer.")
        return

    # Upload Roster
    st.markdown("---")
    with st.expander("📤 Upload Roster", expanded=False):
        st.write("**Upload CSV/Excel file with agent roster data**")
        st.write("**Required columns:** A (Agent ID), C (Name)")
        st.write("**Optional columns:** B (Process)")
        st.info("For shift swapping, agents must have the same process value (column B). Date columns are D..Q.")

        uploaded_file = st.file_uploader(
            "Choose CSV or Excel file",
            type=['csv', 'xlsx', 'xls'],
            key="wfm_roster_upload_admin"
        )
        if uploaded_file is not None:
            try:
                roster_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                st.write("Preview of uploaded data:")
                st.dataframe(roster_df.head())
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Upload Current Roster", key="wfm_upload_current_admin"):
                        if upload_roster(roster_df, target='current'):
                            st.success("Current roster uploaded successfully!")
                            st.rerun()
                with col2:
                    if st.button("Upload Next Roster", key="wfm_upload_next_admin"):
                        if upload_roster(roster_df, target='next'):
                            st.success("Next roster uploaded successfully!")
                            st.rerun()
            except Exception as e:
                st.error(f"Error processing file: {e}")

    # Current Roster
    st.subheader("Current Roster")
    current_roster_df = None
    roster_data = get_roster()
    show_cols = ['Agent_ID', 'Name', 'Process', 'Department'] + build_14_day_columns()
    if roster_data:
        df = pd.DataFrame(roster_data, columns=['ID','Agent_ID','Name','Department','Shift','Schedule','Process','Upload Date','Username'])
        df = expand_schedule_columns(df)
        st.dataframe(df[show_cols], use_container_width=True)
        current_roster_df = df
    else:
        st.info("No current roster data available")

    # Next Roster
    st.subheader("Next Roster")
    roster_next = get_roster_by_table('roster_next')
    if roster_next:
        df_next = pd.DataFrame(roster_next, columns=['ID','Agent_ID','Name','Department','Shift','Schedule','Process','Upload Date','Username'])
        df_next = expand_schedule_columns(df_next)
        st.dataframe(df_next[show_cols], use_container_width=True)
    else:
        st.info("No next roster data available")

    # Search by name
    st.markdown("---")
    search_name = st.text_input("Search agent roster by name", key="wfm_roster_search_admin")
    if search_name and current_roster_df is not None:
        filtered = current_roster_df[current_roster_df['Name'].str.contains(search_name, case=False, na=False)]
        if not filtered.empty:
            st.subheader(f"Search results for '{search_name}'")
            st.dataframe(filtered[show_cols], use_container_width=True)
            options = []
            for _, row in filtered[['Name','Agent_ID']].drop_duplicates().iterrows():
                label = f"{row['Name']} ({row['Agent_ID']})"
                options.append((label, row['Agent_ID']))
            mapping = {label: aid for label, aid in options}
            selected_label = st.selectbox("Select agent for swap history", list(mapping.keys()), key="wfm_search_select_admin")
            selected_agent_id = mapping[selected_label]
            history = []
            for status in ['pending','approved','rejected']:
                for swap in get_swap_requests(status):
                    if swap[1] == selected_agent_id or swap[2] == selected_agent_id:
                        history.append({
                            'Date': swap[3],
                            'Status': (swap[6] or '').capitalize(),
                            'Reason': swap[7] or '',
                            'Requester': swap[11] or swap[1],
                            'Target': swap[12] or swap[2],
                            'Requested': swap[8] or '',
                            'Approved by': swap[9] or '',
                            'Approved at': swap[10] or ''
                        })
            if history:
                st.subheader(f"Swap history for {selected_label}")
                st.dataframe(pd.DataFrame(history), use_container_width=True)
            else:
                st.info(f"No swap history found for {selected_label}")
        else:
            st.info("No agents match that name")

    # Swap Requests Management
    st.markdown("---")
    st.subheader("🔄 Swap Requests Management")
    tab1, tab2, tab3 = st.tabs(["Pending Admin Approval","Approved","Rejected"])
    with tab1:
        # In the admin view, we only show swaps that are already accepted by the target
        # agent and are now waiting for admin action.
        pending_swaps = get_swap_requests('pending_admin')
        if pending_swaps:
            for swap in pending_swaps:
                req = get_agent_by_id(swap[1]); tgt = get_agent_by_id(swap[2])
                req_name = (req and (req[2] or req[0])) or (swap[11] or "Unknown")
                tgt_name = (tgt and (tgt[2] or tgt[0])) or (swap[12] or "Unknown")
                with st.expander(f"{req_name} ↔ {tgt_name} - {swap[3]}"):
                    st.write(f"**Requester:** {req_name}")
                    st.write(f"**Target:** {tgt_name}")
                    st.write(f"**Date:** {swap[3]}")
                    st.write(f"**Reason:** {swap[7]}")
                    st.write(f"**Requested:** {swap[8]}")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("✅ Approve", key=f"admin_approve_{swap[0]}"):
                            if approve_swap_request(swap[0], st.session_state.username):
                                st.success("Swap request approved!")
                                st.rerun()
                    with col2:
                        if st.button("❌ Reject", key=f"admin_reject_{swap[0]}"):
                            if reject_swap_request(swap[0], st.session_state.username):
                                st.success("Swap request rejected!")
                                st.rerun()
        else:
            st.info("No swap requests are currently waiting for admin approval")
    with tab2:
        approved_swaps = get_swap_requests('approved')
        if approved_swaps:
            for swap in approved_swaps:
                req_name = swap[11] or swap[1]; tgt_name = swap[12] or swap[2]
                with st.expander(f"{req_name} ↔ {tgt_name} - {swap[3]}"):
                    st.write(f"**Requester:** {req_name}")
                    st.write(f"**Target:** {tgt_name}")
                    st.write(f"**Date:** {swap[3]}")
                    st.write(f"**Reason:** {swap[7]}")
                    st.write(f"**Approved by:** {swap[9]}")
                    st.write(f"**Approved at:** {swap[10]}")
        else:
            st.info("No approved swap requests")
    with tab3:
        rejected_swaps = get_swap_requests('rejected')
        if rejected_swaps:
            for swap in rejected_swaps:
                req_name = swap[11] or swap[1]; tgt_name = swap[12] or swap[2]
                with st.expander(f"{req_name} ↔ {tgt_name} - {swap[3]}"):
                    st.write(f"**Requester:** {req_name}")
                    st.write(f"**Target:** {tgt_name}")
                    st.write(f"**Date:** {swap[3]}")
                    st.write(f"**Reason:** {swap[7]}")
                    st.write(f"**Rejected by:** {swap[9]}")
                    st.write(f"**Rejected at:** {swap[10]}")
        else:
            st.info("No rejected swap requests")

def wfm_agent_dashboard():
    """WFM Dashboard for Agents"""
    st.title("👥 Workforce Management")
    
    # Get current agent's info
    current_agent_id = None
    current_agent_dept = None
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT agent_id FROM users WHERE username = ?", (st.session_state.username,))
        result = cursor.fetchone()
        current_agent_id = result[0] if result else None
        
        if current_agent_id:
            agent_info = get_agent_by_id(current_agent_id)
            if agent_info:
                current_agent_dept = agent_info[3]  # department
    finally:
        conn.close()
    
    if current_agent_id:
        # Show current agent's roster info
        agent_info = get_agent_by_id(current_agent_id)
        if agent_info:
            st.info(f"**Current Schedule:** {agent_info[4] if agent_info[4] else 'Not assigned'} | **Shift:** {agent_info[3] if agent_info[3] else 'Not assigned'}")
        
        # View roster for same department
        st.markdown("---")
        st.subheader("📋 Department Roster")
        
        if current_agent_dept:
            all_agents = get_roster()  # current roster rows
            next_agents = get_roster_by_table('roster_next') or []  # next roster rows
            # Normalize current agent dept/process key
            my_key = str(current_agent_dept or '').strip().lower()
            # Start with current dept agents (case-insensitive, fallback to process column if needed)
            def match_dept(row):
                dept = str(row[3] or '').strip().lower()
                proc = str(row[6] or '').strip().lower() if len(row) > 6 else ''
                key = dept or proc
                return key == my_key
            dept_agents = [agent for agent in all_agents if match_dept(agent)]
            # Index by Agent_ID for quick union
            present_ids = set([a[1] for a in dept_agents])
            # Add next-only dept agents not present in current
            for n in next_agents:
                try:
                    if match_dept(n) and n[1] not in present_ids:
                        dept_agents.append(n)
                        present_ids.add(n[1])
                except Exception:
                    continue
            # Build a map of next schedules by Agent_ID
            next_sched_map = {}
            for row in next_agents:
                try:
                    aid = row[1]
                    sched = json.loads(row[5]) if row[5] else {}
                    next_sched_map[aid] = sched
                except Exception:
                    continue
            
            if dept_agents:
                dept_df = pd.DataFrame(
                    dept_agents,
                    columns=['ID', 'Agent_ID', 'Name', 'Department', 'Shift', 'Schedule', 'Process', 'Upload Date', 'Username']
                )
                # Merge current Schedule with next Schedule (next overrides) so upcoming dates (e.g., Dec 1) appear
                if 'Schedule' in dept_df.columns:
                    for idx, row in dept_df.iterrows():
                        try:
                            cur_js = json.loads(row['Schedule']) if row['Schedule'] else {}
                        except Exception:
                            cur_js = {}
                        nxt_js = next_sched_map.get(row['Agent_ID'], {})
                        if nxt_js:
                            merged = dict(cur_js)
                            merged.update(nxt_js)
                            dept_df.at[idx, 'Schedule'] = json.dumps(merged, ensure_ascii=False)
                dept_df = expand_schedule_columns(dept_df)
                cols = ['Agent_ID', 'Name', 'Department', 'Process'] + build_14_day_columns()
                st.dataframe(dept_df[cols], use_container_width=True)
            else:
                st.info(f"No agents found in your department: {current_agent_dept}")
        else:
            st.warning("Your department is not assigned in the roster")
        
        # Swap Request Section
        st.markdown("---")
        st.subheader("🔄 Shift Swap Requests")
        
        # Create new swap request
        with st.expander("📝 Create Swap Request", expanded=False):
            st.write("Request to swap your shift with another agent in your department")
            
            # Get all agents for selection
            all_agents = get_roster()
            if all_agents:
                # Filter to same department/process as current agent
                def same_dept_proc(a):
                    adept = str(a[3] or '').strip().lower()
                    aproc = str(a[6] or '').strip().lower() if len(a) > 6 else ''
                    key_a = adept or aproc
                    my_key = str(current_agent_dept or '').strip().lower()
                    return key_a == my_key
                filtered = [a for a in all_agents if a[1] != current_agent_id and same_dept_proc(a)]
                # Display Name, value Agent_ID
                agent_options = [(agent[2], agent[1]) for agent in filtered]
                agent_names = [name for name, _ in agent_options]
                agent_ids = [aid for _, aid in agent_options]
                
                selected_agent_name = st.selectbox("Select agent to swap with:", [""] + agent_names)
                
                if selected_agent_name:
                    selected_agent_id = agent_ids[agent_names.index(selected_agent_name)]
                    
                    col_a, col_b = st.columns(2)
                    with col_a:
                        my_give_date = st.date_input("I will give (my date)", value=datetime.now().date(), key="swap_my_give_old")
                    with col_b:
                        their_take_date = st.date_input("I will take (their date)", value=datetime.now().date(), key="swap_their_take_old")
                    reason = st.text_area("Reason for swap request")
                    
                    if st.button("Submit Swap Request"):
                        if reason.strip():
                            try:
                                ok = create_swap_request(
                                    current_agent_id,
                                    selected_agent_id,
                                    (their_take_date.strftime('%Y-%m-%d') if hasattr(their_take_date, 'strftime') else str(their_take_date)),
                                    reason.strip(),
                                    requester_date=(my_give_date.strftime('%Y-%m-%d') if hasattr(my_give_date, 'strftime') else str(my_give_date)),
                                    target_date=(their_take_date.strftime('%Y-%m-%d') if hasattr(their_take_date, 'strftime') else str(their_take_date))
                                )
                                if ok:
                                    st.success("Swap request submitted successfully!")
                                    st.rerun()
                                else:
                                    # create_swap_request already showed a specific error
                                    st.warning("Swap request could not be created. Please review the errors above.")
                            except Exception as e:
                                st.error(f"Failed to submit swap request: {str(e)}")
                        else:
                            st.error("Please provide a reason for the swap request")
            else:
                st.warning("No agents available in the roster")
        
        # Incoming requests for me (approve/reject as target)
        st.subheader("Incoming Swap Requests")
        # Only show requests that are waiting for the target agent decision
        incoming = [s for s in get_swap_requests('pending_agent') if s[2] == current_agent_id]
        if incoming:
            for swap in incoming:
                requester_name = swap[11] or swap[1]
                with st.expander(f"From {requester_name} - Give: {swap[4] or swap[3]} | Take: {swap[5] or swap[3]}"):
                    st.write(f"**Requester:** {requester_name}")
                    st.write(f"**Their date (you take):** {swap[5] or swap[3]}")
                    st.write(f"**Your date (you give):** {swap[4] or swap[3]}")
                    st.write(f"**Reason:** {swap[7] or ''}")
                    cols = st.columns(2)
                    with cols[0]:
                        if st.button("✅ Accept", key=f"agent_accept_{swap[0]}"):
                            if respond_to_swap_request_as_agent(swap[0], current_agent_id, True):
                                st.success("You accepted this swap. It is now waiting for admin approval.")
                                st.rerun()
                    with cols[1]:
                        if st.button("❌ Reject", key=f"agent_reject_{swap[0]}"):
                            if respond_to_swap_request_as_agent(swap[0], current_agent_id, False):
                                st.success("You rejected this swap request.")
                                st.rerun()
        else:
            st.info("No incoming requests")

        # Show agent's swap requests
        st.subheader("Your Swap Requests")
        # Show all swaps (any status) where you are requester or target
        my_swaps = []
        for status in ['pending_agent', 'pending_admin', 'approved', 'rejected']:
            my_swaps.extend(get_swap_requests(status))
        my_swaps = [s for s in my_swaps if s[1] == current_agent_id or s[2] == current_agent_id]
        
        if my_swaps:
            for swap in my_swaps:
                requester_name = swap[11] or swap[1]
                target_name = swap[12] or swap[2]
                label = target_name if swap[1] == current_agent_id else requester_name
                with st.expander(f"Swap with {label} - {swap[3]} ({(swap[6] or '').upper()})"):
                    if swap[1] == current_agent_id:
                        st.write(f"**You requested** to swap with **{target_name}**")
                    else:
                        st.write(f"**{requester_name}** requested to swap with **you**")
                    st.write(f"**Give (your date):** {swap[4] or swap[3]}")
                    st.write(f"**Take (their date):** {swap[5] or swap[3]}")
                    st.write(f"**Reason:** {swap[7] or ''}")
                    st.write(f"**Status:** {(swap[6] or '').upper()}")
                    st.write(f"**Requested:** {swap[8] or ''}")
                    if swap[9]:
                        st.write(f"**Approved by:** {swap[9]}")
                        st.write(f"**Approved at:** {swap[10]}")
                    
                    # Allow cancellation of own agent-pending requests
                    if swap[1] == current_agent_id and (swap[6] or '') == 'pending_agent':
                        if st.button(f"Cancel Request", key=f"wfm_cancel_{swap[0]}"):
                            try:
                                if reject_swap_request(swap[0], st.session_state.username):
                                    st.success("Request cancelled")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Failed to cancel: {e}")
        else:
            st.info("You have no swap requests yet")
    else:
        st.warning("You don't have an agent ID assigned. Please contact your administrator to be added to the roster system.")

def is_vip_user(username):
    """Check if a user has VIP status"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT is_vip FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
        return bool(result[0]) if result else False
    finally:
        conn.close()

def is_sequential(digits, step=1):
    """Check if digits form a sequential pattern with given step"""
    try:
        return all(int(digits[i]) == int(digits[i-1]) + step for i in range(1, len(digits)))
    except:
        return False

def is_fancy_number(phone_number):
    """Check if a phone number has a fancy pattern"""
    clean_number = re.sub(r'\D', '', phone_number)
    
    # Get last 6 digits according to Lycamobile policy
    if len(clean_number) >= 6:
        last_six = clean_number[-6:]
        last_three = clean_number[-3:]
    else:
        return False, "Number too short (need at least 6 digits)"
    
    patterns = []
    
    # Special case for 13322866688
    if clean_number == "13322866688":
        patterns.append("Special VIP number (13322866688)")
    
    # Check for ABBBAA pattern (like 566655)
    if (len(last_six) == 6 and 
        last_six[0] == last_six[5] and 
        last_six[1] == last_six[2] == last_six[3] and 
        last_six[4] == last_six[0] and 
        last_six[0] != last_six[1]):
        patterns.append("ABBBAA pattern (e.g., 566655)")
    
    # Check for ABBBA pattern (like 233322)
    if (len(last_six) >= 5 and 
        last_six[0] == last_six[4] and 
        last_six[1] == last_six[2] == last_six[3] and 
        last_six[0] != last_six[1]):
        patterns.append("ABBBA pattern (e.g., 233322)")
    
    # 1. 6-digit patterns (strict matches only)
    # All same digits (666666)
    if len(set(last_six)) == 1:
        patterns.append("6 identical digits")
    
    # Consecutive ascending (123456)
    if is_sequential(last_six, 1):
        patterns.append("6-digit ascending sequence")
        
    # Consecutive descending (654321)
    if is_sequential(last_six, -1):
        patterns.append("6-digit descending sequence")
    
    # More flexible ascending/descending patterns (like 141516)
    def is_flexible_sequential(digits, step=1):
        digits = [int(d) for d in digits]
        for i in range(1, len(digits)):
            if digits[i] - digits[i-1] != step:
                return False
        return True
    
    # Check for flexible ascending (e.g., 141516)
    if is_flexible_sequential(last_six, 1):
        patterns.append("Flexible ascending sequence (e.g., 141516)")
    
    # Check for flexible descending
    if is_flexible_sequential(last_six, -1):
        patterns.append("Flexible descending sequence")
        
    # Palindrome (100001)
    if last_six == last_six[::-1]:
        patterns.append("6-digit palindrome")
    
    # 2. 3-digit patterns (strict matches from image)
    first_triple = last_six[:3]
    second_triple = last_six[3:]
    
    # Double triplets (444555)
    if len(set(first_triple)) == 1 and len(set(second_triple)) == 1 and first_triple != second_triple:
        patterns.append("Double triplets (444555)")
    
    # Similar triplets (121122)
    if (first_triple[0] == first_triple[1] and 
        second_triple[0] == second_triple[1] and 
        first_triple[2] == second_triple[2]):
        patterns.append("Similar triplets (121122)")
    
    # Repeating triplets (786786)
    if first_triple == second_triple:
        patterns.append("Repeating triplets (786786)")
    
    # Nearly sequential (457456) - exactly 1 digit difference
    if abs(int(first_triple) - int(second_triple)) == 1:
        patterns.append("Nearly sequential triplets (457456)")
    
    # 3. 2-digit patterns (strict matches from image)
    # Incremental pairs (111213)
    pairs = [last_six[i:i+2] for i in range(0, 5, 1)]
    try:
        if all(int(pairs[i]) == int(pairs[i-1]) + 1 for i in range(1, len(pairs))):
            patterns.append("Incremental pairs (111213)")

        # Repeating pairs (202020)
        if (pairs[0] == pairs[2] == pairs[4] and 
            pairs[1] == pairs[3] and 
            pairs[0] != pairs[1]):
            patterns.append("Repeating pairs (202020)")

        # Alternating pairs (010101)
        if (pairs[0] == pairs[2] == pairs[4] and 
            pairs[1] == pairs[3] and 
            pairs[0] != pairs[1]):
            patterns.append("Alternating pairs (010101)")

        # Stepping pairs (324252) - Fixed this check
        if (all(int(pairs[i][0]) == int(pairs[i-1][0]) + 1 for i in range(1, len(pairs))) and
            all(int(pairs[i][1]) == int(pairs[i-1][1]) + 2 for i in range(1, len(pairs)))):
            patterns.append("Stepping pairs (324252)")
    except:
        pass
    
    # 4. Exceptional cases (must match exactly)
    exceptional_triplets = ['123', '555', '777', '999']
    if last_three in exceptional_triplets:
        patterns.append(f"Exceptional case ({last_three})")
    
    # Strict validation - only allow patterns that exactly match our rules
    valid_patterns = []
    for p in patterns:
        if any(rule in p for rule in [
            "Special VIP number",
            "ABBBAA pattern",
            "ABBBA pattern",
            "6 identical digits",
            "6-digit ascending sequence",
            "6-digit descending sequence",
            "Flexible ascending sequence",
            "Flexible descending sequence",
            "6-digit palindrome",
            "Double triplets (444555)",
            "Similar triplets (121122)",
            "Repeating triplets (786786)",
            "Nearly sequential triplets (457456)",
            "Incremental pairs (111213)",
            "Repeating pairs (202020)",
            "Alternating pairs (010101)",
            "Stepping pairs (324252)",
            "Exceptional case"
        ]):
            valid_patterns.append(p)
    
    return bool(valid_patterns), ", ".join(valid_patterns) if valid_patterns else "No qualifying fancy pattern"

def lycamobile_fancy_number_checker():
    phone_number = st.text_input("Enter a phone number")
    if phone_number:
        is_fancy, pattern = is_fancy_number(phone_number)
        if is_fancy:
            st.success(f"The phone number {phone_number} has a fancy pattern: {pattern}")
        else:
            st.error(f"The phone number {phone_number} does not have a fancy pattern: {pattern}")

def set_vip_status(username, is_vip):
    """Set or remove VIP status for a user"""
    if not username:
        return False
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_vip = ? WHERE username = ?", 
                      (1 if is_vip else 0, username))
        conn.commit()
        return True
    finally:
        conn.close()

# --------------------------
# Streamlit App
# --------------------------

# Add this at the beginning of the file, after the imports
if 'color_mode' not in st.session_state:
    st.session_state.color_mode = 'light'

def inject_custom_css():
    # Add notification JavaScript
    st.markdown("""
    <script>
    // Request notification permission on page load
    document.addEventListener('DOMContentLoaded', function() {
        if (Notification.permission !== 'granted') {
            Notification.requestPermission();
        }
    });

    // Function to show browser notification
    function showNotification(title, body) {
        if (Notification.permission === 'granted') {
            const notification = new Notification(title, {
                body: body,
                icon: '🔔'
            });
            
            notification.onclick = function() {
                window.focus();
                notification.close();
            };
        }
    }

    // Function to check for new messages
    async function checkNewMessages() {
        try {
            const response = await fetch('/check_messages');
            const data = await response.json();
            
            if (data.new_messages) {
                data.messages.forEach(msg => {
                    showNotification('New Message', `${msg.sender}: ${msg.message}`);
                });
            }
        } catch (error) {
            console.error('Error checking messages:', error);
        }
    }

    // Check for new messages every 30 seconds
    setInterval(checkNewMessages, 30000);
    </script>
    """, unsafe_allow_html=True)

    # Define color schemes for both modes
    colors = {
        'dark': {
            'bg': '#0f172a',
            'sidebar': '#1e293b',
            'card': '#1e293b',
            'text': '#f1f5f9',         # Light gray
            'text_secondary': '#94a3b8',
            'border': '#334155',
            'accent': '#94a3b8',       # Muted slate
            'accent_hover': '#f87171', # Cherry hover (bright)
            'muted': '#64748b',
            'input_bg': '#1e293b',
            'input_text': '#f1f5f9',
            'placeholder_text': '#94a3b8',  # Light gray for placeholder in dark mode
            'my_message_bg': '#94a3b8',  # Slate message
            'other_message_bg': '#1e293b',
            'hover_bg': '#475569',      # Darker slate hover
            'notification_bg': '#1e293b',
            'notification_text': '#f1f5f9',
            'button_bg': '#94a3b8',    # Slate button
            'button_text': '#0f172a',   # Near-black text
            'button_hover': '#f87171', # Cherry hover
            'dropdown_bg': '#1e293b',
            'dropdown_text': '#f1f5f9',
            'dropdown_hover': '#475569',
            'table_header': '#1e293b',
            'table_row_even': '#0f172a',
            'table_row_odd': '#1e293b',
            'table_border': '#334155'
        },
'light': {
        'bg': '#f0f9ff',           
        'sidebar': '#ffffff',
        'card': '#ffffff',
        'text': '#0f172a',        
        'text_secondary': '#334155',
        'border': '#bae6fd',       
        'accent': '#0ea5e9',       
        'accent_hover': '#f97316', 
        'muted': '#64748b',
        'input_bg': '#ffffff',
        'input_text': '#0f172a',
        'placeholder_text': '#475569',  # Darker gray (visible but subtle)
        'my_message_bg': '#0ea5e9',  
        'other_message_bg': '#f8fafc',
        'hover_bg': '#ffedd5',      
        'notification_bg': '#ffffff',
        'notification_text': '#0f172a',
        'button_bg': '#0ea5e9',     
        'button_text': '#0f172a',   
        'button_hover': '#f97316',  
        'dropdown_bg': '#ffffff',
        'dropdown_text': '#0f172a',
        'dropdown_hover': '#ffedd5',
        'table_header': '#e0f2fe', 
        'table_row_even': '#ffffff',
        'table_row_odd': '#f0f9ff',
        'table_border': '#bae6fd'
        }
    }

    # Use the appropriate color scheme based on the session state
    c = colors['dark'] if st.session_state.color_mode == 'dark' else colors['light']
    
    st.markdown(f"""
    <style>
        /* Global Styles */
        .stApp {{
            background-color: {c['bg']};
            color: {c['text']};
        }}
        
        /* Button Styling */
        .stButton > button {{
            background-color: {c['button_bg']} !important;
            color: {c['button_text']} !important;
            border: none !important;
            border-radius: 1rem !important;
            padding: 0.5rem 1rem !important;
            font-weight: 500 !important;
            transition: all 0.2s ease-in-out !important;
        }}
        
        .stButton > button:hover {{
            background-color: {c['button_hover']} !important;
            transform: translateY(-2px);
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        }}
        
        /* Dropdown and Date Picker Styling */
        .stSelectbox [data-baseweb="select"],
        .stSelectbox [data-baseweb="select"] div,
        .stSelectbox [data-baseweb="select"] input,
        .stSelectbox [data-baseweb="popover"] ul,
        .stSelectbox [data-baseweb="select"] span,
        .stDateInput input,
        .stDateInput div[data-baseweb="calendar"] {{
            background-color: {c['input_bg']} !important;
            color: {c['text']} !important;
            border-color: {c['border']} !important;
        }}
        
        .stSelectbox [data-baseweb="select"] {{    
            border: 1px solid {c['border']} !important;
        }}
        
        .stSelectbox [data-baseweb="select"]:hover {{    
            border-color: {c['accent']} !important;
        }}
        
        .stSelectbox [data-baseweb="popover"] {{    
            background-color: {c['input_bg']} !important;
        }}
        
        .stSelectbox [data-baseweb="popover"] ul {{    
            background-color: {c['input_bg']} !important;
            border: 1px solid {c['border']} !important;
        }}
        
        .stSelectbox [data-baseweb="popover"] ul li {{    
            background-color: {c['input_bg']} !important;
            color: {c['text']} !important;
        }}
        
        .stSelectbox [data-baseweb="popover"] ul li:hover {{    
            background-color: {c['dropdown_hover']} !important;
        }}
        
        /* Template selection specific */
        .template-selector {{    
            margin-bottom: 1rem;
        }}
        
        .template-selector label,
        .default-template,
        .template-name {{    
            color: {c['text']} !important;
            font-weight: 500;
        }}
        
        /* Template text styles */
        div[data-testid="stMarkdownContainer"] p strong,
        div[data-testid="stMarkdownContainer"] p em,
        div[data-testid="stMarkdownContainer"] p {{    
            color: {c['text']} !important;
        }}
        
        .template-info {{    
            background-color: {c['card']} !important;
            border: 1px solid {c['border']} !important;
            padding: 0.75rem;
            border-radius: 0.375rem;
            margin-bottom: 1rem;
        }}
        
        .template-info p {{    
            color: {c['text']} !important;
            margin: 0;
        }}
        
        /* Template and stats numbers (Total Templates, Active Templates) */
        .template-stats-number, .template-info-number {{
            color: {c['text']} !important;
            font-weight: bold;
            font-size: 2rem;
        }}
        
        /* Input Fields and Labels */
        .stTextInput input, 
        .stTextArea textarea,
        .stNumberInput input {{    
            background-color: {c['input_bg']} !important;
            color: {c['input_text']} !important;
            border-color: {c['border']} !important;
            caret-color: {c['text']} !important;
        }}
        
        /* Placeholder text color for input fields */
        .stTextInput input::placeholder, 
        .stTextArea textarea::placeholder, 
        .stNumberInput input::placeholder {{
            color: {c['placeholder_text']} !important;
            opacity: 1 !important;
        }}
        
        /* Input focus and selection */
        .stTextInput input:focus,
        .stTextArea textarea:focus,
        .stNumberInput input:focus {{    
            border-color: {c['accent']} !important;
            box-shadow: 0 0 0 1px {c['accent']} !important;
        }}
        
        ::selection {{    
            background-color: {c['accent']} !important;
            color: #ffffff !important;
        }}
        
        /* Input Labels and Text */
        .stTextInput label,
        .stTextArea label,
        .stNumberInput label,
        .stSelectbox label,
        .stDateInput label,
        div[data-baseweb="input"] label,
        .stMarkdown p,
        .element-container label,
        .stDateInput div,
        .stSelectbox div[data-baseweb="select"] div,
        .streamlit-expanderHeader,
        .stAlert p {{    
            color: {c['text']} !important;
        }}
        
        /* Message Alerts */
        .stAlert {{    
            background-color: {c['card']} !important;
            color: {c['text']} !important;
            padding: 1rem !important;
            border-radius: 1rem !important;
            margin-bottom: 1rem !important;
            border: 1px solid {c['border']} !important;
        }}
        
        .stAlert p,
        .stSuccess p,
        .stError p,
        .stWarning p,
        .stInfo p {{    
            color: {c['text']} !important;
        }}
        
        /* Empty state messages */
        .empty-state {{    
            color: {c['text']} !important;
            background-color: {c['card']} !important;
            border: 1px solid {c['border']} !important;
            padding: 1rem;
            border-radius: 0.5rem;
            text-align: center;
            margin: 2rem 0;
        }}
        
        /* Cards */
        .card {{
            background-color: {c['card']};
            border: 1px solid {c['border']};
            padding: 1rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
            color: {c['text']};
        }}
        
        /* Chat Message Styling */
        .chat-message {{
            display: flex;
            margin-bottom: 1rem;
            max-width: 80%;
            animation: fadeIn 0.3s ease-in-out;
        }}
        
        .chat-message.received {{
            margin-right: auto;
        }}
        
        .chat-message.sent {{
            margin-left: auto;
            flex-direction: row-reverse;
        }}
        
        .message-content {{
            padding: 0.75rem 1rem;
            border-radius: 1rem;
            position: relative;
        }}
        
        .received .message-content {{
            background-color: {c['other_message_bg']};
            color: {c['text']};
            border-bottom-left-radius: 0.25rem;
            margin-right: 1rem;
            border: 1px solid {c['border']};
        }}
        
        .sent .message-content {{
            background-color: {c['my_message_bg']};
            color: #222 !important;
            border-bottom-right-radius: 0.25rem;
            margin-left: 1rem;
            border: 1px solid {c['accent_hover']};
        }}
        
        .message-meta {{
            font-size: 0.75rem;
            color: {c['text_secondary']};
            margin-top: 0.25rem;
        }}
        
        .message-avatar {{
            width: 2.5rem;
            height: 2.5rem;
            border-radius: 50%;
            background-color: {c['accent']};
            display: flex;
            align-items: center;
            justify-content: center;
            color: #ffffff;
            font-weight: bold;
            font-size: 1rem;
        }}
        
        /* Table Styling */
        .stDataFrame {{
            background-color: {c['card']} !important;
            border: 1px solid {c['table_border']} !important;
            border-radius: 1rem !important;
            overflow: hidden !important;
        }}
        
        .stDataFrame td {{
            color: {c['text']} !important;
            border-color: {c['table_border']} !important;
            background-color: {c['table_row_even']} !important;
        }}
        
        .stDataFrame tr:nth-child(odd) td {{
            background-color: {c['table_row_odd']} !important;
        }}
        
        .stDataFrame th {{
            color: {c['text']} !important;
            background-color: {c['table_header']} !important;
            border-color: {c['table_border']} !important;
            font-weight: 600 !important;
        }}
        
        /* Buttons */
        .stButton button,
        button[kind="primary"],
        .stDownloadButton button,
        div[data-testid="stForm"] button,
        button[data-testid="baseButton-secondary"],
        .stButton > button {{    
            background-color: {c['button_bg']} !important;
            color: #ffffff !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
            border-radius: 0.75rem !important;
            font-weight: 600 !important;
            transition: all 0.2s ease-in-out !important;
        }}
        
        .stButton button:hover,
        button[kind="primary"]:hover,
        .stDownloadButton button:hover,
        div[data-testid="stForm"] button:hover,
        button[data-testid="baseButton-secondary"]:hover,
        .stButton > button:hover {{    
            background-color: {c['button_hover']} !important;
            transform: translateY(-1px) !important;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1) !important;
        }}
        
        /* Secondary Buttons */
        .secondary-button,
        button[data-testid="baseButton-secondary"],
        div[data-baseweb="button"] {{    
            background-color: {c['button_bg']} !important;
            color: #ffffff !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
            border-radius: 0.75rem !important;
            font-weight: 600 !important;
            transition: all 0.2s ease-in-out !important;
        }}
        
        .secondary-button:hover,
        button[data-testid="baseButton-secondary"]:hover,
        div[data-baseweb="button"]:hover {{    
            background-color: {c['button_hover']} !important;
            transform: translateY(-1px) !important;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1) !important;
        }}
        
        /* VIP Button */
        .vip-button {{    
            background-color: {c['accent']} !important;
            color: #ffffff !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
            border-radius: 0.75rem !important;
            font-weight: 600 !important;
            transition: all 0.2s ease-in-out !important;
        }}
        
        .vip-button:hover {{    
            background-color: {c['accent_hover']} !important;
            transform: translateY(-1px) !important;
        }}
        
        /* Checkbox Styling */
        .stCheckbox > label {{
            color: {c['text']} !important;
        }}
        
        .stCheckbox > div[role="checkbox"] {{
            background-color: {c['input_bg']} !important;
            border-color: {c['border']} !important;
        }}
        
        /* Date Input Styling */
        .stDateInput > div > div {{
            background-color: {c['input_bg']} !important;
            color: {c['input_text']} !important;
            border-color: {c['border']} !important;
        }}
        
        /* Expander Styling */
        .streamlit-expanderHeader {{
            background-color: {c['card']} !important;
            color: {c['text']} !important;
            border-color: {c['border']} !important;
        }}
        
        /* Tabs Styling */
        .stTabs [data-baseweb="tab-list"] {{
            background-color: {c['card']} !important;
            border-color: {c['border']} !important;
        }}
        
        .stTabs [data-baseweb="tab"] {{
            color: {c['text']} !important;
        }}
        
        /* Theme Toggle Switch */
        .theme-toggle {{
            display: flex;
            align-items: center;
            padding: 0.5rem;
            margin-bottom: 1rem;
            border-radius: 0.5rem;
            background-color: {c['card']};
            border: 1px solid {c['border']};
        }}
        
        .theme-toggle label {{
            margin-right: 0.5rem;
            color: {c['text']};
        }}
    </style>
    """, unsafe_allow_html=True)

st.set_page_config(
    page_title="Lyca Management System",
    page_icon=":office:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom sidebar background color and text color for light/dark mode
sidebar_bg = '#ffffff' if st.session_state.get('color_mode', 'light') == 'light' else '#1e293b'
sidebar_text = '#1e293b' if st.session_state.get('color_mode', 'light') == 'light' else '#fff'
st.markdown(f'''
    <style>
    [data-testid="stSidebar"] > div:first-child {{
        background-color: {sidebar_bg} !important;
        color: {sidebar_text} !important;
        transition: background-color 0.2s;
    }}
    [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h1, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span {{
        color: {sidebar_text} !important;
    }}
    </style>
''', unsafe_allow_html=True)

if "authenticated" not in st.session_state:
    st.session_state.update({
        "authenticated": False,
        "role": None,
        "username": None,
        "current_section": "requests",
        "last_request_count": 0,
        "last_mistake_count": 0,
        "last_message_ids": []
    })

init_db()
init_break_session_state()

if not st.session_state.authenticated:
    st.markdown("""
        <div class="login-container">
            <h1 style="text-align: center; margin-bottom: 2rem;">💠 Lyca Management System</h1>
    """, unsafe_allow_html=True)
    
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit_col1, submit_col2, submit_col3 = st.columns([1, 2, 1])
        with submit_col2:
            if st.form_submit_button("Login", use_container_width=True):
                if username and password:
                    role = authenticate(username, password)
                    if role:
                        st.session_state.update({
                            "authenticated": True,
                            "role": role,
                            "username": username,
                            "last_request_count": len(get_requests()),
                            "last_mistake_count": len(get_mistakes()),
                            "last_message_ids": [msg[0] for msg in get_group_messages()]
                        })
                        st.session_state["notification_settings"] = get_user_notification_settings(username)
                        st.rerun()
                    else:
                        st.error("Invalid credentials")
    
    st.markdown("</div>", unsafe_allow_html=True)

else:
    if st.session_state.username:
        st.session_state["notification_settings"] = get_user_notification_settings(st.session_state.username)
    if is_killswitch_enabled():
        st.markdown("""
        <div class="killswitch-active">
            <h3>⚠️ SYSTEM LOCKED ⚠️</h3>
            <p>The system is currently in read-only mode.</p>
        </div>
        """, unsafe_allow_html=True)
    elif is_chat_killswitch_enabled():
        st.markdown("""
        <div class="chat-killswitch-active">
            <h3>⚠️ CHAT LOCKED ⚠️</h3>
            <p>The chat functionality is currently disabled.</p>
        </div>
        """, unsafe_allow_html=True)

    def show_notifications():
        current_requests = get_requests()
        current_mistakes = get_mistakes()
        current_messages = get_group_messages()
        prefs = st.session_state.get("notification_settings", {})
        chat_enabled = prefs.get("chat_notifications", True)
        request_enabled = prefs.get("request_notifications", True)
        
        new_requests = len(current_requests) - st.session_state.last_request_count
        if request_enabled and new_requests > 0 and st.session_state.last_request_count > 0:
            st.toast(f"📋 {new_requests} new request(s) submitted!")
        st.session_state.last_request_count = len(current_requests)
        
        new_mistakes = len(current_mistakes) - st.session_state.last_mistake_count
        if request_enabled and new_mistakes > 0 and st.session_state.last_mistake_count > 0:
            st.toast(f"❌ {new_mistakes} new mistake(s) reported!")
        st.session_state.last_mistake_count = len(current_mistakes)
        
        current_message_ids = [msg[0] for msg in current_messages]
        new_messages = [msg for msg in current_messages if msg[0] not in st.session_state.last_message_ids]
        if chat_enabled:
            for msg in new_messages:
                if msg[1] != st.session_state.username:
                    mentions = msg[4].split(',') if msg[4] else []
                    if st.session_state.username in mentions:
                        st.toast(f"💬 You were mentioned by {msg[1]}!")
                    else:
                        st.toast(f"💬 New message from {msg[1]}!")
        st.session_state.last_message_ids = current_message_ids

    show_notifications()

    with st.sidebar:
        # Sidebar welcome text color: dark in light mode, white in dark mode
        welcome_color = '#1e293b' if st.session_state.get('color_mode', 'light') == 'light' else '#fff'
        # Format username for welcome message
        username_display = st.session_state.username
        if username_display.lower() == "Taha kirri":
            username_display = "Taha Kirri ⚙️"
        else:
            username_display = username_display.title()
        st.markdown(f'<h2 style="color: {welcome_color};">✨ Welcome, {username_display}</h2>', unsafe_allow_html=True)
        
        # Theme toggle
        col1, col2 = st.columns([1, 6])
        with col1:
            current_icon = "🌙" if st.session_state.color_mode == 'dark' else "☀️"
            st.write(current_icon)
        with col2:
            if st.toggle("", value=st.session_state.color_mode == 'light', key='theme_toggle', label_visibility="collapsed"):
                if st.session_state.color_mode != 'light':
                    st.session_state.color_mode = 'light'
                    st.rerun()
            else:
                if st.session_state.color_mode != 'dark':
                    st.session_state.color_mode = 'dark'
                    st.rerun()
        st.markdown("---")
        
        # Base navigation options available to all users
        nav_options = []
        
        # QA users only see quality issues and fancy number
        if st.session_state.role == "qa":
            nav_options.extend([
                ("📞 Quality Issues", "quality_issues"),
                ("💎 Fancy Number", "fancy_number")
            ])
        # Admin and agent see all regular options
        elif st.session_state.role in ["admin", "agent"]:
            nav_options.extend([
                ("📋 Requests", "requests"),
                ("☕ Breaks", "breaks"),
                ("📊 Live KPIs ", "Live KPIs"),
                ("📝 Mistakes", "mistakes"),
                ("💬 Chat", "chat"),
                ("🔔 Notification Settings", "notification_settings"),
                ("⏰ Late Login", "late_login"),
                ("📋 Quality Issues", "quality_issues"),
                ("📄 Mid-shift Issues", "midshift_issues"),
                ("💎 Fancy Number", "fancy_number"),
            ])

            # WFM navigation item is globally toggled; always visible to the
            # 'taha kirri' admin, even when disabled for everyone else.
            _user_lower = st.session_state.username.lower() if st.session_state.get("username") else ""
            if is_wfm_enabled() or (_user_lower == "taha kirri" and st.session_state.role == "admin"):
                nav_options.append(("👥 WFM", "wfm"))
        
        # Add admin option for admin users
        if st.session_state.role == "admin":
            nav_options.append(("⚙️ Admin", "admin"))
        
        for option, value in nav_options:
            if st.button(option, key=f"nav_{value}", use_container_width=True):
                st.session_state.current_section = value
        
        st.markdown("---")
        
        # Show notifications only for admin and agent roles
        if st.session_state.role in ["admin", "agent"]:
            pending_requests = len([r for r in get_requests() if not r[6]])
            new_mistakes = len(get_mistakes())
            unread_messages = len([m for m in get_group_messages() 
                                 if m[0] not in st.session_state.last_message_ids 
                                 and m[1] != st.session_state.username])
            
            st.markdown(f"""
            <div style="
                background-color: {'#1e293b' if st.session_state.color_mode == 'dark' else '#ffffff'};
                padding: 1rem;
                border-radius: 0.5rem;
                border: 1px solid {'#334155' if st.session_state.color_mode == 'dark' else '#e2e8f0'};
                margin-bottom: 20px;
            ">
                <h4 style="
                    color: {'#e2e8f0' if st.session_state.color_mode == 'dark' else '#1e293b'};
                    margin-bottom: 1rem;
                ">🔔 Notifications</h4>
                <p style="
                    color: {'#94a3b8' if st.session_state.color_mode == 'dark' else '#475569'};
                    margin-bottom: 0.5rem;
                ">📋 Pending requests: {pending_requests}</p>
                <p style="
                    color: {'#94a3b8' if st.session_state.color_mode == 'dark' else '#475569'};
                    margin-bottom: 0.5rem;
                ">❌ Recent mistakes: {new_mistakes}</p>
                <p style="
                    color: {'#94a3b8' if st.session_state.color_mode == 'dark' else '#475569'};
                ">💬 Unread messages: {unread_messages}</p>
            </div>
            """, unsafe_allow_html=True)

            # --- Break reminder notifications for agents (5-minute warning) ---
            prefs = st.session_state.get("notification_settings", {})
            if st.session_state.role == "agent":
                morocco_tz = pytz.timezone('Africa/Casablanca')
                now_casa = datetime.now(morocco_tz)
                today_str = now_casa.strftime('%Y-%m-%d')
                agent_id = st.session_state.username
                bookings_today = (
                    st.session_state.get('agent_bookings', {}).get(today_str, {}).get(agent_id)
                )
                if bookings_today:
                    try:
                        from streamlit_autorefresh import st_autorefresh  # type: ignore
                        st_autorefresh(interval=60000, key="agent_autorefresh")
                    except ImportError:
                        pass

                if bookings_today and prefs.get("break_notifications", True):
                    break_times = []
                    for b_type in ["lunch", "early_tea", "late_tea"]:
                        entry = bookings_today.get(b_type)
                        if isinstance(entry, dict):
                            t = entry.get("time")
                            if t:
                                break_times.append(t)
                    if break_times:
                        import streamlit.components.v1 as components
                        js_break = f"""
                        <script>
                        const breakTimes = {json.dumps(break_times)};
                        const serverTimeISO = '{now_casa.isoformat()}';
                        const keyPrefix = 'notified_break_sidebar_';
                        (function() {{
                            const now = new Date(serverTimeISO);
                            const today = now.toISOString().split('T')[0];
                            breakTimes.forEach(bt => {{
                                const [h,m] = bt.split(':');
                                const bTime = new Date(now.getFullYear(), now.getMonth(), now.getDate(), h, m, 0);
                                const diffMin = Math.floor((bTime - now) / 60000);
                                const storageKey = keyPrefix + today + '_' + bt;
                                if (diffMin >= 4 && diffMin < 5 && !localStorage.getItem(storageKey)) {{
                                    const notify = () => new Notification('Break Reminder', {{ body: `Your break starts in 5 minutes at ${{bt}}.` }});
                                    if (Notification.permission === 'granted') {{
                                        notify();
                                        localStorage.setItem(storageKey,'1');
                                    }} else if (Notification.permission !== 'denied') {{
                                        Notification.requestPermission().then(p => {{ if (p==='granted') {{ notify(); localStorage.setItem(storageKey,'1'); }} }});
                                    }}
                                }}
                            }});
                        }})();
                        </script>
                        """
                        components.html(js_break, height=0)

                if bookings_today:
                    upcoming_breaks = []
                    for b_type in ["lunch", "early_tea", "late_tea"]:
                        entry = bookings_today.get(b_type)
                        if isinstance(entry, dict):
                            time_str = entry.get("time")
                            if time_str:
                                try:
                                    hour, minute = map(int, time_str.split(":"))
                                    break_dt = now_casa.replace(hour=hour, minute=minute, second=0, microsecond=0)
                                    upcoming_breaks.append((break_dt, b_type, time_str))
                                except Exception:
                                    continue
                    future_breaks = [b for b in upcoming_breaks if b[0] >= now_casa]
                    if future_breaks:
                        next_break = min(future_breaks, key=lambda x: x[0])
                        delta = next_break[0] - now_casa
                        total_seconds = int(delta.total_seconds())
                        hours = total_seconds // 3600
                        minutes = (total_seconds % 3600) // 60
                        seconds = total_seconds % 60
                        display_label = BREAK_LABELS.get(next_break[1], next_break[1].replace("_", " ").title())
                        
                        # Dynamic colors based on theme
                        is_dark = st.session_state.get('color_mode', 'light') == 'dark'
                        bg_color = '#0f172a' if is_dark else '#f8fafc'
                        text_color = '#e2e8f0' if is_dark else '#1e293b'
                        border_color = '#1d4ed8' if is_dark else '#3b82f6'
                        
                        # Get break time in ISO format for JavaScript
                        break_iso = next_break[0].isoformat()
                        
                        # Use components.html for proper JavaScript execution
                        import streamlit.components.v1 as components
                        countdown_html = f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <style>
                                #break-countdown-container {{
                                    background-color: {bg_color};
                                    color: {text_color};
                                    padding: 0.75rem;
                                    border-radius: 0.5rem;
                                    border: 1px solid {border_color};
                                    margin-bottom: 1rem;
                                    margin-top: 0.5rem;
                                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                                }}
                                #break-countdown {{
                                    font-size: 1.2rem;
                                    font-weight: 600;
                                }}
                            </style>
                        </head>
                        <body>
                            <div id="break-countdown-container">
                                <strong>Next break:</strong> {display_label} at {next_break[2]}<br/>
                                <span id="break-countdown">Starts in {hours:02d}:{minutes:02d}:{seconds:02d}</span>
                            </div>
                            <script>
                            (function() {{
                                const breakTimeISO = '{break_iso}';
                                const breakTime = new Date(breakTimeISO);
                                
                                function updateCountdown() {{
                                    const countdownEl = document.getElementById('break-countdown');
                                    if (!countdownEl) {{
                                        return;
                                    }}
                                    
                                    const now = new Date();
                                    const diff = breakTime.getTime() - now.getTime();
                                    
                                    if (diff <= 0) {{
                                        countdownEl.textContent = 'Break time!';
                                        return;
                                    }}
                                    
                                    const totalSeconds = Math.floor(diff / 1000);
                                    const hours = Math.floor(totalSeconds / 3600);
                                    const minutes = Math.floor((totalSeconds % 3600) / 60);
                                    const seconds = totalSeconds % 60;
                                    
                                    countdownEl.textContent = 'Starts in ' + 
                                        String(hours).padStart(2, '0') + ':' + 
                                        String(minutes).padStart(2, '0') + ':' + 
                                        String(seconds).padStart(2, '0');
                                }}
                                
                                // Clear any existing interval
                                if (window.countdownInterval) {{
                                    clearInterval(window.countdownInterval);
                                }}
                                
                                // Update immediately
                                updateCountdown();
                                
                                // Update every second
                                window.countdownInterval = setInterval(updateCountdown, 1000);
                            }})();
                            </script>
                        </body>
                        </html>
                        """
                        components.html(countdown_html, height=120)
                    else:
                        st.caption("✅ All of today's scheduled breaks have passed.")

            # --- Auto-update & browser notification for admin when new request is added ---
            if st.session_state.role == "admin":
                # Server-side rerun every 15 s keeps data fresh without a full tab reload
                try:
                    from streamlit_autorefresh import st_autorefresh  # type: ignore
                    st_autorefresh(interval=15000, key="admin_autorefresh")
                except ImportError:
                    # Package not available – skip (notifications will still work on manual interaction)
                    pass

                import streamlit.components.v1 as components
                js_code = f'''
                <script>
                const currentPending = {pending_requests};
                const key = 'lastPendingRequests';

                function notifyNewRequest() {{
                    if (Notification.permission === "granted") {{
                        new Notification("New Request", {{ body: "A new request has been submitted." }});
                    }} else if (Notification.permission !== "denied") {{
                        Notification.requestPermission().then(perm => {{
                            if (perm === "granted") {{
                                new Notification("New Request", {{ body: "A new request has been submitted." }});
                            }}
                        }});
                    }}
                }}

                function checkAndNotify() {{
                    let last = parseInt(window.localStorage.getItem(key) || '0');
                    if (currentPending > last) {{
                        notifyNewRequest();
                    }}
                    window.localStorage.setItem(key, currentPending);
                }}

                // Run the check on initial load
                checkAndNotify();

                // Set up a polling interval only if one isn't already running
                // Removed JavaScript block that triggers top.location.reload() on an interval

                </script>
                '''
                components.html(js_code, height=0)


        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()

    st.title(st.session_state.current_section.title())

    if st.session_state.current_section == "requests":
        if not is_killswitch_enabled():
            render_notification_permission_banner("requests-permission-banner")
            # Group selection for admin
            group_filter = None
            if st.session_state.role == "admin":
                all_groups = list(set([u[3] for u in get_all_users() if u[3]]))
                group_filter = st.selectbox("Select Group to View Requests", all_groups, key="admin_request_group")
            else:
                # Set group_name in session_state for agents
                if not hasattr(st.session_state, 'group_name') or not st.session_state.group_name:
                    for u in get_all_users():
                        if u[1] == st.session_state.username:
                            st.session_state.group_name = u[3]
                            break
                group_filter = st.session_state.get('group_name')
            with st.expander("➕ Submit New Request"):
                with st.form("request_form"):
                    cols = st.columns([1, 3])
                    request_type = cols[0].selectbox("Type", ["Email", "Phone", "Ticket"])
                    identifier = cols[1].text_input("Identifier")
                    comment = st.text_area("Comment")
                    if st.form_submit_button("Submit"):
                        if identifier and comment:
                            # Determine group for request
                            if st.session_state.role == "admin":
                                # Admins can select any group
                                all_groups = list(set([u[3] for u in get_all_users() if u[3]]))
                                if all_groups:
                                    selected_group = st.selectbox("Assign Request to Group", all_groups, key="admin_request_group_submit")
                                else:
                                    st.warning("No groups available. Please create a group first.")
                                    selected_group = None
                                group_for_request = selected_group
                            else:
                                # Agents use their own group
                                user_group = None
                                for u in get_all_users():
                                    if u[1] == st.session_state.username:
                                        user_group = u[3]
                                        break
                                group_for_request = user_group
                            if group_for_request:
                                if add_request(st.session_state.username, request_type, identifier, comment, group_for_request):
                                    st.success("Request submitted successfully!")
                                    st.rerun()
                            else:
                                st.error("Please select a group for the request.")
        
            st.subheader("🔍 Search Requests")
            search_query = st.text_input("Search requests...")
            # Filter requests by group
            if st.session_state.role == "admin":
                # Admin can filter by any group
                if group_filter:
                    all_requests = search_requests(search_query) if search_query else get_requests()
                    requests = [r for r in all_requests if (len(r) > 7 and r[7] == group_filter)]
                else:
                    requests = search_requests(search_query) if search_query else get_requests()
            else:
                # Agents can only see their own group, regardless of filter
                user_group = None
                for u in get_all_users():
                    if u[1] == st.session_state.username:
                        user_group = u[3]
                        break
                all_requests = search_requests(search_query) if search_query else get_requests()
                requests = [r for r in all_requests if (len(r) > 7 and r[7] == user_group)]
            
            st.subheader("All Requests")
            for req in requests:
                req_id, agent, req_type, identifier, comment, timestamp, completed, group_name = req
                with st.container():
                    cols = st.columns([0.1, 0.9])
                    with cols[0]:
                        st.checkbox("Done", value=bool(completed), 
                                   key=f"check_{req_id}", 
                                   on_change=update_request_status,
                                   args=(req_id, not completed))
                    with cols[1]:
                        st.markdown(f"""
                        <div class="card">
                            <div style="display: flex; justify-content: space-between;">
                                <h4>#{req_id} - {req_type}</h4>
                                <small>{timestamp}</small>
                            </div>
                            <p>Agent: {agent}</p>
                            <p>Identifier: {identifier}</p>
                            <div style="margin-top: 1rem;">
                                <h5>Status Updates:</h5>
                        """, unsafe_allow_html=True)
                        
                        comments = get_request_comments(req_id)
                        for comment in comments:
                            cmt_id, _, user, cmt_text, cmt_time = comment
                            st.markdown(f"""
                                <div class="comment-box">
                                    <div class="comment-user">
                                        <small><strong>{user}</strong></small>
                                        <small>{cmt_time}</small>
                                    </div>
                                    <div class="comment-text">{cmt_text}</div>
                                </div>
                            """, unsafe_allow_html=True)
                        
                        st.markdown("</div>", unsafe_allow_html=True)
                        
                        if st.session_state.role == "admin":
                            with st.form(key=f"comment_form_{req_id}"):
                                new_comment = st.text_input("Add status update/comment")
                                if st.form_submit_button("Add Comment"):
                                    if new_comment:
                                        add_request_comment(req_id, st.session_state.username, new_comment)
                                        st.rerun()
        else:
            st.error("System is currently locked. Access to requests is disabled.")

    elif st.session_state.current_section == "mistakes":
        if not is_killswitch_enabled():
            # Only show mistake reporting form to admin users
            if st.session_state.role == "admin":
                with st.expander("➕ Report New Mistake"):
                    with st.form("mistake_form"):
                        cols = st.columns(3)
                        agent_name = cols[0].text_input("Agent Name")
                        ticket_id = cols[1].text_input("Ticket ID")
                        error_description = st.text_area("Error Description")
                        if st.form_submit_button("Submit"):
                            if agent_name and ticket_id and error_description:
                                add_mistake(st.session_state.username, agent_name, ticket_id, error_description)
                                st.success("Mistake reported successfully!")
                                st.rerun()
        
            st.subheader("🔍 Search Mistakes")
            search_query = st.text_input("Search mistakes...")
            mistakes = search_mistakes(search_query) if search_query else get_mistakes()
            
            st.subheader("Mistakes Log")
            for mistake in mistakes:
                m_id, tl, agent, ticket, error, ts = mistake
                st.markdown(f"""
                <div class="card">
                    <div style="display: flex; justify-content: space-between;">
                        <h4>#{m_id}</h4>
                        <small>{ts}</small>
                    </div>
                    <p>Agent: {agent}</p>
                    <p>Ticket: {ticket}</p>
                    <p>Error: {error}</p>
                    <p><small>Reported by: {tl}</small></p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.error("System is currently locked. Access to mistakes is disabled.")

    elif st.session_state.current_section == "late_login":
        if not is_killswitch_enabled():
            # Agent submission form (existing behavior)
            if st.session_state.role in ["agent", "admin"]:
                with st.expander("➕ Submit Late Login", expanded=False):
                    with st.form("late_login_form"):
                        # Agent name is fixed to the logged-in user; they cannot change it.
                        st.markdown(f"**Agent Name:** {st.session_state.username}")
                        agent_name = st.session_state.username
                        # Use free-text inputs for time so agents can enter any format they use operationally
                        presence_time = st.text_input("Presence Time (e.g. 09:00)")
                        login_time = st.text_input("Login Time (e.g. 09:15)")

                        # Use dropdown options for late_login reasons
                        late_reason_choices = get_dropdown_options("late_login") or []
                        reason_choices = ["Select a reason..."] + late_reason_choices
                        reason_choice = st.selectbox(
                            "Reason for late login",
                            reason_choices,
                            index=0
                        )
                        extra_note = st.text_input("Additional details (optional)")

                        if st.form_submit_button("Submit Late Login"):
                            if agent_name and presence_time and login_time and reason_choice != "Select a reason...":
                                reason_value = reason_choice
                                if extra_note.strip():
                                    reason_value = f"{reason_choice} - {extra_note.strip()}"
                                if add_late_login(agent_name, presence_time, login_time, reason_value):
                                    st.success("Late login submitted and pending approval.")
                                    st.rerun()
                            else:
                                st.error("Please fill all fields and select a reason.")

            st.subheader("Late Login Entries")

            # Load all late logins once
            all_late_rows = get_late_logins()

            # Admins: keep search + date filters and see all entries
            if st.session_state.role == "admin":
                col_f1, col_f2, col_f3 = st.columns(3)
                with col_f1:
                    late_search = st.text_input("Search (agent / reason)")
                with col_f2:
                    late_start_date = st.date_input("From date", value=None)
                with col_f3:
                    late_end_date = st.date_input("To date", value=None)

                filtered_rows = []
                for row in all_late_rows:
                    # id, agent_name, presence_time, login_time, reason, timestamp, status, approved_by, approved_at
                    agent_name_f   = row[1]
                    reason_f       = row[4]
                    submitted_at_f = row[5]

                    # Text search on agent or reason
                    if late_search:
                        s = late_search.lower()
                        if s not in str(agent_name_f).lower() and s not in str(reason_f).lower():
                            continue

                    # Date interval filter based on submitted_at
                    if late_start_date or late_end_date:
                        dt = convert_to_casablanca_date(submitted_at_f)
                        if dt:
                            if late_start_date and dt < late_start_date:
                                continue
                            if late_end_date and dt > late_end_date:
                                continue

                    filtered_rows.append(row)

                late_rows = filtered_rows
            else:
                # Agents: no search/date filters; only see their own late logins
                late_rows = [row for row in all_late_rows if str(row[1]).lower() == str(st.session_state.username).lower()]

            if not late_rows:
                st.info("No late login entries match your filters.")
            else:
                for row in late_rows:
                    # Expected order from SELECT *: id, agent_name, presence_time, login_time,
                    # reason, timestamp, status, approved_by, approved_at
                    entry_id     = row[0]
                    agent_name   = row[1]
                    presence     = row[2]
                    login_t      = row[3]
                    reason       = row[4]
                    submitted_at = row[5]
                    status       = row[6] if len(row) > 6 and row[6] else "pending"
                    approved_by  = row[7] if len(row) > 7 else None
                    approved_at  = row[8] if len(row) > 8 else None

                    header = f"{agent_name} | {presence} → {login_t} | {status.upper()}"
                    with st.expander(header):
                        st.write(f"**Agent:** {agent_name}")
                        st.write(f"**Presence time:** {presence}")
                        st.write(f"**Login time:** {login_t}")
                        st.write(f"**Reason:** {reason}")
                        st.write(f"**Submitted at:** {submitted_at}")
                        st.write(f"**Status:** {status.upper()}")
                        if approved_by:
                            st.write(f"**Approved/Rejected by:** {approved_by}")
                        if approved_at:
                            st.write(f"**Approved/Rejected at:** {approved_at}")

                        # Only admins can approve/reject, and only while pending
                        if st.session_state.role == "admin" and status == "pending":
                            c1, c2 = st.columns(2)
                            with c1:
                                if st.button("✅ Approve", key=f"late_approve_{entry_id}"):
                                    if approve_late_login(entry_id, st.session_state.username):
                                        st.success("Late login approved.")
                                        st.rerun()
                            with c2:
                                if st.button("❌ Reject", key=f"late_reject_{entry_id}"):
                                    if reject_late_login(entry_id, st.session_state.username):
                                        st.success("Late login rejected.")
                                        st.rerun()

                # Optional: simple export button including approval info
                if st.session_state.role == "admin":
                    import pandas as pd
                    df = pd.DataFrame(late_rows, columns=[
                        "ID", "Agent Name", "Presence Time", "Login Time",
                        "Reason", "Submitted At", "Status", "Approved By", "Approved At"
                    ])
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Download Late Logins as CSV",
                        csv,
                        "late_logins.csv",
                        "text/csv"
                    )
        else:
            st.error("System is currently locked. Access to late login is disabled.")

    elif st.session_state.current_section == "chat":
        if not is_killswitch_enabled():
            render_notification_permission_banner("chat-permission-banner")
            if is_chat_killswitch_enabled():
                st.warning("Chat functionality is currently disabled by the administrator.")
            else:

                # Lightweight auto-refresh so new messages appear without manual reload
                try:
                    from streamlit_autorefresh import st_autorefresh  # type: ignore
                    st_autorefresh(
                        interval=5000,
                        key=f"chat_autorefresh_{st.session_state.username}"
                    )
                except ImportError:
                    pass

                # Group chat group selection
                group_filter = None
                if st.session_state.role == "admin":
                    all_groups = list(set([u[3] for u in get_all_users() if u[3]]))
                    group_filter = st.selectbox("Select Group to View Chat", all_groups, key="admin_chat_group")
                else:
                    user_group = None
                    for u in get_all_users():
                        if u[1] == st.session_state.username:
                            user_group = u[3]
                            break
                    st.session_state.group_name = user_group
                    group_filter = user_group

                st.subheader("Group Chat")
                if st.session_state.role == "admin":
                    view_group = group_filter if group_filter else None
                else:
                    user_group = None
                    for u in get_all_users():
                        if u[1] == st.session_state.username:
                            user_group = u[3]
                            break
                    view_group = user_group

                if view_group is not None and str(view_group).strip() != "":
                    messages = get_group_messages(view_group)
                else:
                    messages = []
                    if st.session_state.role == "agent":
                        st.warning("You are not assigned to a group. Please contact an admin.")

                
                st.markdown('''<style>
                .chat-container {background: #f1f5f9; border-radius: 8px; padding: 1rem; max-height: 400px; overflow-y: auto; margin-bottom: 1rem;}
                .chat-message {display: flex; align-items: flex-start; margin-bottom: 12px;}
                .chat-message.sent {flex-direction: row-reverse;}
                .chat-message .message-avatar {width: 36px; height: 36px; background: #3b82f6; color: #fff; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 1.1rem; margin: 0 10px;}
                .chat-message .message-content {background: #fff; border-radius: 6px; padding: 8px 14px; min-width: 80px; box-shadow: 0 1px 3px rgba(0,0,0,0.04);}
                .chat-message.sent .message-content {background: #dbeafe;}
                .chat-message .message-meta {font-size: 0.8rem; color: #64748b; margin-top: 2px;}
                                </style>''', unsafe_allow_html=True)
                st.markdown('<div class="chat-container">', unsafe_allow_html=True)

                for msg in reversed(messages):
                    if isinstance(msg, dict):
                        data = msg
                    else:
                        base_keys = [
                            "id", "sender", "message", "timestamp", "mentions",
                            "group_name", "reactions", "is_deleted", "edited_at",
                            "edited_by", "deleted_by", "deleted_at", "pinned",
                            "pinned_by", "pinned_at"
                        ]
                        values = list(msg) + [None] * max(0, len(base_keys) - len(msg))
                        data = dict(zip(base_keys, values))
                    reactions = data.get('reactions', {})
                    if isinstance(reactions, str):
                        try:
                            reactions = json.loads(reactions) if reactions else {}
                        except Exception:
                            reactions = {}
                    msg_id = data.get('id')
                    sender = data.get('sender', 'Unknown')
                    message_text = data.get('message', '')
                    ts = data.get('timestamp', '')
                    is_deleted = bool(data.get('is_deleted'))
                    is_sent = sender == st.session_state.username

                    if is_deleted:
                        if st.session_state.role == "admin":
                            display_text = f"<em>Removed:</em> {message_text}"
                        else:
                            display_text = "<em>Message removed by a moderator.</em>"
                    else:
                        display_text = message_text

                    meta_parts = [sender, ts]
                    if data.get('edited_at'):
                        meta_parts.append("edited")
                    meta_line = " • ".join(meta_parts)

                    st.markdown(f"""
                    <div class="chat-message {'sent' if is_sent else 'received'}">
                        <div class="message-avatar">{sender[0].upper()}</div>
                        <div class="message-content">
                            <div>{display_text}</div>
                            <div class="message-meta">{meta_line}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                    
                st.markdown('</div>', unsafe_allow_html=True)

                
                    
                with st.form("chat_form", clear_on_submit=True):
                    message = st.text_input("Type your message...", key="chat_input")
                    col1, col2 = st.columns([5,1])
                    with col2:
                        if st.form_submit_button("Send"):
                            if message:
                                if st.session_state.role == "admin":
                                    send_to_group = group_filter
                                else:
                                    send_to_group = None
                                    for u in get_all_users():
                                        if u[1] == st.session_state.username:
                                            send_to_group = u[3]
                                            break
                                if send_to_group:
                                    send_group_message(st.session_state.username, message, send_to_group)
                                else:
                                    st.warning("No group selected for chat.")
                                st.rerun()
        else:
            st.error("System is currently locked. Access to chat is disabled.")

    elif st.session_state.current_section == "notification_settings":
        prefs = st.session_state.get("notification_settings", get_user_notification_settings(st.session_state.username))
        render_notification_permission_banner("notification-settings-banner")
        st.subheader("Browser Notification Preferences")
        st.write("Choose where you want in-app and browser notifications. These apply to both desktop toasts and custom alerts.")
        with st.form("notification_settings_form"):
            request_pref = st.checkbox("Requests & mistakes updates", value=prefs.get("request_notifications", True))
            break_pref = st.checkbox("Break reminders", value=prefs.get("break_notifications", True))
            submitted = st.form_submit_button("Save preferences")
        if submitted:
            update_user_notification_settings(
                st.session_state.username,
                chat=chat_pref,
                request=request_pref,
                breaks=break_pref
            )
            st.session_state.notification_settings = get_user_notification_settings(st.session_state.username)
            st.success("Notification preferences updated.")
        st.markdown("""
        **Tip:** Make sure browser notifications are allowed for this site in addition to the toggles above.  
        You can revisit this page anytime to adjust your preferences.
        """)

    elif st.session_state.current_section == "Live KPIs":
        if not is_killswitch_enabled():
            st.subheader("📋 AHT Table")
            import pandas as pd
            # --- HOLD Table Functions (now using SQLite for persistence) ---
            import io
            def add_hold_table(uploader, table_data):
                conn = get_db_connection()
                try:
                    cursor = conn.cursor()
                    # Only keep the latest table: clear any existing records
                    cursor.execute("DELETE FROM hold_tables")
                    timestamp = get_casablanca_time()  # Ensure Casablanca time
                    cursor.execute("INSERT INTO hold_tables (uploader, table_data, timestamp) VALUES (?, ?, ?)", (uploader, table_data, timestamp))
                    conn.commit()
                    return True
                finally:
                    conn.close()

            def get_hold_tables():
                conn = get_db_connection()
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT id, uploader, table_data, timestamp FROM hold_tables ORDER BY id DESC LIMIT 1")
                    result = cursor.fetchall()
                    return result
                finally:
                    conn.close()

            def clear_hold_tables():
                conn = get_db_connection()
                try:
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM hold_tables")
                    conn.commit()
                    return True
                finally:
                    conn.close()
            # --- END HOLD Table Functions ---
            # Only show table paste option to admin users
            if st.session_state.role == "admin":
                st.write("Paste a table copied from Excel (CSV or tab-separated):")
                pasted_table = st.text_area("Paste table here", height=150)
                if st.button("Save HOLD Table"):
                    if pasted_table.strip():
                        try:
                            # Try to parse as DataFrame
                            try:
                                df = pd.read_csv(io.StringIO(pasted_table), sep=None, engine='python')
                            except Exception:
                                df = pd.read_csv(io.StringIO(pasted_table), sep='\t')
                            table_data = df.to_csv(index=False)
                            clear_hold_tables()  # Only keep latest
                            if add_hold_table(st.session_state.username, table_data):
                                st.success("Table saved successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to save table.")
                        except Exception as e:
                            st.error(f"Error parsing table: {str(e)}")
                    else:
                        st.warning("Please paste a table.")
                # Add clear button with confirmation
                with st.form("clear_hold_tables_form"):
                    confirm_clear_hold = st.checkbox("I understand and want to clear all HOLD tables")
                    if st.form_submit_button("Clear HOLD Tables"):
                        if confirm_clear_hold:
                            if clear_hold_tables():
                                st.success("All HOLD tables deleted successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to delete HOLD tables.")
                        else:
                            st.warning("Please confirm by checking the checkbox.")
            # Display most recent table (visible to all users)
            tables = get_hold_tables()
            if tables:
                table_id, uploader, table_data, timestamp = tables[0]
                st.markdown(f"""
                <div style='border: 1px solid #ddd; padding: 10px; margin-bottom: 20px; border-radius: 5px;'>
                    <p><strong>Uploaded by:</strong> {uploader}</p>
                    <p><small>Uploaded at: {timestamp}</small></p>
                </div>
                """, unsafe_allow_html=True)
                try:
                    import pandas as pd
                    import io
                    df = pd.read_csv(io.StringIO(table_data))
                    search_query = st.text_input("🔍 Search in table", key="hold_table_search")
                    if search_query:
                        filtered_df = df[df.apply(lambda row: row.astype(str).str.contains(search_query, case=False, na=False).any(), axis=1)]
                        st.dataframe(filtered_df, use_container_width=True)
                    else:
                        st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(f"Error displaying table: {str(e)}")
            else:
                st.info("No HOLD tables available")
        else:
            st.error("System is currently locked. Access to HOLD images is disabled.")

    elif st.session_state.current_section == "late_login":
        st.subheader("⏰ Late Login Report")
        
        if not is_killswitch_enabled():
            with st.form("late_login_form"):
                cols = st.columns(3)
                presence_time = cols[0].text_input("Time of presence (HH:MM)", placeholder="08:30")
                login_time = cols[1].text_input("Time of log in (HH:MM)", placeholder="09:15")
                # Get dynamic dropdown options from database
                late_login_options = get_dropdown_options("late_login")
                if not late_login_options:
                    late_login_options = ["No options available"]
                reason = cols[2].selectbox("Reason", late_login_options)
                
                if st.form_submit_button("Submit"):
                    try:
                        datetime.strptime(presence_time, "%H:%M")
                        datetime.strptime(login_time, "%H:%M")
                        add_late_login(
                            st.session_state.username,
                            presence_time,
                            login_time,
                            reason
                        )
                        st.success("Late login reported successfully!")
                    except ValueError:
                        st.error("Invalid time format. Please use HH:MM format (e.g., 08:30)")
        
        st.subheader("Late Login Records")
        late_logins = get_late_logins()
        
        if st.session_state.role == "admin":
            # Search and date filter only for admin users
            col1, col2 = st.columns([2, 1])
            with col1:
                search_query = st.text_input("🔍 Search late login records...", key="late_login_search")
            with col2:
                start_date = st.date_input("Start date", key="late_login_start_date")
                end_date = st.date_input("End date", key="late_login_end_date")

            # Filtering logic
            if search_query or start_date or end_date:
                filtered_logins = []
                for login in late_logins:
                    matches_search = True
                    matches_date = True
                    
                    if search_query:
                        matches_search = (
                            search_query.lower() in login[1].lower() or  # Agent name
                            search_query.lower() in login[4].lower() or  # Reason
                            search_query in login[2] or  # Presence time
                            search_query in login[3]     # Login time
                        )
                    
                    if start_date and end_date:
                        try:
                            record_date = datetime.strptime(login[5], "%Y-%m-%d %H:%M:%S").date()
                            matches_date = start_date <= record_date <= end_date
                        except:
                            matches_date = False
                    elif start_date:
                        try:
                            record_date = datetime.strptime(login[5], "%Y-%m-%d %H:%M:%S").date()
                            matches_date = record_date == start_date
                        except:
                            matches_date = False
                    # else: no date filter
                    if matches_search and matches_date:
                        filtered_logins.append(login)
                late_logins = filtered_logins
            
            if late_logins:
                data = []
                for login in late_logins:
                    _, agent, presence, login_time, reason, ts = login
                    data.append({
                        "Agent's Name": agent,
                        "Time of presence": presence,
                        "Time of log in": login_time,
                        "Reason": reason,
                        "Reported At": ts
                    })
                
                df = pd.DataFrame(data)
                st.dataframe(df)
                csv = df.to_csv(index=False).encode('utf-8')
                # File name logic
                if start_date and end_date:
                    fname = f"late_logins_{start_date}_to_{end_date}.csv"
                elif start_date:
                    fname = f"late_logins_{start_date}.csv"
                else:
                    fname = "late_logins_all.csv"
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=fname,
                    mime="text/csv"
                )
                
                if 'confirm_clear_late_login' not in st.session_state:
                    st.session_state.confirm_clear_late_login = False
                if not st.session_state.confirm_clear_late_login:
                    if st.button("Clear All Records"):
                        st.session_state.confirm_clear_late_login = True
                else:
                    st.warning("⚠️ Are you sure you want to clear all late login records? This cannot be undone!")
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button("Yes, Clear All Late Logins"):
                            clear_late_logins()
                            st.session_state.confirm_clear_late_login = False
                            st.rerun()
                    with col2:
                        if st.button("Cancel"):
                            st.session_state.confirm_clear_late_login = False
                            st.rerun()
            else:
                st.info("No late login records found")
        else:
            # Regular users only see their own records without search
            user_logins = [login for login in late_logins if login[1] == st.session_state.username]
            if user_logins:
                data = []
                for login in user_logins:
                    _, agent, presence, login_time, reason, ts = login
                    data.append({
                        "Time of presence": presence,
                        "Time of log in": login_time,
                        "Reason": reason,
                        "Reported At": ts
                    })
                
                df = pd.DataFrame(data)
                st.dataframe(df)
            else:
                st.info("You have no late login records")

    elif st.session_state.current_section == "quality_issues":
        st.subheader("📞 Quality Related Technical Issue")
        
        if not is_killswitch_enabled():
            with st.form("quality_issue_form"):
                cols = st.columns(4)
                # Get dynamic dropdown options from database
                quality_issue_options = get_dropdown_options("quality_issues")
                if not quality_issue_options:
                    quality_issue_options = ["No options available"]
                issue_type = cols[0].selectbox("Type of issue", quality_issue_options)
                timing = cols[1].text_input("Timing (HH:MM)", placeholder="14:30")
                mobile_number = cols[2].text_input("Mobile number")
                product_options = get_dropdown_options("quality_products")
                if not product_options:
                    product_options = [
                        "LM_CS_LMFR_FR",
                        "LMREG_FR",
                        "LM_CS_LMBE_FR",
                        "LM_PM_LMFR_FR",
                        "LM_CS_LMUSA_EN",
                        "LM_CS_LMUSA_ES",
                        "LM_CS_LMUK_EN",
                        "LM_CS_LMDE_DE",
                        "LM_CS_LMCH_IT",
                        "LM_CS_LMNL_NL",
                        "LM_CS_LMBE_FL",
                        "LM_CS_LMPT_PT",
                        "LM_CS_LMCH_DE",
                        "LM_CS_LMIT_IT",
                        "WC_CS_LMFR_LMCH_LMBE_FR",
                        "WC_CS_LMDE_DE"
                    ]
                    for p in product_options:
                        add_dropdown_option("quality_products", p)
                    product_options = get_dropdown_options("quality_products")
                product = cols[3].selectbox("Product", product_options)
                
                if st.form_submit_button("Submit"):
                    try:
                        datetime.strptime(timing, "%H:%M")
                        add_quality_issue(
                            st.session_state.username,
                            issue_type,
                            timing,
                            mobile_number,
                            product
                        )
                        st.success("Quality issue reported successfully!")
                    except ValueError:
                        st.error("Invalid time format. Please use HH:MM format (e.g., 14:30)")
        
        st.subheader("Quality Issue Records")
        quality_issues = get_quality_issues()
        
        # Allow both admin and QA roles to see all records and use search/filter
        if st.session_state.role in ["admin", "qa"]:
            # Search and date filter for admin and QA users
            col1, col2 = st.columns([2, 1])
            with col1:
                search_query = st.text_input("🔍 Search quality issues...", key="quality_issues_search")
            with col2:
                start_date = st.date_input("Start date", key="quality_issues_start_date")
                end_date = st.date_input("End date", key="quality_issues_end_date")

            # Filtering logic
            if search_query or start_date or end_date:
                filtered_issues = []
                for issue in quality_issues:
                    matches_search = True
                    matches_date = True
                    
                    if search_query:
                        matches_search = (
                            search_query.lower() in issue[1].lower() or  # Agent name
                            search_query.lower() in issue[2].lower() or  # Issue type
                            search_query in issue[3] or  # Timing
                            search_query in issue[4] or  # Mobile number
                            search_query.lower() in issue[5].lower()  # Product
                        )
                    
                    if start_date and end_date:
                        try:
                            record_date = datetime.strptime(issue[6], "%Y-%m-%d %H:%M:%S").date()
                            matches_date = start_date <= record_date <= end_date
                        except:
                            matches_date = False
                    elif start_date:
                        try:
                            record_date = datetime.strptime(issue[6], "%Y-%m-%d %H:%M:%S").date()
                            matches_date = record_date == start_date
                        except:
                            matches_date = False
                    # else: no date filter
                    if matches_search and matches_date:
                        filtered_issues.append(issue)
                quality_issues = filtered_issues
            
            if quality_issues:
                data = []
                for issue in quality_issues:
                    _, agent, issue_type, timing, mobile, product, ts = issue
                    data.append({
                        "Agent's Name": agent,
                        "Type of issue": issue_type,
                        "Timing": timing,
                        "Mobile number": mobile,
                        "Product": product,
                        "Reported At": ts
                    })
                
                df = pd.DataFrame(data)
                st.dataframe(df)
                csv = df.to_csv(index=False).encode('utf-8')
                # File name logic
                if start_date and end_date:
                    fname = f"quality_issues_{start_date}_to_{end_date}.csv"
                elif start_date:
                    fname = f"quality_issues_{start_date}.csv"
                else:
                    fname = "quality_issues_all.csv"
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=fname,
                    mime="text/csv"
                )
                
                # Only show clear button for admins, not QA
                if st.session_state.role == "admin":
                    if 'confirm_clear_quality_issues' not in st.session_state:
                        st.session_state.confirm_clear_quality_issues = False
                    if not st.session_state.confirm_clear_quality_issues:
                        if st.button("Clear All Records"):
                            st.session_state.confirm_clear_quality_issues = True
                    else:
                        st.warning("⚠️ Are you sure you want to clear all quality issue records? This cannot be undone!")
                        col1, col2 = st.columns([1, 1])
                        with col1:
                            if st.button("Yes, Clear All Quality Issues"):
                                clear_quality_issues()
                                st.session_state.confirm_clear_quality_issues = False
                                st.rerun()
                        with col2:
                            if st.button("Cancel"):
                                st.session_state.confirm_clear_quality_issues = False
                                st.rerun()
            else:
                st.info("No quality issue records found")
        else:
            # Regular users only see their own records without search
            user_issues = [issue for issue in quality_issues if issue[1] == st.session_state.username]
            if user_issues:
                data = []
                for issue in user_issues:
                    _, agent, issue_type, timing, mobile, product, ts = issue
                    data.append({
                        "Type of issue": issue_type,
                        "Timing": timing,
                        "Mobile number": mobile,
                        "Product": product,
                        "Reported At": ts
                    })
                
                df = pd.DataFrame(data)
                st.dataframe(df)
            else:
                st.info("You have no quality issue records")

    elif st.session_state.current_section == "midshift_issues":
        st.subheader("🔄 Mid-shift Technical Issue")
        
        if not is_killswitch_enabled():
            with st.form("midshift_issue_form"):
                cols = st.columns(3)
                # Get dynamic dropdown options from database
                midshift_issue_options = get_dropdown_options("midshift_issues")
                if not midshift_issue_options:
                    midshift_issue_options = ["No options available"]
                issue_type = cols[0].selectbox("Issue Type", midshift_issue_options)
                start_time = cols[1].text_input("Start time (HH:MM)", placeholder="10:00")
                end_time = cols[2].text_input("End time (HH:MM)", placeholder="10:30")
                
                if st.form_submit_button("Submit"):
                    try:
                        datetime.strptime(start_time, "%H:%M")
                        datetime.strptime(end_time, "%H:%M")
                        add_midshift_issue(
                            st.session_state.username,
                            issue_type,
                            start_time,
                            end_time
                        )
                        st.success("Mid-shift issue reported successfully!")
                    except ValueError:
                        st.error("Invalid time format. Please use HH:MM format (e.g., 10:00)")
        
        st.subheader("Mid-shift Issue Records")
        midshift_issues = get_midshift_issues()
        
        if st.session_state.role == "admin":
            # Search and date filter only for admin users
            col1, col2 = st.columns([2, 1])
            with col1:
                search_query = st.text_input("🔍 Search mid-shift issues...", key="midshift_issues_search")
            with col2:
                start_date = st.date_input("Start date", key="midshift_issues_start_date")
                end_date = st.date_input("End date", key="midshift_issues_end_date")

            # Filtering logic
            if search_query or start_date or end_date:
                filtered_issues = []
                for issue in midshift_issues:
                    matches_search = True
                    matches_date = True
                    
                    if search_query:
                        matches_search = (
                            search_query.lower() in issue[1].lower() or  # Agent name
                            search_query.lower() in issue[2].lower() or  # Issue type
                            search_query in issue[3] or  # Start time
                            search_query in issue[4]     # End time
                        )
                    
                    if start_date and end_date:
                        try:
                            record_date = datetime.strptime(issue[5], "%Y-%m-%d %H:%M:%S").date()
                            matches_date = start_date <= record_date <= end_date
                        except:
                            matches_date = False
                    elif start_date:
                        try:
                            record_date = datetime.strptime(issue[5], "%Y-%m-%d %H:%M:%S").date()
                            matches_date = record_date == start_date
                        except:
                            matches_date = False
                    # else: no date filter
                    if matches_search and matches_date:
                        filtered_issues.append(issue)
                midshift_issues = filtered_issues
            
            if midshift_issues:
                data = []
                for issue in midshift_issues:
                    _, agent, issue_type, start_time, end_time, ts = issue
                    data.append({
                        "Agent's Name": agent,
                        "Issue Type": issue_type,
                        "Start time": start_time,
                        "End Time": end_time,
                        "Reported At": ts
                    })
                
                df = pd.DataFrame(data)
                st.dataframe(df)
                csv = df.to_csv(index=False).encode('utf-8')
                # File name logic
                if start_date and end_date:
                    fname = f"midshift_issues_{start_date}_to_{end_date}.csv"
                elif start_date:
                    fname = f"midshift_issues_{start_date}.csv"
                else:
                    fname = "midshift_issues_all.csv"
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=fname,
                    mime="text/csv"
                )
                
                if 'confirm_clear_midshift_issues' not in st.session_state:
                    st.session_state.confirm_clear_midshift_issues = False
                if not st.session_state.confirm_clear_midshift_issues:
                    if st.button("Clear All Records"):
                        st.session_state.confirm_clear_midshift_issues = True
                else:
                    st.warning("⚠️ Are you sure you want to clear all mid-shift issue records? This cannot be undone!")
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button("Yes, Clear All Mid-shift Issues"):
                            clear_midshift_issues()
                            st.session_state.confirm_clear_midshift_issues = False
                            st.rerun()
                    with col2:
                        if st.button("Cancel"):
                            st.session_state.confirm_clear_midshift_issues = False
                            st.rerun()
            else:
                st.info("No mid-shift issue records found")
        else:
            # Regular users only see their own records without search
            user_issues = [issue for issue in midshift_issues if issue[1] == st.session_state.username]
            if user_issues:
                data = []
                for issue in user_issues:
                    _, agent, issue_type, start_time, end_time, ts = issue
                    data.append({
                        "Issue Type": issue_type,
                        "Start time": start_time,
                        "End Time": end_time,
                        "Reported At": ts
                    })
                
                df = pd.DataFrame(data)
                st.dataframe(df)
            else:
                st.info("You have no mid-shift issue records")

    elif st.session_state.current_section == "wfm":
        # WFM dashboard is globally controlled by the wfm_enabled flag.
        # Even when disabled for everyone else, the 'taha kirri' admin
        # can still access it for configuration and testing.
        user_is_taha_admin = (
            st.session_state.role == "admin"
            and bool(st.session_state.get("username"))
            and st.session_state.username.lower() == "taha kirri"
        )
        if is_wfm_enabled() or user_is_taha_admin:
            if st.session_state.role == "admin":
                wfm_admin_dashboard()
            else:
                wfm_agent_dashboard()
        else:
            # If somehow reached while disabled (e.g., stale session),
            # show a neutral message instead of the full UI.
            st.info("This feature is currently not available.")

    elif st.session_state.current_section == "admin" and st.session_state.role == "admin":
        if st.session_state.username.lower() in ["taha kirri", "malikay"]:
            st.subheader("🚨 System Killswitch")
            current = is_killswitch_enabled()
            status = "🔴 ACTIVE" if current else "🟢 INACTIVE"
            st.write(f"Current Status: {status}")
            
            with st.form("killswitch_form"):
                col1, col2 = st.columns(2)
                confirm_killswitch = st.checkbox("I understand and want to change the killswitch status")
                if current:
                    if col1.form_submit_button("Deactivate Killswitch"):
                        if confirm_killswitch:
                            toggle_killswitch(False)
                            st.rerun()
                        else:
                            st.warning("Please confirm by checking the checkbox.")
                else:
                    if col1.form_submit_button("Activate Killswitch"):
                        if confirm_killswitch:
                            toggle_killswitch(True)
                            st.rerun()
                        else:
                            st.warning("Please confirm by checking the checkbox.")
            
            st.markdown("---")
            
            st.subheader("💬 Chat Killswitch")
            current_chat = is_chat_killswitch_enabled()
            chat_status = "🔴 ACTIVE" if current_chat else "🟢 INACTIVE"
            st.write(f"Current Status: {chat_status}")
            
            with st.form("chat_killswitch_form"):
                col1, col2 = st.columns(2)
                confirm_chat_killswitch = st.checkbox("I understand and want to change the chat killswitch status")
                if current_chat:
                    if col1.form_submit_button("Deactivate Chat Killswitch"):
                        if confirm_chat_killswitch:
                            toggle_chat_killswitch(False)
                            st.rerun()
                        else:
                            st.warning("Please confirm by checking the checkbox.")
                else:
                    if col1.form_submit_button("Activate Chat Killswitch"):
                        if confirm_chat_killswitch:
                            toggle_chat_killswitch(True)
                            st.rerun()
                        else:
                            st.warning("Please confirm by checking the checkbox.")
            
            st.markdown("---")
        
        # Only the primary admin account ('taha kirri') can control the
        # global visibility of the WFM feature.
        if st.session_state.username.lower() == "taha kirri":
            st.subheader("👥 WFM Visibility")
            current_wfm = is_wfm_enabled()
            new_wfm = st.toggle(
                "Enable WFM for all users",
                value=current_wfm,
                key="wfm_global_toggle",
            )
            if new_wfm != current_wfm:
                if toggle_wfm(new_wfm):
                    st.success("WFM setting updated.")
                    st.rerun()

            st.markdown("---")

        st.subheader("🧹 Data Management")
        
        with st.form("data_clear_form"):
            clear_options = {
                "Requests": clear_all_requests,
                "Mistakes": clear_all_mistakes,
                "Chat Messages": clear_all_group_messages,
                "HOLD Images": clear_hold_images,
                "Late Logins": clear_late_logins,
                "Quality Issues": clear_quality_issues,
                "Mid-shift Issues": clear_midshift_issues,
                "ALL System Data": lambda: all([
                    clear_all_requests(),
                    clear_all_mistakes(),
                    clear_all_group_messages(),
                    clear_hold_images(),
                    clear_late_logins(),
                    clear_quality_issues(),
                    clear_midshift_issues()
                ])
            }
            
            # Dropdown for selecting what to clear
            selected_clear_option = st.selectbox(
                "Select Data to Clear", 
                list(clear_options.keys()),
                help="Choose the type of data you want to permanently delete"
            )
            
            # Warning based on selected option
            warning_messages = {
                "Requests": "This will permanently delete ALL requests and their comments!",
                "Mistakes": "This will permanently delete ALL mistakes!",
                "Chat Messages": "This will permanently delete ALL chat messages!",
                "HOLD Images": "This will permanently delete ALL HOLD images!",
                "Late Logins": "This will permanently delete ALL late login records!",
                "Quality Issues": "This will permanently delete ALL quality issue records!",
                "Mid-shift Issues": "This will permanently delete ALL mid-shift issue records!",
                "ALL System Data": "🚨 THIS WILL DELETE EVERYTHING IN THE SYSTEM! 🚨"
            }
            
            # Display appropriate warning
            if selected_clear_option == "ALL System Data":
                st.error(warning_messages[selected_clear_option])
            else:
                st.warning(warning_messages[selected_clear_option])
            
            # Confirmation checkbox for destructive actions
            confirm_clear = st.checkbox(f"I understand and want to clear {selected_clear_option}")
            
            # Submit button
            if st.form_submit_button("Clear Data"):
                if confirm_clear:
                    try:
                        # Call the corresponding clear function
                        if clear_options[selected_clear_option]():
                            st.success(f"{selected_clear_option} deleted successfully!")
                            st.rerun()
                        else:
                            st.error("Deletion failed. Please try again.")
                    except Exception as e:
                        st.error(f"Error during deletion: {str(e)}")
                else:
                    st.warning("Please confirm the deletion by checking the checkbox.")
        
        st.markdown("---")
        st.subheader("📝 Dropdown Options Management")
        
        # Section selector for dropdown management
        section_names = {
            "late_login": "Late Login - Reason",
            "quality_issues": "Quality Issues - Type of Issue",
            "midshift_issues": "Midshift Issues - Issue Type",
            "quality_products": "Quality Issues - Product"
        }
        
        selected_section = st.selectbox(
            "Select Section to Manage",
            list(section_names.keys()),
            format_func=lambda x: section_names[x]
        )
        
        st.write(f"**Managing: {section_names[selected_section]}**")
        
        # Add new option
        with st.form(f"add_option_{selected_section}"):
            st.write("**Add New Option**")
            new_option = st.text_input("Option Value")
            if st.form_submit_button("Add Option"):
                if new_option and new_option.strip():
                    if add_dropdown_option(selected_section, new_option.strip()):
                        st.success(f"Option '{new_option}' added successfully!")
                        st.rerun()
                    else:
                        st.error("Failed to add option.")
                else:
                    st.warning("Please enter a valid option value.")
        
        # Display and manage existing options
        st.write("**Current Options**")
        current_options = get_all_dropdown_options_with_ids(selected_section)
        
        if current_options:
            for opt_id, opt_value, display_order in current_options:
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.write(f"• {opt_value}")
                with col2:
                    if st.button("🗑️ Delete", key=f"del_{selected_section}_{opt_id}"):
                        if delete_dropdown_option(selected_section, opt_value):
                            st.success(f"Deleted '{opt_value}'")
                            st.rerun()
                        else:
                            st.error("Failed to delete option.")
        else:
            st.info("No options available for this section.")
        
        st.markdown("---")
        st.subheader("User Management")
        # --- Bulk Create Accounts (Admin section) ---
        st.markdown("---")
        st.subheader("👤 Bulk Create Accounts (Admin)")
        with st.expander("📥 Upload users file and auto-generate passwords", expanded=False):
            st.write("Accepted columns (header-insensitive): username, role, agent_id, group, department/process")
            st.write("You will get a downloadable Excel with: DB User ID, username, generated password, role, agent_id, group, department, status, note.")

            uploaded_users_admin = st.file_uploader(
                "Choose CSV or Excel file",
                type=['csv', 'xlsx', 'xls'],
                key="admin_bulk_users_upload"
            )

            def generate_password_admin(length=12):
                import secrets, string
                alphabet = string.ascii_letters + string.digits + string.punctuation
                while True:
                    pwd = ''.join(secrets.choice(alphabet) for _ in range(length))
                    if (any(c.islower() for c in pwd) and any(c.isupper() for c in pwd)
                            and any(c.isdigit() for c in pwd) and any(c in string.punctuation for c in pwd)):
                        return pwd

            def bulk_create_users_from_df_admin(df: pd.DataFrame):
                cols_map = {str(c).strip().lower(): c for c in df.columns}
                ucol = cols_map.get('username') or cols_map.get('user') or cols_map.get('name')
                rcol = cols_map.get('role')
                aidcol = cols_map.get('agent_id') or cols_map.get('agent id') or cols_map.get('agentid') or cols_map.get('id')
                gcol = cols_map.get('group') or cols_map.get('group_name')
                dcol = cols_map.get('department') or cols_map.get('process')
                # Optional: column containing break template assignments, e.g. "Template A,Template B"
                btcol = cols_map.get('break_templates')
                if not ucol:
                    return pd.DataFrame([{"id": "", "username": "", "status": "error", "note": "Missing username column"}])

                # Ensure users table has agent_id column so we can persist provided IDs
                try:
                    ensure_agent_id_column()
                except Exception:
                    pass

                # Detect users table columns
                conn = get_db_connection()
                try:
                    cur = conn.cursor()
                    cur.execute("PRAGMA table_info(users)")
                    user_cols = [row[1] for row in cur.fetchall()]
                finally:
                    conn.close()

                results = []
                for _, row in df.iterrows():
                    username = str(row.get(ucol, '')).strip()
                    role = str(row.get(rcol, 'agent')).strip().lower() if rcol else 'agent'
                    agent_id = str(row.get(aidcol, '')).strip() if aidcol else ''
                    group_name = str(row.get(gcol, '')).strip() if gcol else ''
                    department = str(row.get(dcol, '')).strip() if dcol else ''
                    # Read break_templates from the sheet if provided; keep as raw string
                    break_templates_val = ''
                    if btcol is not None:
                        bt_raw = row.get(btcol, '')
                        if isinstance(bt_raw, str):
                            break_templates_val = bt_raw.strip()
                        elif pd.notna(bt_raw):
                            break_templates_val = str(bt_raw).strip()
                    if not username:
                        results.append({"id": "", "username": username, "status": "skipped", "note": "empty username"})
                        continue
                    plain_pwd = generate_password_admin()
                    try:
                        conn = get_db_connection()
                        cur = conn.cursor()
                        hashed = hash_password(plain_pwd)
                        fields = ["username", "password", "role"]
                        values = [username, hashed, role]
                        if "agent_id" in user_cols:
                            fields.append("agent_id"); values.append(agent_id)
                        if "group_name" in user_cols:
                            fields.append("group_name"); values.append(group_name)
                        if "department" in user_cols:
                            fields.append("department"); values.append(department)
                        # Persist break_templates if the column exists on the users table
                        if "break_templates" in user_cols and break_templates_val:
                            fields.append("break_templates"); values.append(break_templates_val)
                        placeholders = ", ".join(["?"]*len(fields))
                        cur.execute(f"INSERT INTO users ({', '.join(fields)}) VALUES ({placeholders})", values)
                        conn.commit()
                        new_id = cur.lastrowid
                        results.append({
                            "id": new_id,
                            "username": username,
                            "password": plain_pwd,
                            "role": role,
                            "agent_id": agent_id,
                            "group": group_name,
                            "department": department,
                            "status": "created",
                            "note": ""
                        })
                    except sqlite3.IntegrityError:
                        # Fetch existing ID
                        try:
                            cur.execute("SELECT id FROM users WHERE username = ?", (username,))
                            exist = cur.fetchone()
                            exist_id = exist[0] if exist else ''
                        except Exception:
                            exist_id = ''
                        results.append({"id": exist_id, "username": username, "status": "exists", "note": "username already exists"})
                    except Exception as e:
                        results.append({"id": "", "username": username, "status": "error", "note": str(e)})
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
                return pd.DataFrame(results)

            if uploaded_users_admin is not None:
                try:
                    users_df = pd.read_csv(uploaded_users_admin) if uploaded_users_admin.name.endswith('.csv') else pd.read_excel(uploaded_users_admin)
                    st.write("Preview:")
                    st.dataframe(users_df.head())
                    if st.button("Create Accounts", key="admin_bulk_create_btn"):
                        result_df = bulk_create_users_from_df_admin(users_df)
                        st.success("Bulk processing finished")
                        st.dataframe(result_df, use_container_width=True)
                        excel_bytes = None
                        try:
                            out = io.BytesIO()
                            with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                                result_df.to_excel(writer, index=False, sheet_name='accounts')
                            excel_bytes = out.getvalue()
                        except Exception:
                            excel_bytes = None
                        if excel_bytes:
                            st.download_button(
                                label="📄 Download results (Excel)",
                                data=excel_bytes,
                                file_name="bulk_accounts_results.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="admin_bulk_dl_xlsx"
                            )
                        csv_bytes = result_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="⬇️ Download results (CSV)",
                            data=csv_bytes,
                            file_name="bulk_accounts_results.csv",
                            mime="text/csv",
                            key="admin_bulk_dl_csv"
                        )
                except Exception as e:
                    st.error(f"Error reading file: {e}")
        if not is_killswitch_enabled():
            # Show add user form to all admins, but with different options
            with st.form("add_user"):
                user = st.text_input("Username")
                pwd = st.text_input("Password", type="password")
                agent_id = st.text_input("Agent ID (Optional)", help="Unique agent identifier for roster management")
                # Only show role selection to taha kirri, others can only create agent accounts
                if st.session_state.username.lower() in ["taha kirri", "malikay"]:
                    role = st.selectbox("Role", ["agent", "admin", "qa"])
                else:
                    role = "agent"  # Default role for accounts created by other admins
                    st.info("Note: New accounts will be created as agent accounts.")
                # --- Group selection for all new users ---
                # Fetch all groups from users table
                all_groups = list(set([u[3] for u in get_all_users() if u[3]]))
                group_choice = None
                group_name = None
                if all_groups:
                    group_options = all_groups + ["Create new group"]
                    group_choice = st.selectbox("Assign to Group", group_options, key="add_user_group")
                    if group_choice == "Create new group":
                        group_name = st.text_input("New Group Name (required)")
                    else:
                        group_name = group_choice
                else:
                    st.warning("No groups found. Please create a group before adding users.")
                    group_choice = "Create new group"
                    group_name = st.text_input("New Group Name (required)")

                # --- Break Templates Selection for Agents ---
                selected_templates = []
                if role == "agent":
                    # Load templates from templates.json
                    templates = []
                    try:
                        with open("templates.json", "r") as f:
                            templates = list(json.load(f).keys())
                    except Exception:
                        st.warning("No break templates found. Please add templates.json.")
                    if templates:
                        selected_templates = st.multiselect(
                            "Select break templates agent can book from:",
                            templates,
                            help="Choose one or more break templates for this agent"
                        )
                    else:
                        selected_templates = []
                else:
                    selected_templates = []

                if st.form_submit_button("Add User"):
                    def is_password_complex(password):
                        if len(password) < 8:
                            return False
                        if not re.search(r"[A-Z]", password):
                            return False
                        if not re.search(r"[a-z]", password):
                            return False
                        if not re.search(r"[0-9]", password):
                            return False
                        if not re.search(r"[^A-Za-z0-9]", password):
                            return False
                        return True

                    if user and pwd and group_name:
                        if not is_password_complex(pwd):
                            st.error("Password must be at least 8 characters, include uppercase, lowercase, digit, and special character.")
                        elif group_choice == "Create new group" and not group_name:
                            st.error("Please enter a new group name.")
                        else:
                            # Pass selected_templates for agent, or empty for admin
                            result = add_user(user, pwd, role, group_name, selected_templates, agent_id.strip() if agent_id else None)
                            if result == "exists":
                                st.error("User already exists. Please choose a different username.")
                            elif result:
                                st.success("User added successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to add user. Please try again.")

                    elif not group_name:
                        st.error("Group name is required.")
        
        st.subheader("Existing Users")
        users = get_all_users()
        
        # Create tabs for different user types
        user_tabs = st.tabs(["All Users", "Admins", "Agents", "QA"])
        
        # Password reset for admin
        if st.session_state.role == "admin":
            st.write("### Reset User Password")
            with st.form("reset_password_form"):
                reset_user = st.selectbox("Select User", [u[1] for u in users], key="reset_user_select")
                new_pwd = st.text_input("New Password", type="password", key="reset_user_pwd")
                if st.form_submit_button("Reset Password"):
                    def is_password_complex(password):
                        if len(password) < 8:
                            return False
                        if not re.search(r"[A-Z]", password):
                            return False
                        if not re.search(r"[a-z]", password):
                            return False
                        if not re.search(r"[0-9]", password):
                            return False
                        if not re.search(r"[^A-Za-z0-9]", password):
                            return False
                        return True
                    if reset_user and new_pwd:
                        if reset_user.lower() in ["taha kirri", "malikay"]:
                            st.error("You cannot reset the password for this account.")
                        elif not is_password_complex(new_pwd):
                            st.error("Password must be at least 8 characters, include uppercase, lowercase, digit, and special character.")
                        else:
                            reset_password(reset_user, new_pwd)
                            st.success(f"Password reset for {reset_user}")
                            st.rerun()
        
        with user_tabs[0]:
            # All users view
            st.write("### All Users")
            
            # Create a dataframe for better display
            user_data = []
            for uid, uname, urole, gname in users:
                user_data.append({
                    "ID": uid,
                    "Username": uname,
                    "Role": urole,
                    "Group": gname
                })
            
            df = pd.DataFrame(user_data)
            st.dataframe(df, use_container_width=True)
            
            # User deletion with dropdown
            if st.session_state.username.lower() in ["taha kirri", "malikay"]:
                # Taha can delete any user
                with st.form("delete_user_form"):
                    st.write("### Delete User")
                    user_to_delete = st.selectbox(
                        "Select User to Delete",
                        [f"{user[0]} - {user[1]} ({user[2]})" for user in users],
                        key="delete_user_select"
                    )
                    
                    confirm_delete_user = st.checkbox("I understand and want to delete this user")
                    if st.form_submit_button("Delete User") and not is_killswitch_enabled():
                        if confirm_delete_user:
                            user_id = int(user_to_delete.split(' - ')[0])
                            if delete_user(user_id):
                                st.success(f"User deleted successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to delete user.")
                        else:
                            st.warning("Please confirm by checking the checkbox.")
        
        with user_tabs[1]:
            # Admins view
            admin_users = [user for user in users if user[2] == "admin"]
            st.write(f"### Admin Users ({len(admin_users)})")
            
            admin_data = []
            for uid, uname, urole, gname in admin_users:
                admin_data.append({
                    "ID": uid,
                    "Username": uname,
                    "Group": gname
                })
            
            if admin_data:
                st.dataframe(pd.DataFrame(admin_data), use_container_width=True)
            else:
                st.info("No admin users found")
        
        with user_tabs[2]:
            # Agents view
            agent_users = [user for user in users if user[2] == "agent"]
            st.write(f"### Agent Users ({len(agent_users)})")

            # Show a table including each agent's Agent ID
            if agent_users:
                agent_table = []
                for uid, uname, urole, gname in agent_users:
                    try:
                        aid_val = None
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("PRAGMA table_info(users)")
                        cols = [r[1] for r in cur.fetchall()]
                        if "agent_id" in cols:
                            cur.execute("SELECT agent_id FROM users WHERE username = ?", (uname,))
                            r = cur.fetchone()
                            aid_val = r[0] if r else None
                    except Exception:
                        aid_val = None
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
                    agent_table.append({
                        "ID": uid,
                        "Username": uname,
                        "Agent ID": aid_val or "",
                        "Group": gname or ""
                    })
                st.dataframe(pd.DataFrame(agent_table), use_container_width=True)

                # Quick assign/update Agent ID tool
                with st.expander("Assign / Update Agent ID", expanded=False):
                    target_uname = st.selectbox("Select agent user", [u[1] for u in agent_users], key="assign_agent_id_user")
                    new_aid = st.text_input("Agent ID", key="assign_agent_id_value", help="Set the unique roster Agent ID for this user")
                    if st.button("Save Agent ID", key="assign_agent_id_save"):
                        try:
                            conn = get_db_connection()
                            cur = conn.cursor()
                            cur.execute("PRAGMA table_info(users)")
                            cols = [r[1] for r in cur.fetchall()]
                            if "agent_id" not in cols:
                                st.error("The users table has no agent_id column. Please run migrations and try again.")
                            elif not new_aid.strip():
                                st.warning("Please enter a valid Agent ID.")
                            else:
                                cur.execute("UPDATE users SET agent_id = ? WHERE username = ?", (new_aid.strip(), target_uname))
                                conn.commit()
                                st.success("Agent ID saved. Reloading...")
                                st.rerun()
                        except Exception as e:
                            st.error(f"Failed to save Agent ID: {e}")
                        finally:
                            try:
                                conn.close()
                            except Exception:
                                pass

            # --- Admin: Show agent to template assignments ---
            if st.session_state.role == "admin":
                st.subheader("Agent Break Template Assignments")
                agent_templates = get_all_users(include_templates=True)
                templates_list = []
                try:
                    with open("templates.json", "r") as f:
                        templates_list = list(json.load(f).keys())
                except Exception:
                    st.warning("No break templates found. Please add templates.json.")

                # --- Refactored: Single agent dropdown ---
                agent_choices = [(u[1], u[3]) for u in agent_templates if u[2] == "agent"]
                agent_labels = [f"{name} ({group})" if group else name for name, group in agent_choices]
                agent_usernames = [name for name, _ in agent_choices]
                if not agent_labels:
                    st.info("No agents found or no agents assigned to any templates yet.")
                else:
                    selected_idx = st.selectbox("Select agent to edit templates:", options=list(range(len(agent_labels))), format_func=lambda i: agent_labels[i] if i is not None else "Select...", key="admin_agent_select")
                    if selected_idx is not None:
                        username = agent_usernames[selected_idx]
                        # Get current templates
                        agent_row = next(u for u in agent_templates if u[1] == username)
                        current_templates = [t.strip() for t in (agent_row[4] or '').split(',') if t.strip()]
                        st.write(f"**Editing templates for:** {username}")
                        new_templates = st.multiselect(
                            f"Edit templates for {username}",
                            templates_list,
                            default=current_templates,
                            key=f"edit_templates_{username}"
                        )

                        # --- Group selection for agent ---
                        all_groups = list(set([u[3] for u in get_all_users() if u[3]]))
                        group_choice = None
                        group_name = None
                        if all_groups:
                            group_options = all_groups + ["Create new group"]
                            group_choice = st.selectbox("Change Agent Group", group_options, key=f"edit_agent_group_{username}")
                            if group_choice == "Create new group":
                                group_name = st.text_input("New Group Name (required)", key=f"new_group_name_{username}")
                            else:
                                group_name = group_choice
                        else:
                            st.warning("No groups found. Please create a group before assigning.")
                            group_choice = "Create new group"
                            group_name = st.text_input("New Group Name (required)", key=f"new_group_name_{username}")

                        if st.button(f"Save for {username}", key=f"save_templates_{username}"):
                            def update_agent_templates_and_group(username, templates, group_name):
                                conn = sqlite3.connect("data/requests.db")
                                try:
                                    cursor = conn.cursor()
                                    templates_str = ','.join(templates)
                                    cursor.execute(
                                        "UPDATE users SET break_templates = ?, group_name = ? WHERE username = ?",
                                        (templates_str, group_name, username)
                                    )
                                    conn.commit()
                                    return True
                                finally:
                                    conn.close()
                            if group_choice == "Create new group" and not group_name:
                                st.error("Please enter a new group name.")
                            else:
                                update_agent_templates_and_group(username, new_templates, group_name)
                                st.success(f"Templates and group updated for {username}!")
                                st.rerun()


            
            agent_data = []
            for uid, uname, urole, gname in agent_users:
                agent_data.append({
                    "ID": uid,
                    "Username": uname,
                    "Group": gname
                })
            
            if agent_data:
                st.dataframe(pd.DataFrame(agent_data), use_container_width=True)
                # Only admins can delete agent accounts
                with st.form("delete_agent_form"):
                    st.write("### Delete Agent")
                    agent_to_delete = st.selectbox(
                        "Select Agent to Delete",
                        [f"{user[0]} - {user[1]}" for user in agent_users],
                        key="delete_agent_select"
                    )
                    
                    if st.form_submit_button("Delete Agent") and not is_killswitch_enabled():
                        agent_id = int(agent_to_delete.split(' - ')[0])
                        if delete_user(agent_id):
                            st.success(f"Agent deleted successfully!")
                            st.rerun()
            else:
                st.info("No agent users found")
# The old agent group change UI has been removed; use the unified edit panel above.
        
        with user_tabs[3]:
            # QA view
            if not is_killswitch_enabled():
                qa_users = [user for user in users if user[2] == "qa"]
                st.write(f"### QA Users ({len(qa_users)})")
                
                qa_data = []
                for uid, uname, urole, gname in qa_users:
                    qa_data.append({
                        "ID": uid,
                        "Username": uname,
                        "Group": gname
                    })
                
                if qa_data:
                    st.dataframe(pd.DataFrame(qa_data), use_container_width=True)
                else:
                    st.info("No QA users found")
            else:
                st.error("System is currently locked. Please contact the developer.")

    elif st.session_state.current_section == "breaks":
        render_notification_permission_banner("breaks-permission-banner")
        if st.session_state.role == "admin":
            admin_break_dashboard()
        else:
            agent_break_dashboard()
    
    elif st.session_state.current_section == "wfm":
        if not is_killswitch_enabled():
            if st.session_state.role == "admin":
                wfm_admin_dashboard()
            else:
                wfm_agent_dashboard()
        else:
            st.error("System is currently locked. Access to WFM is disabled.")
    
    elif st.session_state.current_section == "fancy_number":
        st.title("💎 Lycamobile Fancy Number Checker")
        st.subheader("Official Policy: Analyzes last 6 digits only for qualifying patterns")

        phone_input = st.text_input("Enter Phone Number", placeholder="e.g., 1555123456 or 44207123456")

        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("🔍 Check Number"):
                if not phone_input:
                    st.warning("Please enter a phone number")
                else:
                    is_fancy, pattern = is_fancy_number(phone_input)
                    clean_number = re.sub(r'\D', '', phone_input)
                    
                    # Extract last 6 digits for display
                    last_six = clean_number[-6:] if len(clean_number) >= 6 else clean_number
                    formatted_num = f"{last_six[:3]}-{last_six[3:]}" if len(last_six) == 6 else last_six

                    if is_fancy:
                        st.markdown(f"""
                        <div class="result-box fancy-result">
                            <h3><span class="fancy-number">✨ {formatted_num} ✨</span></h3>
                            <p>FANCY NUMBER DETECTED!</p>
                            <p><strong>Pattern:</strong> {pattern}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div class="result-box normal-result">
                            <h3><span class="normal-number">{formatted_num}</span></h3>
                            <p>Standard phone number</p>
                            <p><strong>Reason:</strong> {pattern}</p>
                        </div>
                        """, unsafe_allow_html=True)

        with col2:
            st.markdown("""
            ### Lycamobile Fancy Number Policy
            **Qualifying Patterns (last 6 digits only):**
            
            #### 6-Digit Patterns
            - 123456 (ascending)
            - 987654 (descending)
            - 666666 (repeating)
            - 100001 (palindrome)
            
            #### 3-Digit Patterns  
            - 444 555 (double triplets)
            - 121 122 (similar triplets)
            - 786 786 (repeating triplets)
            - 457 456 (nearly sequential)
            
            #### 2-Digit Patterns
            - 11 12 13 (incremental)
            - 20 20 20 (repeating)
            - 01 01 01 (alternating)
            - 32 42 52 (stepping)
            
            #### Exceptional Cases
            - Ending with 123/555/777/999
            """)

        debug_mode = st.checkbox("Show test cases", False)
        if debug_mode:
            st.subheader("Test Cases")
            test_numbers = [
                ("16109055580", False),  # 055580 → No pattern ✗
                ("123456", True),       # 6-digit ascending ✓
                ("444555", True),       # Double triplets ✓
                ("121122", True),       # Similar triplets ✓ 
                ("111213", True),       # Incremental pairs ✓
                ("202020", True),       # Repeating pairs ✓
                ("010101", True),       # Alternating pairs ✓
                ("324252", True),       # Stepping pairs ✓
                ("7900000123", True),   # Ends with 123 ✓
                ("123458", False),      # No pattern ✗
                ("112233", False),      # Not in our strict rules ✗
                ("555555", True)        # 6 identical digits ✓
            ]
            
            for number, expected in test_numbers:
                is_fancy, pattern = is_fancy_number(number)
                result = "PASS" if is_fancy == expected else "FAIL"
                color = "green" if result == "PASS" else "red"
                st.write(f"<span style='color:{color}'>{number[-6:]}: {result} ({pattern})</span>", unsafe_allow_html=True)

def get_new_messages(last_check_time, group_name=None):
    """Get new messages since last check for the specified group only."""
    # Never allow None, empty, or blank group_name to fetch all messages
    if group_name is None or str(group_name).strip() == "":
        return []
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, sender, message, timestamp, mentions, group_name
            FROM group_messages
            WHERE timestamp > ? AND group_name = ?
            ORDER BY timestamp DESC
        """, (last_check_time, group_name))
        return cursor.fetchall()
    finally:
        conn.close()

def handle_message_check():
    if not st.session_state.authenticated:
        return {"new_messages": False, "messages": []}

    current_time = datetime.now()
    if 'last_message_check' not in st.session_state:
        st.session_state.last_message_check = current_time

    # Determine group_name for this user (agent or admin)
    if st.session_state.role == "admin":
        group_name = st.session_state.get("admin_chat_group")
    else:
        group_name = getattr(st.session_state, "group_name", None)

    new_messages = get_new_messages(
        st.session_state.last_message_check.strftime("%Y-%m-%d %H:%M:%S"),
        group_name
    )
    st.session_state.last_message_check = current_time

    if new_messages:
        messages_data = []
        for msg in new_messages:
            # Now msg includes group_name as last field
            msg_id, sender, message, ts, mentions, _group_name = msg
            if sender != st.session_state.username:  # Don't notify about own messages
                mentions_list = mentions.split(',') if mentions else []
                if st.session_state.username in mentions_list:
                    message = f"@{st.session_state.username} {message}"
                messages_data.append({
                    "sender": sender,
                    "message": message
                })
        return {"new_messages": bool(messages_data), "messages": messages_data}
    return {"new_messages": False, "messages": []}

def convert_to_casablanca_date(date_str):
    """Convert a date string to Casablanca timezone"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        morocco_tz = pytz.timezone('Africa/Casablanca')
        return pytz.UTC.localize(dt).astimezone(morocco_tz).date()
    except:
        return None

def get_date_range_casablanca(date):
    """Get start and end of day in Casablanca time"""
    morocco_tz = pytz.timezone('Africa/Casablanca')
    start = morocco_tz.localize(datetime.combine(date, time.min))
    end = morocco_tz.localize(datetime.combine(date, time.max))
    return start, end

if __name__ == "__main__":
    # Initialize color mode if not set
    if 'color_mode' not in st.session_state:
        st.session_state.color_mode = 'dark'
        
    inject_custom_css()
    
    # Add route for message checking
    if st.query_params.get("check_messages"):
        st.json(handle_message_check())
        st.stop()
    
    st.write("Lyca Management System")


