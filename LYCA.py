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
import base64

# ─────────────────────────────────────────────
# AUTO-AUTHENTICATE (skip login for preview)
# ─────────────────────────────────────────────
os.makedirs("data", exist_ok=True)

if "authenticated" not in st.session_state:
    st.session_state.update({
        "authenticated": True,
        "role": "admin",
        "username": "taha kirri",
        "current_section": "dashboard",
        "last_request_count": 0,
        "last_mistake_count": 0,
        "last_message_ids": [],
        "color_mode": "light",
        "notification_settings": {
            "chat_notifications": True,
            "request_notifications": True,
            "break_notifications": True
        }
    })

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def get_db_connection():
    return sqlite3.connect("data/requests.db", timeout=5.0)

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def get_casablanca_time():
    morocco_tz = pytz.timezone('Africa/Casablanca')
    return datetime.now(morocco_tz).strftime("%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────
# DB SCHEMA INIT
# ─────────────────────────────────────────────
def init_db():
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password TEXT,
            role TEXT CHECK(role IN ('agent','admin','manager','qa','wfm')),
            group_name TEXT, agent_id TEXT, break_templates TEXT
        )""")
        for col in ['group_name','agent_id','break_templates']:
            try: c.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
            except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_name TEXT, request_type TEXT,
            identifier TEXT, comment TEXT, timestamp TEXT, completed INTEGER DEFAULT 0, group_name TEXT
        )""")
        try: c.execute("ALTER TABLE requests ADD COLUMN group_name TEXT")
        except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS mistakes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, team_leader TEXT, agent_name TEXT,
            ticket_id TEXT, error_description TEXT, timestamp TEXT, product TEXT
        )""")
        try: c.execute("ALTER TABLE mistakes ADD COLUMN product TEXT")
        except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS group_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT, message TEXT, timestamp TEXT,
            mentions TEXT, group_name TEXT, reactions TEXT DEFAULT '{}', is_deleted INTEGER DEFAULT 0,
            edited_at TEXT, edited_by TEXT, deleted_by TEXT, deleted_at TEXT,
            pinned INTEGER DEFAULT 0, pinned_by TEXT, pinned_at TEXT
        )""")
        for col in ['group_name','reactions','is_deleted','edited_at','edited_by','deleted_by','deleted_at','pinned','pinned_by','pinned_at']:
            try: c.execute(f"ALTER TABLE group_messages ADD COLUMN {col} TEXT DEFAULT '{{}}' " if col=='reactions' else f"ALTER TABLE group_messages ADD COLUMN {col} TEXT")
            except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS hold_tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT, uploader TEXT, table_data TEXT, timestamp TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS system_settings (
            id INTEGER PRIMARY KEY, killswitch_enabled INTEGER DEFAULT 0,
            chat_killswitch_enabled INTEGER DEFAULT 0, wfm_enabled INTEGER DEFAULT 1,
            chat_enabled INTEGER DEFAULT 1, late_login_enabled INTEGER DEFAULT 1,
            midshift_enabled INTEGER DEFAULT 1, quality_enabled INTEGER DEFAULT 1,
            fancy_number_enabled INTEGER DEFAULT 1
        )""")
        for flag in ['wfm_enabled','chat_enabled','late_login_enabled','midshift_enabled','quality_enabled','fancy_number_enabled']:
            try: c.execute(f"ALTER TABLE system_settings ADD COLUMN {flag} INTEGER DEFAULT 1")
            except: pass
        c.execute("INSERT OR IGNORE INTO system_settings (id) VALUES (1)")
        c.execute("""CREATE TABLE IF NOT EXISTS roster (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL, name TEXT NOT NULL,
            department TEXT, shift TEXT, schedule TEXT, process TEXT,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(agent_id)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS roster_next (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL, name TEXT NOT NULL,
            department TEXT, shift TEXT, schedule TEXT, process TEXT,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(agent_id)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS swap_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, requester_id TEXT NOT NULL, target_id TEXT NOT NULL,
            date DATE NOT NULL, requester_date DATE, target_date DATE, reason TEXT,
            status TEXT DEFAULT 'pending', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            approved_by TEXT, approved_at TIMESTAMP
        )""")
        for col in ['requester_date','target_date']:
            try: c.execute(f"ALTER TABLE swap_requests ADD COLUMN {col} DATE")
            except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS holiday_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT, username TEXT,
            start_date TEXT, end_date TEXT, days INTEGER, reason TEXT, status TEXT,
            requested_at TEXT, decided_by TEXT, decided_at TEXT, request_type TEXT DEFAULT 'normal'
        )""")
        try: c.execute("ALTER TABLE holiday_requests ADD COLUMN request_type TEXT DEFAULT 'normal'")
        except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS late_logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_name TEXT, presence_time TEXT,
            login_time TEXT, reason TEXT, timestamp TEXT, status TEXT DEFAULT 'pending',
            approved_by TEXT, approved_at TEXT
        )""")
        for col in ['status','approved_by','approved_at']:
            try: c.execute(f"ALTER TABLE late_logins ADD COLUMN {col} TEXT")
            except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS quality_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_name TEXT, issue_type TEXT,
            timing TEXT, mobile_number TEXT, product TEXT, timestamp TEXT,
            status TEXT DEFAULT 'pending', approved_by TEXT, approved_at TEXT
        )""")
        for col in ['status','approved_by','approved_at','product']:
            try: c.execute(f"ALTER TABLE quality_issues ADD COLUMN {col} TEXT")
            except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS midshift_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_name TEXT, issue_type TEXT,
            start_time TEXT, end_time TEXT, timestamp TEXT, status TEXT DEFAULT 'pending',
            approved_by TEXT, approved_at TEXT
        )""")
        for col in ['status','approved_by','approved_at']:
            try: c.execute(f"ALTER TABLE midshift_issues ADD COLUMN {col} TEXT")
            except: pass
        c.execute("""CREATE TABLE IF NOT EXISTS request_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER, user TEXT,
            comment TEXT, timestamp TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS dropdown_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT, section TEXT NOT NULL,
            option_value TEXT NOT NULL, display_order INTEGER DEFAULT 0
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS user_notification_settings (
            username TEXT PRIMARY KEY, chat_notifications INTEGER DEFAULT 1,
            request_notifications INTEGER DEFAULT 1, break_notifications INTEGER DEFAULT 1
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS vip_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT, message TEXT,
            timestamp TEXT, mentions TEXT
        )""")
        # Default admin
        c.execute("INSERT OR IGNORE INTO users (username,password,role) VALUES (?,?,?)",
                  ("taha kirri", hash_password("Cursed@99"), "admin"))
        c.execute("INSERT OR IGNORE INTO users (username,password,role) VALUES (?,?,?)",
                  ("agent", hash_password("Agent@3356"), "agent"))
        c.execute("INSERT OR IGNORE INTO user_notification_settings (username) SELECT username FROM users")
        # Default dropdowns
        c.execute("SELECT COUNT(*) FROM dropdown_options")
        if c.fetchone()[0] == 0:
            for section, opts in [
                ("late_login", ["Disconnected RC","Frozen Ring","PC ISSUE","RC Extension issue","Ring Central issue","Windows issue"]),
                ("quality_issues", ["Audio Issue","Call Drop From Rc","Call Frozen","CRM Issue","Hold Frozen"]),
                ("midshift_issues", ["Extension issue","Windows Issue","PC Issue","Disconnected RC","Frozen Ring"])
            ]:
                for i, o in enumerate(opts):
                    c.execute("INSERT INTO dropdown_options (section,option_value,display_order) VALUES (?,?,?)", (section,o,i))
        conn.commit()
    finally:
        conn.close()

init_db()

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────
def current_username(): return (st.session_state.get("username") or "").strip()
def current_role(): return st.session_state.get("role")
def is_admin(): return current_role() == "admin"
def is_manager(): return current_role() == "manager"
def has_manager_level_access():
    return current_role() in ["admin","manager"] or current_username().lower() == "malikay"
def can_manage_holidays(): return is_manager() or current_username().lower() in ["taha kirri","malikay"]

def is_killswitch_enabled():
    try:
        c=get_db_connection().cursor(); c.execute("SELECT killswitch_enabled FROM system_settings WHERE id=1")
        r=c.fetchone(); return bool(r[0]) if r else False
    except: return False

def is_chat_killswitch_enabled():
    try:
        c=get_db_connection().cursor(); c.execute("SELECT chat_killswitch_enabled FROM system_settings WHERE id=1")
        r=c.fetchone(); return bool(r[0]) if r else False
    except: return False

def _flag_get(flag):
    try:
        c=get_db_connection().cursor(); c.execute(f"SELECT {flag} FROM system_settings WHERE id=1")
        r=c.fetchone(); return bool(r[0]) if r is not None else True
    except: return True

def _flag_set(flag, val):
    try:
        c=get_db_connection().cursor(); c.execute(f"UPDATE system_settings SET {flag}=? WHERE id=1",(1 if val else 0,))
        c.connection.commit(); return True
    except: return False

def toggle_killswitch(e): return _flag_set('killswitch_enabled',e)
def toggle_chat_killswitch(e): return _flag_set('chat_killswitch_enabled',e)

def is_wfm_enabled(): return _flag_get('wfm_enabled')
def is_chat_enabled(): return _flag_get('chat_enabled')
def is_late_login_enabled(): return _flag_get('late_login_enabled')
def is_midshift_enabled(): return _flag_get('midshift_enabled')
def is_quality_enabled(): return _flag_get('quality_enabled')
def is_fancy_number_enabled(): return _flag_get('fancy_number_enabled')

def toggle_wfm(e): return _flag_set('wfm_enabled',e)
def toggle_chat_enabled(e): return _flag_set('chat_enabled',e)
def toggle_late_login_enabled(e): return _flag_set('late_login_enabled',e)
def toggle_midshift_enabled(e): return _flag_set('midshift_enabled',e)
def toggle_quality_enabled(e): return _flag_set('quality_enabled',e)
def toggle_fancy_number_enabled(e): return _flag_set('fancy_number_enabled',e)

@st.cache_data(ttl=15)
def get_requests():
    c=get_db_connection().cursor(); c.execute("SELECT * FROM requests ORDER BY timestamp DESC"); return c.fetchall()

def search_requests(q):
    q=f"%{q.lower()}%"
    c=get_db_connection().cursor()
    c.execute("SELECT * FROM requests WHERE LOWER(agent_name) LIKE ? OR LOWER(request_type) LIKE ? OR LOWER(identifier) LIKE ? OR LOWER(comment) LIKE ? ORDER BY timestamp DESC",(q,q,q,q))
    return c.fetchall()

def add_request(agent,request_type,identifier,comment,group_name=None):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try:
        c=conn.cursor(); ts=get_casablanca_time()
        if group_name: c.execute("INSERT INTO requests (agent_name,request_type,identifier,comment,timestamp,group_name) VALUES (?,?,?,?,?,?)",(agent,request_type,identifier,comment,ts,group_name))
        else: c.execute("INSERT INTO requests (agent_name,request_type,identifier,comment,timestamp) VALUES (?,?,?,?,?)",(agent,request_type,identifier,comment,ts))
        rid=c.lastrowid
        c.execute("INSERT INTO request_comments (request_id,user,comment,timestamp) VALUES (?,?,?,?)",(rid,agent,f"Request created: {comment}",ts))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def update_request_status(rid,completed):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE requests SET completed=? WHERE id=?",(1 if completed else 0,rid)); conn.commit(); return True
    except: return False
    finally: conn.close()

def add_request_comment(rid,user,comment):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("INSERT INTO request_comments (request_id,user,comment,timestamp) VALUES (?,?,?,?)",(rid,user,comment,get_casablanca_time())); conn.commit(); return True
    except: return False
    finally: conn.close()

def get_request_comments(rid):
    c=get_db_connection().cursor(); c.execute("SELECT * FROM request_comments WHERE request_id=? ORDER BY timestamp ASC",(rid,)); return c.fetchall()

@st.cache_data(ttl=15)
def get_mistakes():
    c=get_db_connection().cursor(); c.execute("SELECT id,team_leader,product,error_description,timestamp FROM mistakes ORDER BY timestamp DESC"); return c.fetchall()

def add_mistake(tl,product,desc):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("INSERT INTO mistakes (team_leader,agent_name,ticket_id,error_description,timestamp) VALUES (?,?,?,?,?)",(tl,product,'',desc,get_casablanca_time())); conn.commit(); return True
    except: return False
    finally: conn.close()

def send_group_message(sender,message,group_name=None):
    if is_killswitch_enabled() or is_chat_killswitch_enabled(): return False
    conn=get_db_connection()
    try:
        c=conn.cursor(); mentions=re.findall(r'@(\w+)',message)
        if group_name: c.execute("INSERT INTO group_messages (sender,message,timestamp,mentions,group_name,reactions) VALUES (?,?,?,?,?,?)",(sender,message,get_casablanca_time(),','.join(mentions),group_name,json.dumps({})))
        else: c.execute("INSERT INTO group_messages (sender,message,timestamp,mentions,reactions) VALUES (?,?,?,?,?)",(sender,message,get_casablanca_time(),','.join(mentions),json.dumps({})))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def get_group_messages(group_name=None):
    if not group_name or not str(group_name).strip(): return []
    c=get_db_connection().cursor(); c.execute("SELECT * FROM group_messages WHERE group_name=? ORDER BY timestamp DESC LIMIT 50",(group_name,))
    rows=c.fetchall(); msgs=[]
    cols=[d[0] for d in c.description]
    for row in rows:
        m=dict(zip(cols,row))
        try: m['reactions']=json.loads(m.get('reactions','{}') or '{}')
        except: m['reactions']={}
        msgs.append(m)
    return msgs

@st.cache_data(ttl=30)
def get_all_users(include_templates=False):
    c=get_db_connection().cursor()
    if include_templates:
        c.execute("PRAGMA table_info(users)"); cols=[r[1] for r in c.fetchall()]
        if "break_templates" in cols: c.execute("SELECT id,username,role,group_name,break_templates FROM users")
        else: c.execute("SELECT id,username,role,group_name FROM users")
    else: c.execute("SELECT id,username,role,group_name FROM users")
    return c.fetchall()

def add_user(username,password,role,group_name=None,break_templates=None,agent_id=None):
    if is_killswitch_enabled(): return False
    if len(password)<8 or not re.search(r"[A-Z]",password) or not re.search(r"[a-z]",password) or not re.search(r"[0-9]",password) or not re.search(r"[^A-Za-z0-9]",password):
        st.error("Password must be 8+ chars with uppercase, lowercase, digit, and special character."); return False
    conn=get_db_connection()
    try:
        c=conn.cursor(); c.execute("PRAGMA table_info(users)"); ecols=[r[1] for r in c.fetchall()]
        fields=["username","password","role"]; vals=[username,hash_password(password),role]
        if "group_name" in ecols and group_name: fields.append("group_name"); vals.append(group_name)
        if "agent_id" in ecols and agent_id: fields.append("agent_id"); vals.append(agent_id)
        if "break_templates" in ecols and break_templates: fields.append("break_templates"); vals.append(','.join(break_templates) if isinstance(break_templates,list) else str(break_templates))
        ph=','.join(['?']*len(vals))
        c.execute(f"INSERT INTO users ({','.join(fields)}) VALUES ({ph})",vals); conn.commit(); return True
    except sqlite3.IntegrityError:
        if "unique" in str(sqlite3.IntegrityError).lower(): return "exists"
        return False
    except: return False
    finally: conn.close()

def delete_user(uid):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM users WHERE id=?",(uid,)); conn.commit(); return True
    except: return False
    finally: conn.close()

def get_dropdown_options(section):
    c=get_db_connection().cursor(); c.execute("SELECT option_value FROM dropdown_options WHERE section=? ORDER BY display_order,option_value",(section,)); return [r[0] for r in c.fetchall()]

def add_dropdown_option(section,val):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try:
        c=conn.cursor(); c.execute("SELECT MAX(display_order) FROM dropdown_options WHERE section=?",(section,)); mx=c.fetchone()[0]
        c.execute("INSERT INTO dropdown_options (section,option_value,display_order) VALUES (?,?,?)",(section,val,(mx+1) if mx is not None else 0))
        conn.commit(); return True
    except: return False
    finally: conn.close()

def delete_dropdown_option(section,val):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM dropdown_options WHERE section=? AND option_value=?",(section,val)); conn.commit(); return True
    except: return False
    finally: conn.close()

def get_all_dropdown_options_with_ids(section):
    c=get_db_connection().cursor(); c.execute("SELECT id,option_value,display_order FROM dropdown_options WHERE section=? ORDER BY display_order,option_value",(section,)); return c.fetchall()

def clear_all_requests():
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM requests"); c.execute("DELETE FROM request_comments"); conn.commit(); return True
    except: return False
    finally: conn.close()

def clear_all_mistakes():
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM mistakes"); conn.commit(); return True
    except: return False
    finally: conn.close()

def clear_all_group_messages():
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM group_messages"); conn.commit(); return True
    except: return False
    finally: conn.close()

# Late logins
def add_late_login(agent,presence,login,reason):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("INSERT INTO late_logins (agent_name,presence_time,login_time,reason,timestamp,status) VALUES (?,?,?,?,?,?)",(agent,presence,login,reason,get_casablanca_time(),'pending')); conn.commit(); return True
    except: return False
    finally: conn.close()

def approve_late_login(eid,admin):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE late_logins SET status='approved',approved_by=?,approved_at=? WHERE id=?",(admin,get_casablanca_time(),eid)); conn.commit(); return True
    except: return False
    finally: conn.close()

def reject_late_login(eid,admin):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE late_logins SET status='rejected',approved_by=?,approved_at=? WHERE id=?",(admin,get_casablanca_time(),eid)); conn.commit(); return True
    except: return False
    finally: conn.close()

@st.cache_data(ttl=30)
def get_late_logins():
    c=get_db_connection().cursor(); c.execute("SELECT * FROM late_logins ORDER BY timestamp DESC"); return c.fetchall()

def clear_late_logins():
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM late_logins"); conn.commit(); return True
    except: return False
    finally: conn.close()

# Quality issues
def add_quality_issue(agent,issue_type,timing,mobile,product):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("INSERT INTO quality_issues (agent_name,issue_type,timing,mobile_number,product,timestamp,status) VALUES (?,?,?,?,?,?,?)",(agent,issue_type,timing,mobile,product,get_casablanca_time(),'pending')); conn.commit(); return True
    except: return False
    finally: conn.close()

def approve_quality_issue(eid,admin):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE quality_issues SET status='approved',approved_by=?,approved_at=? WHERE id=?",(admin,get_casablanca_time(),eid)); conn.commit(); return True
    except: return False
    finally: conn.close()

def reject_quality_issue(eid,admin):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE quality_issues SET status='rejected',approved_by=?,approved_at=? WHERE id=?",(admin,get_casablanca_time(),eid)); conn.commit(); return True
    except: return False
    finally: conn.close()

@st.cache_data(ttl=30)
def get_quality_issues():
    c=get_db_connection().cursor(); c.execute("SELECT * FROM quality_issues ORDER BY timestamp DESC"); return c.fetchall()

def clear_quality_issues():
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM quality_issues"); conn.commit(); return True
    except: return False
    finally: conn.close()

# Midshift
def add_midshift_issue(agent,issue_type,start,end):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("INSERT INTO midshift_issues (agent_name,issue_type,start_time,end_time,timestamp,status) VALUES (?,?,?,?,?,?)",(agent,issue_type,start,end,get_casablanca_time(),'pending')); conn.commit(); return True
    except: return False
    finally: conn.close()

def approve_midshift_issue(eid,admin):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE midshift_issues SET status='approved',approved_by=?,approved_at=? WHERE id=?",(admin,get_casablanca_time(),eid)); conn.commit(); return True
    except: return False
    finally: conn.close()

def reject_midshift_issue(eid,admin):
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE midshift_issues SET status='rejected',approved_by=?,approved_at=? WHERE id=?",(admin,get_casablanca_time(),eid)); conn.commit(); return True
    except: return False
    finally: conn.close()

@st.cache_data(ttl=30)
def get_midshift_issues():
    c=get_db_connection().cursor(); c.execute("SELECT * FROM midshift_issues ORDER BY timestamp DESC"); return c.fetchall()

def clear_midshift_issues():
    if is_killswitch_enabled(): return False
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM midshift_issues"); conn.commit(); return True
    except: return False
    finally: conn.close()

# HOLD tables
def add_hold_table(uploader,table_data):
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM hold_tables"); c.execute("INSERT INTO hold_tables (uploader,table_data,timestamp) VALUES (?,?,?)",(uploader,table_data,get_casablanca_time())); conn.commit(); return True
    except: return False
    finally: conn.close()

def get_hold_tables():
    c=get_db_connection().cursor(); c.execute("SELECT id,uploader,table_data,timestamp FROM hold_tables ORDER BY id DESC LIMIT 1"); return c.fetchall()

def clear_hold_tables():
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("DELETE FROM hold_tables"); conn.commit(); return True
    except: return False
    finally: conn.close()

# Holidays
def create_holiday_request(agent_id,username,start,end,days,reason,rtype="normal"):
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("INSERT INTO holiday_requests (agent_id,username,start_date,end_date,days,reason,status,requested_at,request_type) VALUES (?,?,?,?,?,?,?,?)",(str(agent_id),username,start,end,int(days),reason,'pending',get_casablanca_time(),rtype)); conn.commit(); return True
    except: return False
    finally: conn.close()

def get_holiday_requests(status=None,rtype=None):
    c=get_db_connection().cursor(); q="SELECT * FROM holiday_requests"; p=[]; cl=[]
    if status is not None: cl.append("status=?"); p.append(status)
    if rtype is not None: cl.append("request_type=?"); p.append(rtype)
    if cl: q+=" WHERE "+" AND ".join(cl)
    q+=" ORDER BY requested_at DESC"; c.execute(q,tuple(p)); return c.fetchall()

def update_holiday_request_status(rid,status,decided_by):
    conn=get_db_connection()
    try: c=conn.cursor(); c.execute("UPDATE holiday_requests SET status=?,decided_by=?,decided_at=? WHERE id=?",(status,decided_by,get_casablanca_time(),rid)); conn.commit(); return c.rowcount>0
    except: return False
    finally: conn.close()

# Fancy number
def is_fancy_number(phone):
    clean=re.sub(r'\D','',phone)
    if len(clean)<6: return False,"Number too short"
    last_six=clean[-6:]; last_three=clean[-3:]; patterns=[]
    if clean=="13322866688": patterns.append("Special VIP number")
    if len(last_six)==6 and last_six[0]==last_six[5] and last_six[1]==last_six[2]==last_six[3] and last_six[4]==last_six[0] and last_six[0]!=last_six[1]: patterns.append("ABBBAA pattern")
    if len(set(last_six))==1: patterns.append("6 identical digits")
    def is_seq(d,s=1):
        try: return all(int(d[i])==int(d[i-1])+s for i in range(1,len(d)))
        except: return False
    if is_seq(last_six,1): patterns.append("6-digit ascending sequence")
    if is_seq(last_six,-1): patterns.append("6-digit descending sequence")
    if last_six==last_six[::-1]: patterns.append("6-digit palindrome")
    t1,t2=last_six[:3],last_six[3:]
    if len(set(t1))==1 and len(set(t2))==1 and t1!=t2: patterns.append("Double triplets")
    if t1==t2: patterns.append("Repeating triplets")
    if abs(int(t1)-int(t2))==1: patterns.append("Nearly sequential triplets")
    pairs=[last_six[i:i+2] for i in range(0,5)]
    try:
        if all(int(pairs[i])==int(pairs[i-1])+1 for i in range(1,len(pairs))): patterns.append("Incremental pairs")
        if pairs[0]==pairs[2]==pairs[4] and pairs[1]==pairs[3] and pairs[0]!=pairs[1]: patterns.append("Repeating/Alternating pairs")
    except: pass
    if last_three in ['123','555','777','999']: patterns.append(f"Exceptional case ({last_three})")
    valid=[p for p in patterns]
    return bool(valid), ", ".join(valid) if valid else "No qualifying fancy pattern"

# Notification settings
def get_user_notification_settings(username):
    conn=get_db_connection()
    try:
        c=conn.cursor(); c.execute("INSERT OR IGNORE INTO user_notification_settings (username) VALUES (?)",(username,)); conn.commit()
        c.execute("SELECT chat_notifications,request_notifications,break_notifications FROM user_notification_settings WHERE username=?",(username,))
        r=c.fetchone()
        if not r: return {"chat_notifications":True,"request_notifications":True,"break_notifications":True}
        return {"chat_notifications":bool(r[0]),"request_notifications":bool(r[1]),"break_notifications":bool(r[2])}
    except: return {"chat_notifications":True,"request_notifications":True,"break_notifications":True}
    finally: conn.close()

def update_user_notification_settings(username,chat=None,request=None,breaks=None):
    conn=get_db_connection()
    try:
        c=conn.cursor(); cur=get_user_notification_settings(username)
        cv=chat if chat is not None else cur["chat_notifications"]; rv=request if request is not None else cur["request_notifications"]; bv=breaks if breaks is not None else cur["break_notifications"]
        c.execute("UPDATE user_notification_settings SET chat_notifications=?,request_notifications=?,break_notifications=? WHERE username=?",(1 if cv else 0,1 if rv else 0,1 if bv else 0,username))
        conn.commit(); return True
    except: return False
    finally: conn.close()

# ─────────────────────────────────────────────
# PAGE CONFIG & MODERN CSS
# ─────────────────────────────────────────────
st.set_page_config(page_title="Lyca Management System", page_icon="◆", layout="wide", initial_sidebar_state="expanded")

MODERN_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

:root {
    --primary: #6366f1;
    --primary-hover: #4f46e5;
    --primary-light: #eef2ff;
    --primary-50: #eef2ff;
    --primary-100: #e0e7ff;
    --primary-600: #4f46e5;
    --surface: #ffffff;
    --surface-raised: #f8fafc;
    --surface-overlay: #f1f5f9;
    --border: #e2e8f0;
    --border-light: #f1f5f9;
    --text-primary: #0f172a;
    --text-secondary: #475569;
    --text-tertiary: #94a3b8;
    --success: #10b981;
    --success-bg: #ecfdf5;
    --warning: #f59e0b;
    --warning-bg: #fffbeb;
    --error: #ef4444;
    --error-bg: #fef2f2;
    --info: #3b82f6;
    --info-bg: #eff6ff;
    --radius-sm: 8px;
    --radius-md: 12px;
    --radius-lg: 16px;
    --radius-xl: 20px;
    --shadow-sm: 0 1px 2px rgba(0,0,0,0.04);
    --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.06), 0 2px 4px -2px rgba(0,0,0,0.04);
    --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.06), 0 4px 6px -4px rgba(0,0,0,0.04);
    --shadow-xl: 0 20px 25px -5px rgba(0,0,0,0.08), 0 8px 10px -6px rgba(0,0,0,0.04);
}

* { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important; }

.stApp { background: var(--surface-raised) !important; }

/* ─── SIDEBAR ─── */
[data-testid="stSidebar"] > div:first-child {
    background: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
    padding: 1.5rem 1rem !important;
}
[data-testid="stSidebar"] .stMarkdown { color: var(--text-primary) !important; }

/* ─── NAV BUTTONS ─── */
[data-testid="stSidebar"] button[kind="secondary"],
.nav-btn {
    display: flex !important; align-items: center !important; gap: 0.625rem !important;
    width: 100% !important; padding: 0.625rem 0.875rem !important;
    border: none !important; border-radius: var(--radius-sm) !important;
    background: transparent !important; color: var(--text-secondary) !important;
    font-size: 0.875rem !important; font-weight: 500 !important;
    transition: all 0.15s ease !important; text-align: left !important;
    margin: 0 !important; white-space: nowrap !important;
}
[data-testid="stSidebar"] button[kind="secondary"]:hover,
.nav-btn:hover {
    background: var(--primary-50) !important; color: var(--primary) !important;
}
[data-testid="stSidebar"] button[kind="secondary"]:active { transform: scale(0.98); }

/* ─── MAIN CONTAINER ─── */
.main-container {
    max-width: 1200px; margin: 0 auto; padding: 2rem 1.5rem;
}
.block-container { padding: 2rem 2.5rem !important; max-width: 1200px !important; }

/* ─── HEADINGS ─── */
h1 { color: var(--text-primary) !important; font-weight: 700 !important; font-size: 1.75rem !important; letter-spacing: -0.025em; }
h2 { color: var(--text-primary) !important; font-weight: 600 !important; font-size: 1.25rem !important; letter-spacing: -0.015em; }
h3 { color: var(--text-primary) !important; font-weight: 600 !important; font-size: 1.05rem !important; }

/* ─── CARDS ─── */
.mod-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius-md); padding: 1.25rem 1.5rem;
    box-shadow: var(--shadow-sm); transition: box-shadow 0.15s ease;
}
.mod-card:hover { box-shadow: var(--shadow-md); }
.mod-card-compact { padding: 1rem 1.25rem; }

/* ─── BUTTONS ─── */
.stButton > button,
.stDownloadButton > button,
div[data-testid="stForm"] button[type="submit"] {
    display: inline-flex !important; align-items: center !important; justify-content: center !important;
    padding: 0.5rem 1.25rem !important; border-radius: var(--radius-sm) !important;
    font-size: 0.875rem !important; font-weight: 600 !important;
    border: none !important; cursor: pointer !important;
    transition: all 0.15s ease !important; gap: 0.375rem !important;
    background: var(--primary) !important; color: #ffffff !important;
    box-shadow: 0 1px 2px rgba(99,102,241,0.3) !important;
}
.stButton > button:hover,
.stDownloadButton > button:hover,
div[data-testid="stForm"] button[type="submit"]:hover {
    background: var(--primary-hover) !important;
    box-shadow: 0 4px 8px rgba(99,102,241,0.35) !important;
    transform: translateY(-1px);
}
.stButton > button:active { transform: translateY(0) scale(0.98); }

/* ─── INPUTS ─── */
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stNumberInput > div > div > input,
.stSelectbox > div > div {
    border: 1.5px solid var(--border) !important; border-radius: var(--radius-sm) !important;
    background: var(--surface) !important; color: var(--text-primary) !important;
    font-size: 0.875rem !important; padding: 0.625rem 0.875rem !important;
    transition: border-color 0.15s, box-shadow 0.15s !important;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus,
.stNumberInput > div > div > input:focus,
.stSelectbox > div > div:focus-within {
    border-color: var(--primary) !important;
    box-shadow: 0 0 0 3px rgba(99,102,241,0.12) !important;
}
.stTextInput input::placeholder, .stTextArea textarea::placeholder { color: var(--text-tertiary) !important; }

/* ─── LABELS ─── */
.stTextInput label, .stTextArea label, .stNumberInput label,
.stSelectbox label, .stDateInput label, .stFileUploader label {
    color: var(--text-secondary) !important; font-size: 0.8125rem !important; font-weight: 500 !important;
}

/* ─── SELECTBOX DROPDOWN ─── */
.stSelectbox [data-baseweb="popover"] { background: var(--surface) !important; border-radius: var(--radius-sm) !important; }
.stSelectbox [data-baseweb="popover"] ul { background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: var(--radius-sm) !important; padding: 0.25rem !important; }
.stSelectbox [data-baseweb="popover"] ul li { border-radius: 6px !important; color: var(--text-primary) !important; font-size: 0.875rem !important; }
.stSelectbox [data-baseweb="popover"] ul li:hover { background: var(--primary-50) !important; }

/* ─── DATAFRAMES ─── */
.stDataFrame { border: 1px solid var(--border) !important; border-radius: var(--radius-md) !important; overflow: hidden !important; box-shadow: var(--shadow-sm) !important; }
.stDataFrame th { background: var(--surface-overlay) !important; color: var(--text-primary) !important; font-weight: 600 !important; font-size: 0.8125rem !important; border-bottom: 1px solid var(--border) !important; }
.stDataFrame td { color: var(--text-primary) !important; font-size: 0.8125rem !important; border-color: var(--border-light) !important; }
.stDataFrame tr:hover td { background: var(--primary-50) !important; }

/* ─── EXPANDERS ─── */
.streamlit-expanderHeader {
    background: var(--surface) !important; border: 1px solid var(--border) !important;
    border-radius: var(--radius-sm) !important; color: var(--text-primary) !important;
    font-weight: 500 !important; font-size: 0.875rem !important;
}
[data-testid="stExpander"] { border: none !important; }

/* ─── TABS ─── */
.stTabs [data-baseweb="tab-list"] {
    background: var(--surface-overlay) !important; border-radius: var(--radius-sm) !important;
    padding: 0.25rem !important; gap: 0.25rem !important; border: 1px solid var(--border) !important;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 6px !important; color: var(--text-secondary) !important;
    font-size: 0.8125rem !important; font-weight: 500 !important; padding: 0.5rem 1rem !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: var(--surface) !important; color: var(--primary) !important;
    box-shadow: var(--shadow-sm) !important; font-weight: 600 !important;
}
.stTabs [data-baseweb="tab"]:hover { color: var(--primary) !important; }

/* ─── ALERTS ─── */
.stAlert {
    border-radius: var(--radius-sm) !important; border: none !important;
    font-size: 0.875rem !important; padding: 0.875rem 1.125rem !important;
    box-shadow: var(--shadow-sm) !important;
}
[data-baseweb="notification"][kind="success"] { background: var(--success-bg) !important; color: #065f46 !important; }
[data-baseweb="notification"][kind="error"] { background: var(--error-bg) !important; color: #991b1b !important; }
[data-baseweb="notification"][kind="warning"] { background: var(--warning-bg) !important; color: #92400e !important; }
[data-baseweb="notification"][kind="info"] { background: var(--info-bg) !important; color: #1e40af !important; }

/* ─── CHECKBOX & TOGGLE ─── */
.stCheckbox label { color: var(--text-primary) !important; font-size: 0.875rem !important; }
.stToggle label { color: var(--text-secondary) !important; font-size: 0.8125rem !important; }

/* ─── RADIO ─── */
.stRadio label { color: var(--text-secondary) !important; font-size: 0.875rem !important; }

/* ─── METRICS ─── */
div[data-testid="stMetricValue"] { color: var(--text-primary) !important; font-weight: 700 !important; }
div[data-testid="stMetricLabel"] { color: var(--text-tertiary) !important; font-size: 0.75rem !important; }

/* ─── STATUS BADGES ─── */
.badge {
    display: inline-flex; align-items: center; padding: 0.2rem 0.625rem;
    border-radius: 9999px; font-size: 0.75rem; font-weight: 600; letter-spacing: 0.01em;
}
.badge-pending { background: var(--warning-bg); color: #92400e; }
.badge-approved { background: var(--success-bg); color: #065f46; }
.badge-rejected { background: var(--error-bg); color: #991b1b; }
.badge-active { background: var(--primary-50); color: var(--primary); }

/* ─── SECTION DIVIDER ─── */
.section-divider {
    height: 1px; background: var(--border); margin: 1.5rem 0; border: none;
}

/* ─── EMPTY STATE ─── */
.empty-state {
    text-align: center; padding: 3rem 2rem; color: var(--text-tertiary);
}
.empty-state .empty-icon { font-size: 2.5rem; margin-bottom: 0.75rem; }
.empty-state .empty-title { font-size: 1rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 0.25rem; }
.empty-state .empty-desc { font-size: 0.875rem; }

/* ─── SIDEBAR SECTIONS ─── */
.sidebar-section-title {
    font-size: 0.6875rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.08em; color: var(--text-tertiary); padding: 0.75rem 0.875rem 0.375rem;
}
.sidebar-divider { height: 1px; background: var(--border-light); margin: 0.5rem 0; }

/* ─── CHAT BUBBLES ─── */
.chat-bubble {
    max-width: 75%; padding: 0.75rem 1rem; border-radius: var(--radius-md);
    font-size: 0.875rem; line-height: 1.5; word-break: break-word;
}
.chat-bubble-sent { background: var(--primary); color: #fff; border-bottom-right-radius: 4px; margin-left: auto; }
.chat-bubble-received { background: var(--surface-overlay); color: var(--text-primary); border-bottom-left-radius: 4px; border: 1px solid var(--border); }
.chat-meta { font-size: 0.75rem; color: var(--text-tertiary); margin-top: 0.25rem; }
.chat-avatar {
    width: 2rem; height: 2rem; border-radius: 50%; display: flex;
    align-items: center; justify-content: center; font-size: 0.75rem;
    font-weight: 700; color: #fff; flex-shrink: 0;
}

/* ─── FILE UPLOADER ─── */
.stFileUploader > div > div {
    border: 2px dashed var(--border) !important; border-radius: var(--radius-md) !important;
    background: var(--surface) !important; transition: border-color 0.15s !important;
}
.stFileUploader > div > div:hover { border-color: var(--primary) !important; }
.stFileUploader label { color: var(--text-secondary) !important; }

/* ─── SCROLLBAR ─── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-tertiary); }

/* ─── HIDE DEFAULT STREAMLIT ELEMENTS ─── */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
[data-testid="stToolbar"] { visibility: hidden; }
header[data-testid="stHeader"] { background: var(--surface-raised) !important; }
</style>
"""

st.markdown(MODERN_CSS, unsafe_allow_html=True)

# ─────────────────────────────────────────────
# STATUS BADGE HELPER
# ─────────────────────────────────────────────
def status_badge(status):
    s = str(status).lower().strip()
    cls = "badge-pending" if s == "pending" else "badge-approved" if s == "approved" else "badge-rejected" if s == "rejected" else "badge-active"
    return f'<span class="badge {cls}">{status.upper()}</span>'

# ─────────────────────────────────────────────
# EMPTY STATE HELPER
# ─────────────────────────────────────────────
def empty_state(icon, title, desc=""):
    st.markdown(f'''
    <div class="empty-state">
        <div class="empty-icon">{icon}</div>
        <div class="empty-title">{title}</div>
        <div class="empty-desc">{desc}</div>
    </div>
    ''', unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    # Brand
    st.markdown('''
    <div style="margin-bottom: 1.5rem; padding: 0 0.25rem;">
        <div style="display: flex; align-items: center; gap: 0.625rem; margin-bottom: 0.375rem;">
            <div style="width: 2.25rem; height: 2.25rem; background: var(--primary); border-radius: 8px; display: flex; align-items: center; justify-content: center;">
                <span style="color: #fff; font-weight: 800; font-size: 1rem;">L</span>
            </div>
            <div>
                <div style="font-weight: 700; font-size: 0.9375rem; color: var(--text-primary); line-height: 1.2;">Lyca Management</div>
                <div style="font-size: 0.6875rem; color: var(--text-tertiary); font-weight: 500;">Operations Platform</div>
            </div>
        </div>
    </div>
    ''', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    # User info
    uname = st.session_state.username
    role = st.session_state.role
    role_icon = {"admin": "🛡️", "manager": "📋", "agent": "👤", "qa": "🔍", "wfm": "👥"}.get(role, "👤")
    st.markdown(f'''
    <div style="padding: 0.625rem 0.875rem; background: var(--surface-overlay); border-radius: var(--radius-sm); margin-bottom: 0.75rem;">
        <div style="display: flex; align-items: center; gap: 0.5rem;">
            <span style="font-size: 1.25rem;">{role_icon}</span>
            <div>
                <div style="font-weight: 600; font-size: 0.875rem; color: var(--text-primary);">{uname.title()}</div>
                <div style="font-size: 0.6875rem; color: var(--text-tertiary); text-transform: uppercase; font-weight: 600; letter-spacing: 0.05em;">{role}</div>
            </div>
        </div>
    </div>
    ''', unsafe_allow_html=True)

    # Navigation
    st.markdown('<div class="sidebar-section-title">Main</div>', unsafe_allow_html=True)
    nav_main = [
        ("📊", "Dashboard", "dashboard"),
        ("📋", "Requests", "requests"),
        ("☕", "Breaks", "breaks"),
        ("💬", "Chat", "chat"),
    ]
    for icon, label, key in nav_main:
        active = st.session_state.current_section == key
        if st.button(f"{icon}  {label}", key=f"nav_{key}", use_container_width=True):
            st.session_state.current_section = key
            st.rerun()

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-section-title">Reports</div>', unsafe_allow_html=True)
    nav_reports = [
        ("📝", "Mistakes", "mistakes"),
        ("⏰", "Late Login", "late_login"),
        ("📞", "Quality Issues", "quality_issues"),
        ("🔄", "Mid-shift", "midshift_issues"),
        ("📊", "Live KPIs", "live_kpis"),
    ]
    for icon, label, key in nav_reports:
        if st.button(f"{icon}  {label}", key=f"nav_{key}", use_container_width=True):
            st.session_state.current_section = key
            st.rerun()

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-section-title">Tools</div>', unsafe_allow_html=True)
    nav_tools = [
        ("👥", "WFM", "wfm"),
        ("💎", "Fancy Number", "fancy_number"),
        ("🔔", "Notifications", "notification_settings"),
    ]
    for icon, label, key in nav_tools:
        if st.button(f"{icon}  {label}", key=f"nav_{key}", use_container_width=True):
            st.session_state.current_section = key
            st.rerun()

    if has_manager_level_access():
        st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sidebar-section-title">Admin</div>', unsafe_allow_html=True)
        if st.button("⚙️  Settings", key="nav_admin", use_container_width=True):
            st.session_state.current_section = "admin"
            st.rerun()

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    if st.button("🚪  Sign Out", key="nav_logout", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()

# ─────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────
def render_dashboard():
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <h1 style="margin-bottom: 0.25rem;">Dashboard</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Welcome back! Here's your operations overview.</p>
    </div>
    """, unsafe_allow_html=True)

    # Stats cards
    reqs = get_requests()
    mistakes = get_mistakes()
    pending_reqs = len([r for r in reqs if not r[6]])
    open_mistakes = len(mistakes)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f'''
        <div class="mod-card">
            <div style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-tertiary); margin-bottom: 0.5rem;">Total Requests</div>
            <div style="font-size: 2rem; font-weight: 800; color: var(--text-primary); line-height: 1;">{len(reqs)}</div>
            <div style="font-size: 0.8125rem; color: var(--text-tertiary); margin-top: 0.25rem;">{pending_reqs} pending</div>
        </div>
        ''', unsafe_allow_html=True)
    with col2:
        st.markdown(f'''
        <div class="mod-card">
            <div style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-tertiary); margin-bottom: 0.5rem;">Mistakes</div>
            <div style="font-size: 2rem; font-weight: 800; color: var(--text-primary); line-height: 1;">{open_mistakes}</div>
            <div style="font-size: 0.8125rem; color: var(--text-tertiary); margin-top: 0.25rem;">Open issues</div>
        </div>
        ''', unsafe_allow_html=True)
    with col3:
        late_count = len(get_late_logins())
        st.markdown(f'''
        <div class="mod-card">
            <div style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-tertiary); margin-bottom: 0.5rem;">Late Logins</div>
            <div style="font-size: 2rem; font-weight: 800; color: var(--text-primary); line-height: 1;">{late_count}</div>
            <div style="font-size: 0.8125rem; color: var(--text-tertiary); margin-top: 0.25rem;">This period</div>
        </div>
        ''', unsafe_allow_html=True)
    with col4:
        qual_count = len(get_quality_issues())
        st.markdown(f'''
        <div class="mod-card">
            <div style="font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-tertiary); margin-bottom: 0.5rem;">Quality Issues</div>
            <div style="font-size: 2rem; font-weight: 800; color: var(--text-primary); line-height: 1;">{qual_count}</div>
            <div style="font-size: 0.8125rem; color: var(--text-tertiary); margin-top: 0.25rem;">Reported</div>
        </div>
        ''', unsafe_allow_html=True)

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    # Recent activity
    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown('<h3 style="margin-bottom: 1rem;">Recent Requests</h3>', unsafe_allow_html=True)
        if reqs:
            for req in reqs[:5]:
                rid, agent, rtype, ident, comment, ts, completed = req[:7]
                st.markdown(f'''
                <div class="mod-card mod-card-compact" style="margin-bottom: 0.5rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-weight: 600; font-size: 0.875rem; color: var(--text-primary);">#{rid} · {rtype}</span>
                            <span style="font-size: 0.75rem; color: var(--text-tertiary); margin-left: 0.5rem;">by {agent}</span>
                        </div>
                        {status_badge("Done" if completed else "Pending")}
                    </div>
                    <div style="font-size: 0.8125rem; color: var(--text-secondary); margin-top: 0.375rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{comment[:80]}</div>
                </div>
                ''', unsafe_allow_html=True)
        else:
            empty_state("📋", "No requests yet", "Submit your first request to get started.")

    with col_right:
        st.markdown('<h3 style="margin-bottom: 1rem;">Recent Mistakes</h3>', unsafe_allow_html=True)
        if mistakes:
            for m in mistakes[:5]:
                mid, tl, product, error, ts = m
                st.markdown(f'''
                <div class="mod-card mod-card-compact" style="margin-bottom: 0.5rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-weight: 600; font-size: 0.875rem; color: var(--text-primary);">#{mid}</span>
                        <span style="font-size: 0.75rem; color: var(--text-tertiary);">{ts[:16]}</span>
                    </div>
                    <div style="font-size: 0.8125rem; color: var(--text-secondary); margin-top: 0.25rem;">
                        <strong>Product:</strong> {product or "N/A"}
                    </div>
                    <div style="font-size: 0.8125rem; color: var(--text-tertiary); margin-top: 0.125rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">{error[:100]}</div>
                </div>
                ''', unsafe_allow_html=True)
        else:
            empty_state("✅", "No mistakes logged", "Everything looks good!")

# ─────────────────────────────────────────────
# REQUESTS PAGE
# ─────────────────────────────────────────────
def render_requests():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Requests</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Track and manage agent requests</p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("➕  Submit New Request", expanded=False):
        with st.form("request_form"):
            cols = st.columns([1, 3])
            rtype = cols[0].selectbox("Type", ["Email", "Phone", "Ticket"])
            ident = cols[1].text_input("Identifier")
            comment = st.text_area("Comment", placeholder="Describe the request...")
            if st.form_submit_button("Submit Request"):
                if ident and comment:
                    if add_request(st.session_state.username, rtype, ident, comment):
                        st.success("Request submitted!")
                        st.rerun()
                else:
                    st.warning("Please fill in all fields.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    search = st.text_input("🔍  Search requests...", placeholder="Search by agent, type, or comment...")
    all_reqs = search_requests(search) if search else get_requests()

    if not all_reqs:
        empty_state("📋", "No requests found", "Try adjusting your search or submit a new request.")
        return

    for req in all_reqs:
        rid, agent, rtype, ident, comment, ts, completed = req[:7]
        with st.expander(f"#{rid}  ·  {rtype}  ·  {agent}"):
            st.markdown(f'''
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                <div>
                    <span style="font-weight: 600;">{agent}</span>
                    <span style="color: var(--text-tertiary); margin-left: 0.5rem;">·</span>
                    <span style="color: var(--text-tertiary); margin-left: 0.5rem;">{ts[:16]}</span>
                </div>
                {status_badge("Done" if completed else "Pending")}
            </div>
            <div class="mod-card mod-card-compact">
                <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-tertiary); text-transform: uppercase; margin-bottom: 0.25rem;">Identifier</div>
                <div style="font-size: 0.9375rem; font-weight: 500;">{ident}</div>
            </div>
            <div class="mod-card mod-card-compact" style="margin-top: 0.5rem;">
                <div style="font-size: 0.75rem; font-weight: 600; color: var(--text-tertiary); text-transform: uppercase; margin-bottom: 0.25rem;">Comment</div>
                <div style="font-size: 0.9375rem;">{comment}</div>
            </div>
            ''', unsafe_allow_html=True)

            # Comments
            comments = get_request_comments(rid)
            if comments:
                st.markdown('<div style="font-size: 0.75rem; font-weight: 600; color: var(--text-tertiary); text-transform: uppercase; margin: 0.75rem 0 0.375rem;">Activity</div>', unsafe_allow_html=True)
                for cmt in comments:
                    _, _, user, cmt_text, cmt_time = cmt
                    st.markdown(f'''
                    <div style="padding: 0.375rem 0; border-bottom: 1px solid var(--border-light);">
                        <span style="font-weight: 600; font-size: 0.8125rem; color: var(--text-primary);">{user}</span>
                        <span style="font-size: 0.75rem; color: var(--text-tertiary); margin-left: 0.5rem;">{cmt_time[:16]}</span>
                        <div style="font-size: 0.8125rem; color: var(--text-secondary); margin-top: 0.125rem;">{cmt_text}</div>
                    </div>
                    ''', unsafe_allow_html=True)

            col_check, col_form = st.columns([1, 4])
            with col_check:
                st.checkbox("Done", value=bool(completed), key=f"check_{rid}",
                           on_change=update_request_status, args=(rid, not completed))
            with col_form:
                with st.form(key=f"cmt_form_{rid}"):
                    nc = st.text_input("Add update...", key=f"cmt_{rid}", label_visibility="collapsed")
                    if st.form_submit_button("Comment", use_container_width=True):
                        if nc:
                            add_request_comment(rid, st.session_state.username, nc)
                            st.rerun()

# ─────────────────────────────────────────────
# MISTAKES PAGE
# ─────────────────────────────────────────────
def render_mistakes():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Mistakes Log</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Track and review reported mistakes</p>
    </div>
    """, unsafe_allow_html=True)

    if has_manager_level_access():
        with st.expander("➕  Report Mistake", expanded=False):
            with st.form("mistake_form"):
                product = st.text_input("Product")
                desc = st.text_area("Error Description")
                if st.form_submit_button("Submit"):
                    if product and desc:
                        add_mistake(st.session_state.username, product, desc)
                        st.success("Mistake reported!")
                        st.rerun()

    search = st.text_input("🔍  Search mistakes...", placeholder="Search by product or description...")
    mistakes = search_mistakes(search) if search else get_mistakes()

    def search_mistakes(q):
        q=f"%{q.lower()}%"
        c=get_db_connection().cursor()
        c.execute("SELECT id,team_leader,product,error_description,timestamp FROM mistakes WHERE LOWER(product) LIKE ? OR LOWER(error_description) LIKE ? ORDER BY timestamp DESC",(q,q))
        return c.fetchall()

    if not mistakes:
        empty_state("✅", "No mistakes found", "No mistakes match your search.")
        return

    for m in mistakes:
        mid, tl, product, error, ts = m
        st.markdown(f'''
        <div class="mod-card" style="margin-bottom: 0.625rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div style="display: flex; align-items: center; gap: 0.75rem;">
                    <div style="width: 2rem; height: 2rem; background: var(--error-bg); border-radius: 50%; display: flex; align-items: center; justify-content: center;">
                        <span style="color: var(--error); font-size: 0.75rem; font-weight: 700;">!</span>
                    </div>
                    <div>
                        <div style="font-weight: 600; font-size: 0.875rem;">#{mid} · {product or "N/A"}</div>
                        <div style="font-size: 0.75rem; color: var(--text-tertiary);">by {tl} · {ts[:16]}</div>
                    </div>
                </div>
            </div>
            <div style="font-size: 0.875rem; color: var(--text-secondary); margin-top: 0.625rem; padding-left: 2.75rem;">{error}</div>
        </div>
        ''', unsafe_allow_html=True)

# ─────────────────────────────────────────────
# CHAT PAGE
# ─────────────────────────────────────────────
def render_chat():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Group Chat</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Communicate with your team</p>
    </div>
    """, unsafe_allow_html=True)

    groups = list(set([u[3] for u in get_all_users() if u[3]]))
    if groups:
        view_group = st.selectbox("Group", groups) if has_manager_level_access() else (groups[0] if groups else None)
    else:
        view_group = None
        st.warning("No groups configured.")

    if view_group:
        messages = get_group_messages(view_group)

        # Chat container
        st.markdown(f'''
        <div style="background: var(--surface-overlay); border: 1px solid var(--border); border-radius: var(--radius-md); padding: 1rem; max-height: 500px; overflow-y: auto; margin-bottom: 1rem;">
        ''', unsafe_allow_html=True)

        if not messages:
            empty_state("💬", "No messages yet", "Start the conversation!")
        else:
            for msg in reversed(messages):
                is_sent = msg.get('sender') == st.session_state.username
                sender = msg.get('sender', '?')
                text = msg.get('message', '')
                ts = msg.get('timestamp', '')[:16]
                is_del = bool(msg.get('is_deleted'))
                display = "<em style='color: var(--text-tertiary);'>Message removed</em>" if is_del else text
                avatar_color = "var(--primary)" if is_sent else "#64748b"

                st.markdown(f'''
                <div style="display: flex; gap: 0.5rem; margin-bottom: 0.875rem; flex-direction: {'row-reverse' if is_sent else 'row'}; align-items: flex-start;">
                    <div class="chat-avatar" style="background: {avatar_color};">{sender[0].upper()}</div>
                    <div>
                        <div class="chat-bubble {'chat-bubble-sent' if is_sent else 'chat-bubble-received'}">{display}</div>
                        <div class="chat-meta" style="text-align: {'right' if is_sent else 'left'};">{sender} · {ts}</div>
                    </div>
                </div>
                ''', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

        # Input
        with st.form("chat_form", clear_on_submit=True):
            msg_input = st.text_input("Message", placeholder="Type your message...", label_visibility="collapsed")
            if st.form_submit_button("Send", use_container_width=True):
                if msg_input:
                    send_group_message(st.session_state.username, msg_input, view_group)
                    st.rerun()

# ─────────────────────────────────────────────
# BREAKS PAGE (simplified)
# ─────────────────────────────────────────────
def render_breaks():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Break Scheduling</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Book and manage your break slots</p>
    </div>
    """, unsafe_allow_html=True)

    empty_state("☕", "Break Management", "Break templates are managed through the admin settings. Agents can select their preferred slots during the booking window.")

# ─────────────────────────────────────────────
# LATE LOGIN PAGE
# ─────────────────────────────────────────────
def render_late_login():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Late Login</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Report and track late login incidents</p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("➕  Submit Late Login", expanded=False):
        with st.form("late_form"):
            st.info(f"**Agent:** {st.session_state.username}")
            c1, c2 = st.columns(2)
            presence = c1.text_input("Presence Time", placeholder="09:00")
            login = c2.text_input("Login Time", placeholder="09:15")
            reasons = get_dropdown_options("late_login") or ["Other"]
            reason = st.selectbox("Reason", reasons)
            if st.form_submit_button("Submit"):
                if presence and login and reason:
                    add_late_login(st.session_state.username, presence, login, reason)
                    st.success("Late login submitted!")
                    st.rerun()
                else:
                    st.warning("Please fill all fields.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    late_rows = get_late_logins()
    if not late_rows:
        empty_state("⏰", "No late logins", "No late login records found.")
        return

    for row in late_rows:
        eid, agent, pres, log_t, reason, ts = row[:6]
        status = row[6] if len(row) > 6 else "pending"
        st.markdown(f'''
        <div class="mod-card mod-card-compact" style="margin-bottom: 0.5rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <span style="font-weight: 600; font-size: 0.875rem;">{agent}</span>
                    <span style="color: var(--text-tertiary); margin-left: 0.5rem;">· {pres} → {log_t}</span>
                </div>
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    {status_badge(status)}
                </div>
            </div>
            <div style="font-size: 0.8125rem; color: var(--text-secondary); margin-top: 0.25rem;">{reason} · {ts[:16]}</div>
            {"<div style='margin-top: 0.5rem; display: flex; gap: 0.5rem;'><form></form></div>" if has_manager_level_access() and status=="pending" else ""}
        </div>
        ''', unsafe_allow_html=True)
        if has_manager_level_access() and status == "pending":
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✓ Approve", key=f"la_{eid}", use_container_width=True):
                    approve_late_login(eid, st.session_state.username); st.rerun()
            with c2:
                if st.button("✗ Reject", key=f"lr_{eid}", use_container_width=True):
                    reject_late_login(eid, st.session_state.username); st.rerun()

# ─────────────────────────────────────────────
# QUALITY ISSUES PAGE
# ─────────────────────────────────────────────
def render_quality_issues():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Quality Issues</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Report and track quality-related technical issues</p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("➕  Report Quality Issue", expanded=False):
        with st.form("qi_form"):
            c1, c2, c3, c4 = st.columns(4)
            issues = get_dropdown_options("quality_issues") or ["Other"]
            issue_type = c1.selectbox("Issue Type", issues)
            timing = c2.text_input("Timing", placeholder="14:30")
            mobile = c3.text_input("Mobile Number")
            products = get_dropdown_options("quality_products")
            product = c4.selectbox("Product", products if products else ["N/A"])
            if st.form_submit_button("Submit"):
                if issue_type and timing:
                    add_quality_issue(st.session_state.username, issue_type, timing, mobile, product)
                    st.success("Quality issue reported!")
                    st.rerun()
                else:
                    st.warning("Please fill required fields.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    qi_rows = get_quality_issues()
    if not qi_rows:
        empty_state("📞", "No quality issues", "No quality issues have been reported.")
        return

    for row in qi_rows:
        eid, agent, itype, timing, mobile, product, ts = row[:7]
        status = row[7] if len(row) > 7 else "pending"
        st.markdown(f'''
        <div class="mod-card mod-card-compact" style="margin-bottom: 0.5rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div style="display: flex; align-items: center; gap: 0.75rem;">
                    <div style="width: 2rem; height: 2rem; background: var(--info-bg); border-radius: 50%; display: flex; align-items: center; justify-content: center;">
                        <span style="color: var(--info); font-size: 0.75rem;">📞</span>
                    </div>
                    <div>
                        <div style="font-weight: 600; font-size: 0.875rem;">{agent} · {itype}</div>
                        <div style="font-size: 0.75rem; color: var(--text-tertiary);">{timing} · {product} · {mobile or 'N/A'}</div>
                    </div>
                </div>
                {status_badge(status)}
            </div>
        </div>
        ''', unsafe_allow_html=True)
        if has_manager_level_access() and status == "pending":
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✓", key=f"qa_{eid}", use_container_width=True):
                    approve_quality_issue(eid, st.session_state.username); st.rerun()
            with c2:
                if st.button("✗", key=f"qr_{eid}", use_container_width=True):
                    reject_quality_issue(eid, st.session_state.username); st.rerun()

# ─────────────────────────────────────────────
# MIDSHIFT ISSUES PAGE
# ─────────────────────────────────────────────
def render_midshift():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Mid-shift Issues</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Report mid-shift technical problems</p>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("➕  Report Issue", expanded=False):
        with st.form("ms_form"):
            c1, c2, c3 = st.columns(3)
            issues = get_dropdown_options("midshift_issues") or ["Other"]
            issue_type = c1.selectbox("Issue Type", issues)
            start = c2.text_input("Start Time", placeholder="10:00")
            end = c3.text_input("End Time", placeholder="10:30")
            if st.form_submit_button("Submit"):
                if issue_type and start and end:
                    add_midshift_issue(st.session_state.username, issue_type, start, end)
                    st.success("Mid-shift issue reported!")
                    st.rerun()

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    ms_rows = get_midshift_issues()
    if not ms_rows:
        empty_state("🔄", "No mid-shift issues", "No issues have been reported.")
        return

    for row in ms_rows:
        eid, agent, itype, start, end, ts = row[:6]
        status = row[6] if len(row) > 6 else "pending"
        st.markdown(f'''
        <div class="mod-card mod-card-compact" style="margin-bottom: 0.5rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <span style="font-weight: 600; font-size: 0.875rem;">{agent} · {itype}</span>
                    <span style="color: var(--text-tertiary); margin-left: 0.5rem;">{start} → {end}</span>
                </div>
                {status_badge(status)}
            </div>
        </div>
        ''', unsafe_allow_html=True)
        if has_manager_level_access() and status == "pending":
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✓", key=f"ma_{eid}", use_container_width=True):
                    approve_midshift_issue(eid, st.session_state.username); st.rerun()
            with c2:
                if st.button("✗", key=f"mr_{eid}", use_container_width=True):
                    reject_midshift_issue(eid, st.session_state.username); st.rerun()

# ─────────────────────────────────────────────
# LIVE KPIs PAGE
# ─────────────────────────────────────────────
def render_live_kpis():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Live KPIs</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">View uploaded performance tables</p>
    </div>
    """, unsafe_allow_html=True)

    if has_manager_level_access():
        with st.expander("📤  Upload Table", expanded=False):
            pasted = st.text_area("Paste table from Excel", height=120, placeholder="Paste tab-separated or CSV data...")
            if st.button("Save Table"):
                if pasted.strip():
                    try:
                        import io
                        df = pd.read_csv(io.StringIO(pasted), sep=None, engine='python')
                        add_hold_table(st.session_state.username, df.to_csv(index=False))
                        st.success("Table saved!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    tables = get_hold_tables()
    if tables:
        _, uploader, data, ts = tables[0]
        st.markdown(f'''
        <div class="mod-card mod-card-compact" style="margin-bottom: 1rem;">
            <span style="font-size: 0.75rem; color: var(--text-tertiary);">Uploaded by <strong>{uploader}</strong> · {ts[:16]}</span>
        </div>
        ''', unsafe_allow_html=True)
        try:
            import io
            df = pd.read_csv(io.StringIO(data))
            search = st.text_input("🔍  Search table...", placeholder="Filter rows...")
            if search:
                df = df[df.apply(lambda r: r.astype(str).str.contains(search, case=False, na=False).any(), axis=1)]
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        empty_state("📊", "No tables uploaded", "Upload a table to view it here.")

# ─────────────────────────────────────────────
# FANCY NUMBER PAGE
# ─────────────────────────────────────────────
def render_fancy_number():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Fancy Number Checker</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Analyze phone numbers for Lycamobile qualifying patterns</p>
    </div>
    """, unsafe_allow_html=True)

    c1, c2 = st.columns([1, 2])
    with c1:
        phone = st.text_input("Phone Number", placeholder="e.g., 1555123456")
        if st.button("🔍  Check", use_container_width=True):
            if phone:
                is_fancy, pattern = is_fancy_number(phone)
                clean = re.sub(r'\D', '', phone)
                last6 = clean[-6:] if len(clean) >= 6 else clean
                fmt = f"{last6[:3]}-{last6[3:]}" if len(last6) == 6 else last6
                if is_fancy:
                    st.markdown(f'''
                    <div class="mod-card" style="background: var(--success-bg); border-color: var(--success); margin-top: 1rem;">
                        <div style="text-align: center;">
                            <div style="font-size: 1.5rem; margin-bottom: 0.25rem;">✨</div>
                            <div style="font-size: 1.5rem; font-weight: 800; color: #065f46;">{fmt}</div>
                            <div style="font-size: 0.875rem; color: #065f46; font-weight: 500; margin-top: 0.25rem;">FANCY NUMBER</div>
                            <div style="font-size: 0.8125rem; color: #065f46; margin-top: 0.5rem;">{pattern}</div>
                        </div>
                    </div>
                    ''', unsafe_allow_html=True)
                else:
                    st.markdown(f'''
                    <div class="mod-card" style="margin-top: 1rem;">
                        <div style="text-align: center;">
                            <div style="font-size: 1.5rem; font-weight: 800; color: var(--text-primary);">{fmt}</div>
                            <div style="font-size: 0.875rem; color: var(--text-tertiary); margin-top: 0.25rem;">Standard Number</div>
                            <div style="font-size: 0.8125rem; color: var(--text-tertiary); margin-top: 0.5rem;">{pattern}</div>
                        </div>
                    </div>
                    ''', unsafe_allow_html=True)
            else:
                st.warning("Enter a phone number")

    with c2:
        st.markdown('''
        <div class="mod-card">
            <h3 style="margin-bottom: 0.75rem;">Qualifying Patterns (Last 6 Digits)</h3>
            <div style="font-size: 0.8125rem; color: var(--text-secondary); line-height: 1.8;">
                <strong>6-Digit:</strong> Ascending, Descending, Repeating, Palindrome<br>
                <strong>3-Digit:</strong> Double Triplets, Similar Triplets, Repeating Triplets, Nearly Sequential<br>
                <strong>2-Digit:</strong> Incremental, Repeating, Alternating, Stepping Pairs<br>
                <strong>Exceptional:</strong> Ending with 123, 555, 777, 999
            </div>
        </div>
        ''', unsafe_allow_html=True)

# ─────────────────────────────────────────────
# NOTIFICATION SETTINGS PAGE
# ─────────────────────────────────────────────
def render_notification_settings():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Notification Settings</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Configure how you receive notifications</p>
    </div>
    """, unsafe_allow_html=True)

    prefs = get_user_notification_settings(st.session_state.username)

    st.markdown('''
    <div class="mod-card" style="margin-bottom: 1rem;">
        <h3 style="margin-bottom: 1rem;">Browser Notifications</h3>
        <div style="font-size: 0.875rem; color: var(--text-secondary); margin-bottom: 1rem;">
            Control which notifications you see in-browser and as desktop alerts.
        </div>
    ''', unsafe_allow_html=True)

    with st.form("notif_form"):
        chat_p = st.checkbox("💬  Chat mentions & group alerts", value=prefs.get("chat_notifications", True))
        req_p = st.checkbox("📋  Requests & mistakes updates", value=prefs.get("request_notifications", True))
        break_p = st.checkbox("☕  Break reminders", value=prefs.get("break_notifications", True))
        if st.form_submit_button("Save Preferences"):
            update_user_notification_settings(st.session_state.username, chat=chat_p, request=req_p, breaks=break_p)
            st.success("Preferences updated!")

    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('''
    <div class="mod-card">
        <h3 style="margin-bottom: 0.5rem;">💡 Tips</h3>
        <div style="font-size: 0.875rem; color: var(--text-secondary); line-height: 1.6;">
            • Make sure browser notifications are allowed for this site<br>
            • Desktop alerts only appear when the browser is in the background<br>
            • You can adjust these settings anytime
        </div>
    </div>
    ''', unsafe_allow_html=True)

# ─────────────────────────────────────────────
# WFM PAGE (simplified)
# ─────────────────────────────────────────────
def render_wfm():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Workforce Management</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Roster, swaps, and holiday management</p>
    </div>
    """, unsafe_allow_html=True)

    tab = st.tabs(["📅 Roster", "🔄 Swaps", "🌴 Holidays"])

    with tab[0]:
        if has_manager_level_access():
            with st.expander("📤  Upload Roster"):
                f = st.file_uploader("CSV or Excel", type=['csv','xlsx','xls'], key="roster_up")
                if f:
                    try:
                        df = pd.read_csv(f) if f.name.endswith('.csv') else pd.read_excel(f)
                        st.dataframe(df.head(), use_container_width=True)
                        if st.button("Upload Current"):
                            st.success("Roster uploaded! (simplified preview)")
                    except Exception as e:
                        st.error(f"Error: {e}")
        empty_state("📅", "Roster Management", "Upload roster files to manage agent schedules, shifts, and assignments.")

    with tab[1]:
        empty_state("🔄", "Shift Swaps", "Agents can request shift swaps. Admins can approve or reject pending requests.")

    with tab[2]:
        # Normal holidays
        with st.expander("📝  Submit Holiday Request"):
            with st.form("hol_form"):
                c1, c2 = st.columns(2)
                hs = c1.date_input("Start Date")
                he = c2.date_input("End Date")
                hr = st.text_area("Reason")
                if st.form_submit_button("Submit"):
                    if hs and he and hr and hs <= he:
                        days = (he - hs).days + 1
                        create_holiday_request("", st.session_state.username, str(hs), str(he), days, hr)
                        st.success("Holiday request submitted!")
                        st.rerun()
                    else:
                        st.warning("Provide valid dates and reason.")

        pending_h = get_holiday_requests(status="pending", rtype="normal")
        if pending_h:
            for h in pending_h:
                hid, aid, uname, sd, ed, days, reason, status, rat, db, da, rt = h
                st.markdown(f'''
                <div class="mod-card mod-card-compact" style="margin-bottom: 0.5rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-weight: 600;">{uname}</span>
                            <span style="color: var(--text-tertiary);"> · {sd} → {ed} ({days}d)</span>
                        </div>
                        {status_badge("Pending")}
                    </div>
                    <div style="font-size: 0.8125rem; color: var(--text-secondary); margin-top: 0.25rem;">{reason}</div>
                </div>
                ''', unsafe_allow_html=True)
                if can_manage_holidays():
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("✓", key=f"ha_{hid}", use_container_width=True):
                            update_holiday_request_status(hid, "approved", st.session_state.username); st.rerun()
                    with c2:
                        if st.button("✗", key=f"hr_{hid}", use_container_width=True):
                            update_holiday_request_status(hid, "rejected", st.session_state.username); st.rerun()
        else:
            empty_state("🌴", "No pending holidays", "No holiday requests are waiting for approval.")

# ─────────────────────────────────────────────
# ADMIN PAGE
# ─────────────────────────────────────────────
def render_admin():
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="margin-bottom: 0.25rem;">Admin Settings</h1>
        <p style="color: var(--text-secondary); font-size: 0.9375rem;">Manage users, flags, and system data</p>
    </div>
    """, unsafe_allow_html=True)

    tab = st.tabs(["⚙️ Flags", "👥 Users", "🧹 Data", "📝 Dropdowns"])

    with tab[0]:
        st.markdown('<h3 style="margin-bottom: 1rem;">Feature Flags</h3>', unsafe_allow_html=True)
        flags = [
            ("WFM Module", "wfm_enabled", is_wfm_enabled, toggle_wfm),
            ("Chat", "chat_enabled", is_chat_enabled, toggle_chat_enabled),
            ("Late Login", "late_login_enabled", is_late_login_enabled, toggle_late_login_enabled),
            ("Quality Issues", "quality_enabled", is_quality_enabled, toggle_quality_enabled),
            ("Mid-shift Issues", "midshift_enabled", is_midshift_enabled, toggle_midshift_enabled),
            ("Fancy Number", "fancy_number_enabled", is_fancy_number_enabled, toggle_fancy_number_enabled),
        ]
        for label, key, getter, setter in flags:
            cur = getter()
            st.markdown(f'''
            <div class="mod-card mod-card-compact" style="margin-bottom: 0.375rem;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-weight: 500; font-size: 0.9375rem;">{label}</span>
                    {status_badge("Active" if cur else "Disabled")}
                </div>
            </div>
            ''', unsafe_allow_html=True)
            if st.toggle(f"Enable {label}", value=cur, key=f"flag_{key}"):
                if not cur: setter(True); st.rerun()
            else:
                if cur: setter(False); st.rerun()

    with tab[1]:
        st.markdown('<h3 style="margin-bottom: 1rem;">User Management</h3>', unsafe_allow_html=True)

        with st.expander("➕  Add User"):
            with st.form("add_user_form"):
                c1, c2 = st.columns(2)
                un = c1.text_input("Username")
                pw = c2.text_input("Password", type="password")
                c3, c4 = st.columns(2)
                rl = c3.selectbox("Role", ["agent","admin","manager","qa","wfm"])
                groups = list(set([u[3] for u in get_all_users() if u[3]]))
                gn = c4.selectbox("Group", groups) if groups else c4.text_input("Group Name")
                if st.form_submit_button("Add User"):
                    if un and pw:
                        r = add_user(un, pw, rl, gn)
                        if r == "exists": st.error("User already exists.")
                        elif r: st.success("User added!"); st.rerun()
                        else: st.error("Failed to add user.")

        users = get_all_users()
        if users:
            st.markdown(f'''
            <div class="mod-card" style="padding: 0; overflow: hidden;">
                <table style="width: 100%; border-collapse: collapse; font-size: 0.8125rem;">
                    <thead>
                        <tr style="background: var(--surface-overlay);">
                            <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600; color: var(--text-secondary); border-bottom: 1px solid var(--border);">ID</th>
                            <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600; color: var(--text-secondary); border-bottom: 1px solid var(--border);">Username</th>
                            <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600; color: var(--text-secondary); border-bottom: 1px solid var(--border);">Role</th>
                            <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600; color: var(--text-secondary); border-bottom: 1px solid var(--border);">Group</th>
                        </tr>
                    </thead>
                    <tbody>
            ''', unsafe_allow_html=True)
            for uid, uname, urole, gname in users:
                st.markdown(f'''
                    <tr style="border-bottom: 1px solid var(--border-light);">
                        <td style="padding: 0.625rem 1rem; color: var(--text-tertiary);">{uid}</td>
                        <td style="padding: 0.625rem 1rem; font-weight: 500;">{uname}</td>
                        <td style="padding: 0.625rem 1rem;">{status_badge(urole)}</td>
                        <td style="padding: 0.625rem 1rem; color: var(--text-secondary);">{gname or '-'}</td>
                    </tr>
                ''', unsafe_allow_html=True)
            st.markdown('</tbody></table></div>', unsafe_allow_html=True)

    with tab[2]:
        st.markdown('<h3 style="margin-bottom: 1rem;">Data Management</h3>', unsafe_allow_html=True)
        st.warning("⚠️ These actions are permanent and cannot be undone.")

        clear_ops = [
            ("Clear All Requests", clear_all_requests),
            ("Clear All Mistakes", clear_all_mistakes),
            ("Clear All Chat Messages", clear_all_group_messages),
            ("Clear Late Logins", clear_late_logins),
            ("Clear Quality Issues", clear_quality_issues),
            ("Clear Mid-shift Issues", clear_midshift_issues),
        ]
        for label, fn in clear_ops:
            if st.button(f"🗑️  {label}", key=f"clear_{label}"):
                if fn(): st.success(f"{label} completed!"); st.rerun()

    with tab[3]:
        st.markdown('<h3 style="margin-bottom: 1rem;">Dropdown Options</h3>', unsafe_allow_html=True)
        sections = {"Late Login Reasons": "late_login", "Quality Issues": "quality_issues", "Mid-shift Issues": "midshift_issues"}
        sel = st.selectbox("Section", list(sections.keys()))
        section_key = sections[sel]

        with st.form(f"add_opt_{section_key}"):
            new_opt = st.text_input("New Option")
            if st.form_submit_button("Add"):
                if new_opt:
                    add_dropdown_option(section_key, new_opt.strip())
                    st.success("Option added!"); st.rerun()

        opts = get_all_dropdown_options_with_ids(section_key)
        if opts:
            for oid, oval, _ in opts:
                c1, c2 = st.columns([4, 1])
                with c1:
                    st.markdown(f'<div style="padding: 0.5rem 0; font-size: 0.875rem; color: var(--text-primary);">• {oval}</div>', unsafe_allow_html=True)
                with c2:
                    if st.button("🗑️", key=f"del_opt_{oid}"):
                        delete_dropdown_option(section_key, oval); st.rerun()

# ─────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────
section = st.session_state.current_section

if section == "dashboard": render_dashboard()
elif section == "requests": render_requests()
elif section == "mistakes": render_mistakes()
elif section == "chat": render_chat()
elif section == "breaks": render_breaks()
elif section == "late_login": render_late_login()
elif section == "quality_issues": render_quality_issues()
elif section == "midshift_issues": render_midshift()
elif section == "live_kpis": render_live_kpis()
elif section == "fancy_number": render_fancy_number()
elif section == "notification_settings": render_notification_settings()
elif section == "wfm": render_wfm()
elif section == "admin" and has_manager_level_access(): render_admin()
else: render_dashboard()
