import asyncio
import json
import logging
import os
import threading
import time
from threading import Thread

from flask import Flask, request
from splusthon import SoroushClient
from splusthon.sessions import StringSession

# ================= تنظیمات =================
ACCOUNTS_FILE = "accounts_data.json"
LEGACY_SESSION_FILE = "session_local.txt"
PORT = int(os.environ.get("PORT", 8080))
HTML_FILE = "panel.html"

accounts_store = {
    "active_id": None,
    "accounts": [],
}

# هر آیتم در spam_jobs:
# {
#   "id": "sp_1",
#   "target": "...",
#   "target_name": "...",
#   "texts": ["متن 1", "متن 2"],
#   "text_index": 0,
#   "speed": 0.5,
#   "running": True,
#   "sent_count": 0,
#   "last_status": "در حال ارسال..."
# }
spam_jobs = []
spam_counter = 0

data = {
    "status_msg": "در انتظار ورود از پنل...",
    "ping": "نامشخص",
    "auth_status": "disconnected",
    "auth_message": "شماره تلفن خود را وارد کنید (مثال: +98912...)",
    "phone_number": "",
}

client = SoroushClient(StringSession())
group_list = []
bot_loop = None
pending_phone = ""
switch_requested = False
reconnect_needed = False


def load_accounts():
    global accounts_store
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict) and "accounts" in loaded:
                    accounts_store = loaded
        except Exception as e:
            print(f"[ERR load_accounts] {e}")

    if not accounts_store["accounts"] and os.path.exists(LEGACY_SESSION_FILE):
        try:
            with open(LEGACY_SESSION_FILE, "r", encoding="utf-8") as f:
                old_s = f.read().strip()
            if old_s:
                acc_id = "acc_1"
                accounts_store["accounts"].append(
                    {"id": acc_id, "phone": "اکانت قدیمی", "name": "اکانت ۱", "session": old_s}
                )
                accounts_store["active_id"] = acc_id
                save_accounts()
        except Exception as e:
            print(f"[ERR legacy_session] {e}")


def save_accounts():
    try:
        with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(accounts_store, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERR save_accounts] {e}")


def get_active_account():
    aid = accounts_store.get("active_id")
    for acc in accounts_store.get("accounts", []):
        if acc["id"] == aid:
            return acc
    return None


def get_public_accounts_list():
    return [
        {
            "id": acc["id"],
            "phone": acc.get("phone", ""),
            "name": acc.get("name", acc.get("phone", acc["id"])),
            "active": acc["id"] == accounts_store.get("active_id"),
        }
        for acc in accounts_store.get("accounts", [])
    ]


def _resolve_group_name(target: str) -> str:
    t_str = str(target).strip()
    for g in group_list:
        if str(g.get("id")) == t_str:
            return g.get("name") or t_str
    return t_str


def _parse_multi_texts(raw_texts_encoded: str) -> list:
    if not raw_texts_encoded:
        return []
    decoded = raw_texts_encoded.replace("_NEWLINE_", "\n")
    try:
        arr = json.loads(decoded)
        if isinstance(arr, list):
            cleaned = [str(x).strip() for x in arr if str(x).strip()]
            if cleaned:
                return cleaned
    except Exception:
        pass
    lines = [line.strip() for line in decoded.split("|||") if line.strip()]
    if lines:
        return lines
    return [decoded.strip()] if decoded.strip() else []


load_accounts()


# ================= احراز هویت و مدیریت حساب =================
async def _ensure_connected():
    try:
        await client.connect()
    except Exception as e:
        print(f"[WARN connect] {e}")


async def _request_code_coro(phone: str):
    global pending_phone
    pending_phone = phone
    data["phone_number"] = phone
    data["auth_message"] = "در حال اتصال و ارسال کد..."
    try:
        await _ensure_connected()
        if hasattr(client, "send_code_request"):
            await client.send_code_request(phone)
        elif hasattr(client, "send_code"):
            await client.send_code(phone)
        elif hasattr(client, "sign_in"):
            await client.sign_in(phone=phone)
        else:
            raise RuntimeError("متد ارسال کد در SoroushClient پیدا نشد.")
        data["auth_status"] = "code_sent"
        data["auth_message"] = "کد ارسال شد؛ کد را وارد کنید."
    except Exception as e:
        print(f"[ERR send_code] {e}")
        data["auth_status"] = "disconnected"
        data["auth_message"] = f"خطا در ارسال کد: {e}"


async def _submit_code_coro(code: str):
    global switch_requested
    data["auth_message"] = "در حال بررسی کد..."
    try:
        await _ensure_connected()
        if hasattr(client, "sign_in"):
            try:
                await client.sign_in(phone=pending_phone, code=code)
            except TypeError:
                await client.sign_in(pending_phone, code)

        saved_session = client.session.save() if getattr(client, "session", None) else ""
        me_name = pending_phone
        try:
            me = await client.get_me()
            if getattr(me, "first_name", None):
                me_name = me.first_name
        except Exception as e:
            print(f"[WARN get_me after login] {e}")

        acc_id = f"acc_{int(time.time() * 1000)}"
        accounts_store["accounts"].append(
            {
                "id": acc_id,
                "phone": pending_phone,
                "name": me_name,
                "session": saved_session or "",
            }
        )
        accounts_store["active_id"] = acc_id
        save_accounts()

        data["auth_status"] = "connected"
        data["auth_message"] = f"حساب {me_name} اضافه شد."
        switch_requested = True
    except Exception as e:
        print(f"[ERR verify_code] {e}")
        data["auth_status"] = "code_sent"
        data["auth_message"] = f"خطا در کد: {e}"


async def _switch_account_coro(acc_id: str):
    global client, switch_requested, group_list
    target_acc = next((a for a in accounts_store["accounts"] if a["id"] == acc_id), None)
    if not target_acc:
        data["auth_message"] = "حساب پیدا نشد."
        return

    accounts_store["active_id"] = acc_id
    save_accounts()
    group_list = []

    try:
        await client.disconnect()
    except Exception:
        pass

    client = SoroushClient(StringSession(target_acc.get("session", "")))
    switch_requested = True
    data["auth_status"] = "disconnected"
    data["auth_message"] = f"تغییر به حساب: {target_acc.get('name')}"


async def _delete_account_coro(acc_id: str):
    global client, switch_requested
    accounts_store["accounts"] = [a for a in accounts_store["accounts"] if a["id"] != acc_id]
    if accounts_store.get("active_id") == acc_id:
        accounts_store["active_id"] = (
            accounts_store["accounts"][0]["id"] if accounts_store["accounts"] else None
        )
        try:
            await client.disconnect()
        except Exception:
            pass
        next_acc = get_active_account()
        client = SoroushClient(StringSession(next_acc.get("session", "") if next_acc else ""))
        switch_requested = True
        data["auth_status"] = "disconnected"
    save_accounts()
    data["auth_message"] = "حساب حذف شد."


def trigger_send_code(phone: str):
    if bot_loop and not bot_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_request_code_coro(phone), bot_loop)


def trigger_verify_code(code: str):
    if bot_loop and not bot_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_submit_code_coro(code), bot_loop)


def trigger_switch_account(acc_id: str):
    if bot_loop and not bot_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_switch_account_coro(acc_id), bot_loop)


def trigger_delete_account(acc_id: str):
    if bot_loop and not bot_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_delete_account_coro(acc_id), bot_loop)


# ================= پنل چند اسپم و چند متن =================
def create_html_file():
    html_content = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>پنل چند اسپم سلف</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: Tahoma, sans-serif;
            background: #0d1117;
            color: #e6edf3;
            padding: 14px;
        }
        .wrap { max-width: 480px; margin: 0 auto; }
        .top-banner {
            background: #161b22;
            border: 1px solid #30363d;
            border-right: 4px solid #a371f7;
            border-radius: 10px;
            padding: 12px 14px;
            margin-bottom: 12px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .badge {
            background: #21262d;
            border: 1px solid #388bfd;
            color: #79c0ff;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: bold;
        }
        .box {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 10px;
            padding: 14px;
            margin-bottom: 12px;
        }
        .box-info { border-top: 3px solid #a371f7; }
        .box-cyan { border-top: 3px solid #2f81f7; }
        .box-purple { border-top: 3px solid #d2a8ff; }
        .box-green { border-top: 3px solid #3fb950; }
        h3 {
            font-size: 15px;
            margin-bottom: 10px;
            padding-bottom: 6px;
            border-bottom: 1px solid #21262d;
            color: #f0f6fc;
        }
        label { display: block; font-size: 12px; color: #8b949e; margin: 8px 0 4px; }
        input, textarea {
            width: 100%;
            padding: 9px;
            border-radius: 7px;
            border: 1px solid #30363d;
            background: #0d1117;
            color: #e6edf3;
            font-family: Tahoma, sans-serif;
            font-size: 13px;
        }
        input:focus, textarea:focus { outline: none; border-color: #58a6ff; }
        textarea { min-height: 60px; resize: vertical; }
        button {
            width: 100%;
            padding: 9px 12px;
            border: none;
            border-radius: 7px;
            background: #21262d;
            color: #f0f6fc;
            font-family: Tahoma, sans-serif;
            font-size: 13px;
            font-weight: bold;
            cursor: pointer;
            margin-top: 6px;
        }
        button:active { transform: scale(0.99); }
        .btn-blue { background: #1f6feb; color: #fff; }
        .btn-green { background: #238636; color: #fff; }
        .btn-red { background: #da3633; color: #fff; }
        .btn-amber { background: #9e6a03; color: #fff; }
        .btn-purple { background: #8957e5; color: #fff; }
        .row { display: flex; gap: 8px; }
        .row button { flex: 1; }
        .groups { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; max-height: 110px; overflow-y: auto; }
        .gbtn { width: auto; padding: 5px 10px; font-size: 11px; margin-top: 0; background: #21262d; border: 1px solid #30363d; color: #79c0ff; }
        .msg { font-size: 12px; color: #8b949e; margin-top: 8px; word-break: break-word; }
        .log {
            background: #090c10;
            border: 1px solid #21262d;
            color: #3fb950;
            padding: 8px;
            border-radius: 7px;
            font-family: monospace;
            font-size: 11px;
            margin-top: 8px;
            direction: ltr;
            text-align: left;
        }
        .acc-row, .job-card {
            border: 1px solid #30363d;
            background: #0d1117;
            padding: 9px 10px;
            border-radius: 8px;
            margin-top: 7px;
        }
        .acc-row { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
        .acc-row.active { border-color: #3fb950; }
        .acc-meta { font-size: 12px; flex: 1; word-break: break-word; }
        .acc-btns { display: flex; gap: 5px; flex-wrap: wrap; }
        .mini { width: auto; padding: 5px 9px; font-size: 11px; margin-top: 0; }
        .info-item {
            background: #0d1117;
            border: 1px solid #2b313a;
            border-right: 3px solid #3fb950;
            border-radius: 7px;
            padding: 8px 10px;
            margin-top: 6px;
            font-size: 12px;
            line-height: 1.7;
        }
        .info-item.stopped { border-right-color: #f85149; }
        .text-slot {
            display: flex;
            gap: 6px;
            align-items: flex-start;
            margin-top: 6px;
        }
        .text-slot textarea { flex: 1; min-height: 52px; }
    </style>
</head>
<body>
<div class="wrap">
    <div class="top-banner">
        <div>
            <div style="font-size:14px;font-weight:bold;color:#f0f6fc;">⚡ پنل چند اسپم و حساب‌ها</div>
            <div style="font-size:11px;color:#8b949e;margin-top:2px;">مدیریت چندین اسپم و چندین متن</div>
        </div>
        <span class="badge" id="ping_badge">پینگ: ...</span>
    </div>

    <!-- اطلاعات اسپم بالای پنل -->
    <div class="box box-info">
        <h3>📊 اطلاعات اسپم</h3>
        <div id="spam_info_panel">
            <div class="msg">هیچ اسپمی ثبت نشده است.</div>
        </div>
        <div class="log" id="log_box">Ready</div>
    </div>

    <!-- افزودن حساب -->
    <div class="box box-cyan">
        <h3>📱 افزودن حساب</h3>
        <label>شماره تلفن:</label>
        <input type="text" id="phone_input" placeholder="+98912..." dir="ltr">
        <button class="btn-blue" onclick="sendPhone()">📨 ارسال کد</button>
        <div id="code_sec" style="display:none; margin-top:8px;">
            <label>کد تأیید:</label>
            <input type="text" id="code_input" placeholder="12345" dir="ltr">
            <button class="btn-green" onclick="verifyCode()">✅ تأیید و ذخیره</button>
        </div>
        <div class="msg" id="auth_msg">در حال بررسی...</div>
    </div>

    <!-- مدیریت حساب‌ها -->
    <div class="box box-purple">
        <h3>👥 مدیریت حساب‌ها</h3>
        <button class="btn-amber" onclick="loadAccounts()">🔄 بروزرسانی لیست</button>
        <div id="accounts_list"></div>
    </div>

    <!-- ساخت و مدیریت چند اسپم -->
    <div class="box box-green">
        <h3>🚀 افزودن اسپم جدید</h3>
        <button class="btn-amber" onclick="loadGroups('target', 'spam_groups')">📋 دریافت لیست گروه‌ها</button>
        <div id="spam_groups" class="groups"></div>

        <label>گروه یا آیدی مقصد:</label>
        <input type="text" id="target" placeholder="انتخاب از لیست یا وارد کردن آیدی...">

        <label>متن‌های اسپم (چند متن):</label>
        <div id="multi_texts_container">
            <div class="text-slot">
                <textarea class="spam-text-input" placeholder="متن شماره ۱..."></textarea>
                <button type="button" class="mini btn-red" onclick="removeTextSlot(this)">✖</button>
            </div>
        </div>
        <button type="button" class="btn-purple" onclick="addTextSlot()">➕ افزودن متن دیگر</button>

        <label>سرعت دقیق (ثانیه بین هر ارسال):</label>
        <input type="number" id="speed" value="0.5" step="0.05" min="0.01">

        <button class="btn-green" onclick="addSpamJob()">➕ افزودن و شروع اسپم</button>

        <h3 style="margin-top:16px;">📂 لیست اسپم‌های ساخته‌شده</h3>
        <div id="spam_jobs_list">
            <div class="msg">هنوز اسپمی ساخته نشده است.</div>
        </div>
    </div>
</div>

<script>
function escapeHtml(str) {
    return String(str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function addTextSlot() {
    const container = document.getElementById('multi_texts_container');
    const idx = container.querySelectorAll('.spam-text-input').length + 1;
    const div = document.createElement('div');
    div.className = 'text-slot';
    div.innerHTML = `
        <textarea class="spam-text-input" placeholder="متن شماره ${idx}..."></textarea>
        <button type="button" class="mini btn-red" onclick="removeTextSlot(this)">✖</button>
    `;
    container.appendChild(div);
}

function removeTextSlot(btn) {
    const container = document.getElementById('multi_texts_container');
    const slots = container.querySelectorAll('.text-slot');
    if (slots.length <= 1) {
        slots[0].querySelector('textarea').value = '';
        return;
    }
    btn.parentElement.remove();
}

function collectTextsArray() {
    const els = document.querySelectorAll('.spam-text-input');
    const arr = [];
    els.forEach(el => {
        const v = el.value.trim();
        if (v) arr.push(v);
    });
    return arr;
}

async function sendPhone() {
    let p = document.getElementById('phone_input').value.trim();
    if (!p) return alert('شماره را وارد کن');
    document.getElementById('auth_msg').innerText = 'در حال ارسال درخواست...';
    await fetch('/send_phone?phone=' + encodeURIComponent(p));
}

async function verifyCode() {
    let c = document.getElementById('code_input').value.trim();
    if (!c) return alert('کد را وارد کن');
    await fetch('/verify_code?code=' + encodeURIComponent(c));
    setTimeout(loadAccounts, 1000);
}

async function loadAccounts() {
    try {
        let res = await fetch('/getaccounts');
        let list = await res.json();
        let box = document.getElementById('accounts_list');
        if (!list.length) {
            box.innerHTML = '<div class="msg">حسابی ثبت نشده</div>';
            return;
        }
        box.innerHTML = list.map(a => `
            <div class="acc-row ${a.active ? 'active' : ''}">
                <div class="acc-meta">
                    <div><b>${escapeHtml(a.name)}</b> ${a.active ? '<span style="color:#3fb950;">[فعال]</span>' : ''}</div>
                    <div style="color:#8b949e;font-size:11px;" dir="ltr">${escapeHtml(a.phone)}</div>
                </div>
                <div class="acc-btns">
                    ${!a.active ? `<button class="mini btn-blue" onclick="switchAccount('${a.id}')">فعال</button>` : ''}
                    <button class="mini btn-red" onclick="deleteAccount('${a.id}')">حذف</button>
                </div>
            </div>
        `).join('');
    } catch (e) {}
}

async function switchAccount(id) {
    await fetch('/switch_account?id=' + encodeURIComponent(id));
    setTimeout(loadAccounts, 600);
}

async function deleteAccount(id) {
    await fetch('/delete_account?id=' + encodeURIComponent(id));
    setTimeout(loadAccounts, 500);
}

async function loadGroups(inputId, listId) {
    document.getElementById(listId).innerHTML = '<span class="msg">در حال دریافت...</span>';
    try {
        let res = await fetch('/getgroups');
        let data = await res.json();
        if (!Array.isArray(data) || !data.length) {
            document.getElementById(listId).innerHTML = '<span class="msg">لیستی دریافت نشد</span>';
            return;
        }
        document.getElementById(listId).innerHTML = data.map(g => {
            const safeId = String(g.id).replace(/'/g, "\\'");
            return `<button class="gbtn" onclick="selectGroup('${safeId}', '${inputId}')">${escapeHtml(g.name)}</button>`;
        }).join('');
    } catch (e) {
        document.getElementById(listId).innerHTML = '<span class="msg">خطا در دریافت لیست</span>';
    }
}

function selectGroup(id, inputId) {
    document.getElementById(inputId).value = id;
}

async function addSpamJob() {
    let t = document.getElementById('target').value.trim();
    let texts = collectTextsArray();
    let s = document.getElementById('speed').value;
    if (!t) return alert('گروه یا مقصد را انتخاب یا وارد کن');
    if (!texts.length) return alert('حداقل یک متن اسپم وارد کن');

    const encodedTexts = encodeURIComponent(JSON.stringify(texts).replace(/\\n/g, '_NEWLINE_'));
    await fetch('/add_spam?target=' + encodeURIComponent(t) + '&texts=' + encodedTexts + '&speed=' + encodeURIComponent(s));
    refreshSpamState();
}

async function toggleSpamJob(jobId, action) {
    await fetch('/' + action + '_spam?id=' + encodeURIComponent(jobId));
    refreshSpamState();
}

async function deleteSpamJob(jobId) {
    await fetch('/del_spam?id=' + encodeURIComponent(jobId));
    refreshSpamState();
}

function renderSpamPanels(jobs) {
    const infoBox = document.getElementById('spam_info_panel');
    const jobsBox = document.getElementById('spam_jobs_list');

    if (!Array.isArray(jobs) || !jobs.length) {
        infoBox.innerHTML = '<div class="msg">هیچ اسپم فعالی ثبت نشده است.</div>';
        jobsBox.innerHTML = '<div class="msg">هنوز اسپمی ساخته نشده است.</div>';
        return;
    }

    infoBox.innerHTML = jobs.map(j => `
        <div class="info-item ${j.running ? '' : 'stopped'}">
            <div><b>گروه:</b> ${escapeHtml(j.target_name)} <span style="color:#8b949e;">(${escapeHtml(j.target)})</span></div>
            <div><b>سرعت:</b> ${j.speed} ثانیه | <b>تعداد ارسال:</b> ${j.sent_count} | <b>تعداد متن:</b> ${(j.texts || []).length}</div>
            <div style="color:${j.running ? '#3fb950' : '#f85149'};"><b>وضعیت:</b> ${j.running ? 'در حال ارسال' : 'متوقف'} (${escapeHtml(j.last_status || '')})</div>
        </div>
    `).join('');

    jobsBox.innerHTML = jobs.map(j => {
        const previewTexts = (j.texts || []).map((tx, idx) => `${idx + 1}. ${escapeHtml(tx)}`).join(' | ');
        return `
        <div class="job-card">
            <div style="font-size:12px;"><b>${escapeHtml(j.target_name)}</b> — سرعت: <b>${j.speed}s</b> — ارسال: <b>${j.sent_count}</b></div>
            <div class="msg" style="margin-top:4px;">متن‌ها: ${previewTexts}</div>
            <div class="acc-btns" style="margin-top:8px;">
                ${!j.running ? `<button class="mini btn-green" onclick="toggleSpamJob('${j.id}', 'resume')">▶ ادامه</button>` : `<button class="mini btn-amber" onclick="toggleSpamJob('${j.id}', 'pause')">⏸ توقف</button>`}
                <button class="mini btn-red" onclick="deleteSpamJob('${j.id}')">🗑 حذف</button>
            </div>
        </div>`;
    }).join('');
}

async function refreshSpamState() {
    try {
        let res = await fetch('/getlog');
        let d = await res.json();
        document.getElementById('log_box').innerText = d.status;
        document.getElementById('ping_badge').innerText = 'پینگ: ' + d.ping;
        document.getElementById('auth_msg').innerText = d.auth_message;
        document.getElementById('code_sec').style.display = (d.auth_status === 'code_sent') ? 'block' : 'none';
        renderSpamPanels(d.spam_jobs || []);
    } catch (e) {}
}

setInterval(refreshSpamState, 1500);
loadAccounts();
refreshSpamState();
</script>
</body>
</html>"""
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)


create_html_file()


# ================= Flask =================
logging.getLogger("werkzeug").setLevel(logging.ERROR)
flask_app = Flask(__name__)


@flask_app.route("/")
def index():
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        return f.read()


@flask_app.route("/health")
def health():
    return "OK", 200


@flask_app.route("/send_phone")
def route_send_phone():
    phone = (request.args.get("phone") or "").strip()
    if phone:
        trigger_send_code(phone)
    return "", 200


@flask_app.route("/verify_code")
def route_verify_code():
    code = (request.args.get("code") or "").strip()
    if code:
        trigger_verify_code(code)
    return "", 200


@flask_app.route("/getaccounts")
def route_getaccounts():
    return flask_app.response_class(
        response=json.dumps(get_public_accounts_list(), ensure_ascii=False),
        mimetype="application/json; charset=utf-8",
    )


@flask_app.route("/switch_account")
def route_switch_account():
    acc_id = (request.args.get("id") or "").strip()
    if acc_id:
        trigger_switch_account(acc_id)
    return "", 200


@flask_app.route("/delete_account")
def route_delete_account():
    acc_id = (request.args.get("id") or "").strip()
    if acc_id:
        trigger_delete_account(acc_id)
    return "", 200


@flask_app.route("/add_spam")
def route_add_spam():
    global spam_counter
    target = (request.args.get("target") or "").strip()
    raw_texts = request.args.get("texts") or ""
    texts = _parse_multi_texts(raw_texts)
    try:
        speed = max(0.01, float(request.args.get("speed") or "0.5"))
    except Exception:
        speed = 0.5

    if not target or not texts:
        return "", 200

    spam_counter += 1
    job = {
        "id": f"sp_{spam_counter}_{int(time.time() * 1000) % 10000}",
        "target": target,
        "target_name": _resolve_group_name(target),
        "texts": texts,
        "text_index": 0,
        "speed": speed,
        "running": True,
        "sent_count": 0,
        "last_status": "شروع شد",
    }
    spam_jobs.append(job)
    data["status_msg"] = f"اسپم جدید روی {job['target_name']} با سرعت {speed}s اضافه شد."
    return "", 200


@flask_app.route("/pause_spam")
def route_pause_spam():
    jid = (request.args.get("id") or "").strip()
    for j in spam_jobs:
        if j["id"] == jid:
            j["running"] = False
            j["last_status"] = "متوقف شد"
            data["status_msg"] = f"اسپم {j['target_name']} متوقف شد."
    return "", 200


@flask_app.route("/resume_spam")
def route_resume_spam():
    jid = (request.args.get("id") or "").strip()
    for j in spam_jobs:
        if j["id"] == jid:
            j["running"] = True
            j["last_status"] = "در حال ارسال"
            data["status_msg"] = f"اسپم {j['target_name']} ادامه یافت."
    return "", 200


@flask_app.route("/del_spam")
def route_del_spam():
    jid = (request.args.get("id") or "").strip()
    spam_jobs[:] = [j for j in spam_jobs if j["id"] != jid]
    data["status_msg"] = "یک اسپم حذف شد."
    return "", 200


@flask_app.route("/getgroups")
def route_getgroups():
    return flask_app.response_class(
        response=json.dumps(group_list, ensure_ascii=False),
        mimetype="application/json; charset=utf-8",
    )


@flask_app.route("/getlog")
def route_getlog():
    result = {
        "status": data["status_msg"],
        "ping": data["ping"],
        "auth_status": data["auth_status"],
        "auth_message": data["auth_message"],
        "spam_jobs": spam_jobs,
    }
    return flask_app.response_class(
        response=json.dumps(result, ensure_ascii=False),
        mimetype="application/json; charset=utf-8",
    )


def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False)


# ================= لوپ‌های سلف =================
async def fetch_groups():
    global group_list
    try:
        dialogs = await client.get_dialogs(limit=200)
        group_list = [
            {"id": d.id, "name": d.name or str(d.id)}
            for d in dialogs
            if getattr(d, "is_group", False) or getattr(d, "is_channel", False)
        ]
        for j in spam_jobs:
            j["target_name"] = _resolve_group_name(j["target"])
    except Exception as e:
        data["status_msg"] = f"خطا در دریافت لیست: {e}"


async def ping_loop():
    global reconnect_needed
    fail_streak = 0
    while True:
        try:
            t0 = time.perf_counter()
            await asyncio.wait_for(client.get_me(), timeout=5)
            ms = int((time.perf_counter() - t0) * 1000)
            data["ping"] = f"{ms} ms"
            fail_streak = 0
        except Exception:
            fail_streak += 1
            data["ping"] = "قطع"
            if fail_streak >= 2:
                reconnect_needed = True
        await asyncio.sleep(6)


async def single_spam_job_loop(job: dict):
    global reconnect_needed
    while job in spam_jobs:
        if job.get("running") and job.get("target") and job.get("texts"):
            cycle_start = time.perf_counter()
            try:
                texts = job["texts"]
                idx = int(job.get("text_index", 0)) % len(texts)
                msg_text = texts[idx]
                job["text_index"] = (idx + 1) % len(texts)

                t_val = str(job["target"]).strip()
                chat_id = int(t_val) if t_val.lstrip("-").isdigit() else t_val

                await asyncio.wait_for(client.send_message(chat_id, msg_text), timeout=10)
                job["sent_count"] = int(job.get("sent_count", 0)) + 1
                job["last_status"] = f"ارسال #{job['sent_count']}"
                data["status_msg"] = f"{job['target_name']} | ارسال #{job['sent_count']} ({job['speed']}s)"
            except Exception as e:
                err_msg = str(e).lower()
                if "flood" in err_msg:
                    job["last_status"] = "Flood-wait (5s)"
                    data["status_msg"] = f"Flood-wait در {job['target_name']}: 5s صبر..."
                    await asyncio.sleep(5)
                    continue
                elif "websocket" in err_msg or "closed" in err_msg or "timeout" in err_msg or "connect" in err_msg:
                    reconnect_needed = True
                    job["last_status"] = "قطع اتصال"
                    data["status_msg"] = "اتصال قطع شد؛ در حال بازیابی..."
                    await asyncio.sleep(2)
                    continue
                else:
                    job["last_status"] = f"خطا: {e}"
                    data["status_msg"] = f"خطا در {job['target_name']}: {e}"
                    await asyncio.sleep(1)
                    continue

            elapsed = time.perf_counter() - cycle_start
            target_speed = max(0.01, float(job.get("speed") or 0.5))
            remaining = target_speed - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
        else:
            await asyncio.sleep(0.25)


async def spam_dispatcher_loop():
    spawned_ids = set()
    while True:
        for job in list(spam_jobs):
            jid = job["id"]
            if jid not in spawned_ids:
                spawned_ids.add(jid)
                asyncio.create_task(single_spam_job_loop(job))
        active_ids = {j["id"] for j in spam_jobs}
        spawned_ids.intersection_update(active_ids)
        await asyncio.sleep(0.3)


async def main():
    global bot_loop, client, switch_requested, reconnect_needed
    bot_loop = asyncio.get_running_loop()

    asyncio.create_task(spam_dispatcher_loop())
    asyncio.create_task(ping_loop())

    while True:
        try:
            switch_requested = False
            reconnect_needed = False
            active_acc = get_active_account()
            if not active_acc or not active_acc.get("session"):
                data["status_msg"] = "منتظر افزودن یا انتخاب حساب..."
                await asyncio.sleep(1)
                continue

            try:
                await client.disconnect()
            except Exception:
                pass

            client = SoroushClient(StringSession(active_acc["session"]))
            data["status_msg"] = f"اتصال به {active_acc.get('name')}..."
            await client.connect()

            if hasattr(client, "is_user_authorized"):
                if not await client.is_user_authorized():
                    data["auth_status"] = "disconnected"
                    data["auth_message"] = "سشن نامعتبر است؛ حساب را دوباره اضافه کنید."
                    await asyncio.sleep(2)
                    continue

            me = await client.get_me()
            if getattr(me, "first_name", None):
                active_acc["name"] = me.first_name
                save_accounts()

            data["auth_status"] = "connected"
            data["auth_message"] = f"متصل به: {active_acc.get('name')}"
            data["status_msg"] = f"{active_acc.get('name')} آنلاین شد."
            await fetch_groups()

            while not switch_requested and not reconnect_needed:
                await asyncio.sleep(1)

        except Exception as e:
            data["status_msg"] = f"خطای اتصال: {e}"
            await asyncio.sleep(4)
            try:
                await client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    Thread(target=run_flask, daemon=True).start()
    Thread(target=lambda: asyncio.run(main()), daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("خاموش شد")
