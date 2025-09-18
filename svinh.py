import os
import discord
from discord.ext import commands
from discord import app_commands
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from functools import wraps
from datetime import datetime
import unicodedata
from difflib import get_close_matches
import re
import asyncio
import random
# --- Load .env ---
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
SHEET_KEY_URL = os.getenv("SHEET_KEY_URL")
SHEET_KEY = os.getenv("sheet_key")  # URL trong .env
GUILD_ID = int(os.getenv("GUILD_ID"))
GUILD = discord.Object(id=GUILD_ID)
CHECK_SHEET_URL = os.getenv("CHECK_SHEET_URL")
# ================== CONFIG ==================
VSINH_MAX_CONFLICTS = 46
XGHE_MAX_MEMBERS = 28
STATE_FILE = "vsinh.txt"
# --- Google Sheets ---
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)
check_spreadsheet = client.open_by_url(CHECK_SHEET_URL)
check_sheet = check_spreadsheet.sheet1  # hoặc worksheet("TênSheet") nếu cần
sheetmon = client.open_by_url(SHEET_KEY_URL)
spreadsheet = client.open_by_url(SHEET_KEY)
# Ví dụ tạo một sheet riêng để lưu user đã xác minh
verify_sheet = spreadsheet.worksheet("Sheet1")

def get_name_by_discord_id(user_id: str) -> str:
    try:
        key_values = safe_get_all_values(verify_sheet)[1:]  # bỏ header
        for row in key_values:
            if len(row) >= 3 and row[1].strip() == str(user_id):
                return row[2].strip()  # cột C là họ tên
    except Exception as e:
        print(f"get_name_by_discord_id error: {e}")
    return "<Không rõ tên>"


from datetime import datetime

def write_log(action: str, executor_id: str, detail: str):
    try:
        log_sheet = spreadsheet.worksheet("Logs")  # sheet "Log" phải tồn tại
        executor_name = get_name_by_discord_id(executor_id)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        new_row = [timestamp, executor_name, str(executor_id), action, detail]
        log_sheet.append_row(new_row, value_input_option="RAW")
    except Exception as e:
        print(f"Lỗi ghi log: {e}")

def write_log_TN(action: str, executor_id: str, detail: str):
    try:
        log_sheet = spreadsheet.worksheet("Trực Nhật")  # sheet "Log" phải tồn tại
        executor_name = get_name_by_discord_id(executor_id)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        new_row = [timestamp, executor_name, str(executor_id), action, detail]
        log_sheet.append_row(new_row, value_input_option="RAW")
    except Exception as e:
        print(f"Lỗi ghi log: {e}")


def normalize_text(text: str) -> str:
    """Chuẩn hóa chuỗi: bỏ dấu, về chữ thường, bỏ khoảng trắng thừa"""
    if not text:
        return ""
    text = unicodedata.normalize("NFD", str(text))
    text = text.encode("ascii", "ignore").decode("utf-8")  # bỏ dấu
    text = re.sub(r"\s+", " ", text)  # gom nhiều khoảng trắng thành 1
    return text.strip().lower()


def remove_accents(text: str) -> str:
    if text is None:
        return ""
    nkfd = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in nkfd if not unicodedata.combining(ch)).strip().lower()

# --- Discord Bot ---
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

def safe_get_all_values(sheet, retries=3, delay=2):
    import time
    for i in range(retries):
        try:
            return sheet.get_all_values()
        except Exception as e:
            if i < retries - 1:
                time.sleep(delay)
                continue
            raise e
import time

def safe_update_cell(ws, row, col, value, retries=3, delay=1):
    """
    Cập nhật 1 ô trong worksheet, có retry nếu thất bại.
    ws     : worksheet (ví dụ sheet1)
    row    : số dòng (int)
    col    : số cột (int)
    value  : giá trị muốn ghi vào
    retries: số lần thử lại (mặc định 3)
    delay  : số giây chờ giữa các lần thử
    """
    for i in range(retries):
        try:
            return ws.update_cell(row, col, value)
        except Exception as e:
            if i < retries - 1:
                print(f"safe_update_cell: lỗi {e}, thử lại ({i+1}/{retries})...")
                time.sleep(delay)
                continue
            raise e

# --- Decorator check verified ---
def check_verified():
    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            await interaction.response.defer(ephemeral=False)
            user_id = str(interaction.user.id)

            verified = False  # khai báo mặc định ở đây
            try:
                key_values = safe_get_all_values(verify_sheet)[1:]  # bỏ header
                verified = any(
                    len(row) >= 2 and str(user_id) == str(row[1]).strip()
                    for row in key_values
                )
            except Exception as e:
                print(f"Lỗi check_verified: {e}")

            if not verified:
                await interaction.followup.send(
                    "❌ Bạn không có quyền hạn sử dụng lệnh này",
                    ephemeral=True
                )
                return

            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

def check_verified_TN():
    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            await interaction.response.defer(ephemeral=False)
            user_id = str(interaction.user.id)

            verified = False
            try:
                key_values = safe_get_all_values(verify_sheet)[1:]  # bỏ header
                for i, row in enumerate(key_values, start=2):
                    if not isinstance(row, (list, tuple)):
                        print(f"[check_verified_TN] Dòng {i} không phải list: {row!r}")
                        continue
                    if len(row) > 5:  # phải có ít nhất 5 cột
                        cell = str(row[5]).strip()  # Cột E = index 4
                        if user_id == cell:
                            verified = True
                            break
            except Exception as e:
                print(f"Lỗi check_verified_TN: {e}")

            if not verified:
                await interaction.followup.send(
                    "❌ Bạn không có quyền hạn sử dụng lệnh này",
                    ephemeral=True
                )
                return

            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

STATE_FILE = "vsinh.txt"
XGHE_MAX_MEMBERS = 28

def init_state():
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write("vsinh=\n")
            f.write("xghe=\n")
            f.write("to=\n")
            f.write("not_recent_vsinh=\n")
            f.write("not_recent_xghe=\n")
            f.write("vipham=[]\n") 


def format_name(name: str) -> str:
    """Chuẩn hóa tên: bỏ khoảng trắng thừa + viết hoa chữ cái đầu."""
    return name.strip().title()


def load_state():
    init_state()
    state = {
        "vsinh": [],
        "xghe": [],
        "to": None,
        "all_vsinh": [],
        "not_recent_vsinh": set(),
        "not_recent_xghe": set(),
        "vipham": []
    }

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue  # bỏ qua dòng trống hoặc sai định dạng

            key, val = line.split("=", 1)
            val = val.strip().strip('"')

            if key == "vsinh" and val:
                state["vsinh"] = [format_name(x) for x in val.split(",") if x.strip()]
            elif key == "xghe" and val:
                state["xghe"] = [format_name(x) for x in val.split(",") if x.strip()]
            elif key == "to" and val.isdigit():
                state["to"] = int(val)
            elif key == "not_recent_vsinh" and val:
                state["not_recent_vsinh"] = set(format_name(x) for x in val.split(",") if x.strip())
            elif key == "not_recent_xghe" and val:
                state["not_recent_xghe"] = set(format_name(x) for x in val.split(",") if x.strip())
            elif key == "vipham" and val and val != "[]":
                try:
                    import ast
                    state["vipham"] = ast.literal_eval(val)
                except:
                    state["vipham"] = []
    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write("vsinh=" + ",".join(format_name(x) for x in state.get("vsinh", [])) + "\n")
        f.write("xghe=" + ",".join(format_name(x) for x in state.get("xghe", [])) + "\n")
        f.write("to=" + (str(state["to"]) if state.get("to") else "") + "\n")
        f.write("not_recent_vsinh=" + ",".join(format_name(x) for x in state.get("not_recent_vsinh", set())) + "\n")
        f.write("not_recent_xghe=" + ",".join(format_name(x) for x in state.get("not_recent_xghe", set())) + "\n")
        f.write("vipham=" + str(state.get("vipham", [])) + "\n")
# ================== GOOGLE SHEET HELPERS ==================
def load_data_from_sheet():
    """Load dữ liệu từ sheet, trả về list of dict"""
    load_dotenv()
    gc = gspread.service_account(filename=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    sh = gc.open_by_key(os.getenv("SHEET_ID"))
    ws = sh.sheet1
    return ws.get_all_records()  # list of dict


def candidates_from_data(data, only_male=False, exception_set=None):
    """
    Lấy danh sách học sinh, trả về list of str (tên)
    - only_male: chỉ lấy nam
    - exception_set: set các tên cần loại bỏ
    """
    res = []
    for row in data:
        if isinstance(row, dict):
            name = row.get("Họ tên") or row.get("Name")
            gender = row.get("Giới tính") or row.get("Gender")
        elif isinstance(row, str):
            name = row
            gender = None
        else:
            continue

        if not name:
            continue
        if only_male and gender and gender.lower().strip() != "nam":
            continue
        if exception_set and str(name).strip() in exception_set:
            continue
        res.append(str(name).strip())
    return res


def get_exception_set(data):
    """
    Lấy set tên ở cột 'Ngoại lệ' hoặc 'Exception'
    """
    exceptions = set()
    for row in data:
        if isinstance(row, dict):
            name = row.get("Ngoại Lệ") or row.get("Exception")
        elif isinstance(row, str):
            name = row
        else:
            continue
        if name and str(name).strip():
            exceptions.add(str(name).strip())
    return exceptions


def init_not_recent(state, data, vipham=None):
    """Khởi tạo set not_recent cho VSINH và XGHE."""
    exceptions = get_exception_set(data)
    all_students = candidates_from_data(data, exception_set=exceptions)
    all_male_students = candidates_from_data(data, only_male=True, exception_set=exceptions)

    # VIPHAM hết buổi
    vipham_done = set()
    if vipham:
        for v in vipham:
            if v[1] == 0:
                vipham_done.add(v[0])

    vsinh_done = set(state.get("vsinh_done", []))
    xghe_done = set(state.get("xghe", []))

    # Loại tất cả: ngoại lệ + vipham hết buổi + đã trực
    state["not_recent_vsinh"] = set(all_students) - vsinh_done - vipham_done - exceptions
    state["not_recent_xghe"] = set(all_male_students) - xghe_done - vipham_done - exceptions

    save_state(state)
    return state["not_recent_vsinh"], state["not_recent_xghe"]



def sync_not_recent_vsinh(state, all_students):
    """
    Đồng bộ not_recent_vsinh với danh sách gốc từ sheet.
    Xoá tên dư, giữ đúng số lượng và báo tên nào bị sai.
    """

    # Làm sạch danh sách gốc (xóa khoảng trắng, loại trùng)
    all_students = list({s.strip(): None for s in all_students}.keys())

    # Nếu chưa có trong state thì khởi tạo
    if "not_recent_vsinh" not in state:
        state["not_recent_vsinh"] = set(all_students)
        return

    not_recent = set(state["not_recent_vsinh"])
    valid_set = set(all_students)

    # Tìm tên dư và tên thiếu
    extra = not_recent - valid_set
    missing = valid_set - not_recent

    # In cảnh báo (log/debug)
    if extra:
        print("⚠️ Có tên dư trong not_recent_vsinh:", ", ".join(extra))
    if missing:
        print("⚠️ Có tên chưa có trong not_recent_vsinh:", ", ".join(missing))

    # Đồng bộ lại: chỉ giữ đúng danh sách từ sheet
    state["not_recent_vsinh"] = not_recent & valid_set

# ================== RANDOM VSINH/XGHE ==================


def _clean_all_students(all_students):
    """Trả về list tên đã strip, title-case, loại trùng giữ thứ tự."""
    seen = set()
    out = []
    for s in all_students:
        if s is None:
            continue
        name = str(s).strip()
        if not name:
            continue
        name = name.title()
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out

def _normalize_not_recent(raw, all_students_clean):
    """
    Chuẩn hóa state['not_recent_vsinh'] về set hợp lệ (chỉ chứa tên trong all_students_clean).
    raw có thể là None / set / list / comma string.
    """
    if raw is None:
        return set()
    if isinstance(raw, set):
        s = set(raw)
    elif isinstance(raw, list):
        s = set(raw)
    elif isinstance(raw, str):
        # có thể là "A,B,C"
        s = set(x.strip() for x in raw.split(",") if x.strip())
    else:
        try:
            s = set(raw)
        except Exception:
            s = set()
    # chỉ giữ tên hợp lệ (theo all_students_clean)
    valid = set(all_students_clean)
    return set([x for x in s if x in valid])


def random_vsinh(state, all_students, vipham=None):
    """
    Gộp logic VIPHAM + random VSINH (TH1/TH2) theo yêu cầu của bạn.
    Trả về: (vsinh_queue (5 pairs), vipham_after)
    """
    # --- chuẩn dữ liệu ---
    all_students_clean = _clean_all_students(all_students)
    if len(all_students_clean) < 2:
        raise ValueError("Danh sách học sinh không đủ để random.")

    # chuẩn vipham_in (title-case)
    if vipham is None:
        vipham_in = [v[:] for v in state.get("vipham", [])]
    else:
        vipham_in = [[str(v[0]).strip().title(), int(v[1])] for v in vipham]

    # Active VIPHAM (còn buổi >0) sẽ bị loại khỏi pool random
    vipham_active = set(n for n, b in vipham_in if b > 0)

    # available_students: không có vipham_active
    available_students = [s for s in all_students_clean if s not in vipham_active]
    if not available_students:
        raise ValueError("Không còn học sinh hợp lệ (tất cả đều vi phạm/ngoại lệ).")

    # chuẩn not_recent (chỉ trong available_students)
    raw_not = state.get("not_recent_vsinh")
    not_recent = _normalize_not_recent(raw_not, available_students)
    if not not_recent:
        not_recent = set(available_students)

    # khởi tạo kết quả
    vsinh_queue = [None] * 5
    used_names = set()           # mọi tên đã được đặt tuần này (bao gồm VIPHAM đã xếp)
    vipham_after = []            # vipham còn buổi >0 sau khi trừ
    vsinh_done_vipham = []       # vipham giảm về 0 -> sẽ được thêm vào lịch (tuỳ TH1/TH2)

    # --- Ưu tiên xử lý VIPHAM (chỉ tác động lên slots, không đưa vipham>0 vào pool random) ---
    if vipham_in:
        mot_buoi = [v[:] for v in vipham_in if v[1] == 1]
        nhieu_buoi = [v[:] for v in vipham_in if v[1] >= 2]

        # xử lý nhóm 1 buổi: đặt vào NGÀY 2 (index=1) theo yêu cầu cũ của bạn
        if len(mot_buoi) == 1:
            name = mot_buoi[0][0]
            cand = [x for x in not_recent if x != name and x not in used_names]
            if cand:
                partner = random.choice(list(cand))
                not_recent.discard(partner)
            else:
                partner = random.choice([x for x in available_students if x != name and x not in used_names])
            vsinh_queue[1] = [name, partner]
            used_names.update([name, partner])
            # giảm buổi
            mot_buoi[0][1] = 0

        elif len(mot_buoi) >= 2:
            # lấy 2 đầu để ghép vào ngày 2
            a, b = mot_buoi[0][0], mot_buoi[1][0]
            vsinh_queue[1] = [a, b]
            used_names.update([a, b])
            not_recent.discard(a); not_recent.discard(b)
            mot_buoi[0][1] = 0; mot_buoi[1][1] = 0
            # chuyển phần còn lại (nếu có) sang phân bổ như nhieu_buoi
            for v in mot_buoi[2:]:
                nhieu_buoi.append(v)

        # phân bổ cho nhieu_buoi (>=2 buổi): trừ 2 buổi mỗi lần, đặt vào các ngày trống
        days_left = [0, 2, 3, 4]   # index các ngày còn lại
        random.shuffle(days_left)
        for v in nhieu_buoi:
            name, buoi = v[0], int(v[1])
            while buoi >= 2 and days_left:
                idx = days_left.pop(0)
                cand = [x for x in not_recent if x != name and x not in used_names]
                if cand:
                    partner = random.choice(list(cand))
                    not_recent.discard(partner)
                else:
                    partner = random.choice([x for x in available_students if x != name and x not in used_names])
                vsinh_queue[idx] = [name, partner]
                used_names.update([name, partner])
                buoi -= 2
            # cập nhật vipham_after / done
            if buoi > 0:
                vipham_after.append([name, buoi])
            else:
                vsinh_done_vipham.append(name)

    # loại các used_names khỏi not_recent để tránh chọn lại
    for u in list(used_names):
        if u in not_recent:
            not_recent.discard(u)

    # --- Bây giờ lấp các ngày trống ---
    remaining_slots = sum(1 for x in vsinh_queue if x is None)
    if remaining_slots == 0:
        # đã đủ
        final_used = set(used_names)
        # cập nhật state
        state["vsinh_queue"] = [list(pair) for pair in vsinh_queue]
        # note: vsinh cập nhật phía dưới
    else:
        # TH1: nếu not_recent đủ lớn (>=10) thì lấy trực tiếp các người cần cho remaining_slots
        if len(not_recent) >= 10:
            needed = remaining_slots * 2
            picks = random.sample(list(not_recent), needed)
            # chia thành cặp
            pairs = [[picks[i], picks[i+1]] for i in range(0, needed, 2)]
            # fill vào slots theo thứ tự
            pi = 0
            for i in range(5):
                if vsinh_queue[i] is None:
                    vsinh_queue[i] = pairs[pi]
                    pi += 1
            # cập nhật state: thêm vào lịch sử (cộng dồn) và trừ not_recent
            state.setdefault("vsinh", [])
            for p in picks:
                if p not in state["vsinh"]:
                    state["vsinh"].append(p)
                if p in not_recent:
                    not_recent.discard(p)
                used_names.add(p)
            final_used = set(used_names)
            state["not_recent_vsinh"] = list(not_recent)
            state["vsinh_queue"] = [list(pair) for pair in vsinh_queue]
            save_state(state)
        else:
            # TH2: not_recent < 10 -> theo đúng flow bạn mô tả:
            # - ghép cặp từ not_recent đến khi chỉ còn 1 (hoặc cạn)
            # - nếu còn last_student -> reset_pool = all_students - last_student - các cặp đã random
            # - chọn partner từ reset_pool, ghép, tiếp tục tạo đủ remaining_slots
            pool = list(not_recent)
            random.shuffle(pool)
            pairs_made = []
            final_used = set(used_names)  # bắt đầu với used từ vipham
            # ghép từ pool
            while len(pool) >= 2 and len(pairs_made) < remaining_slots:
                a = pool.pop()
                b = pool.pop()
                pairs_made.append([a, b])
                final_used.update([a, b])
            last_student = None
            if len(pairs_made) < remaining_slots and len(pool) == 1:
                last_student = pool.pop()

            # tạo reset_pool = available_students - final_used - {last_student}
            reset_pool = set(available_students) - final_used
            if last_student:
                # ensure last_student is not in reset_pool
                reset_pool.discard(last_student)

            # nếu cần ghép last_student
            if last_student and len(pairs_made) < remaining_slots:
                # chọn partner tránh trùng trong final_used nếu có thể
                candidates = [x for x in reset_pool if x not in final_used and x != last_student]
                if not candidates:
                    candidates = [x for x in available_students if x not in final_used and x != last_student]
                if not candidates:
                    # fallback (lớp nhỏ)
                    candidates = [x for x in available_students if x != last_student]
                partner = random.choice(candidates)
                pairs_made.append([last_student, partner])
                final_used.update([last_student, partner])
                if partner in reset_pool:
                    reset_pool.discard(partner)

            # bây giờ bổ sung thêm các cặp từ reset_pool hoặc từ available_students để đủ remaining_slots
            while len(pairs_made) < remaining_slots:
                # đảm bảo lấy 2 khác nhau không trùng final_used nếu có thể
                candidate_pool = [x for x in reset_pool if x not in final_used]
                if len(candidate_pool) >= 2:
                    a, b = random.sample(candidate_pool, 2)
                else:
                    # refill từ available_students - final_used
                    candidate_pool = [x for x in available_students if x not in final_used]
                    if len(candidate_pool) >= 2:
                        a, b = random.sample(candidate_pool, 2)
                    else:
                        # cuối cùng fallback: chọn bất kỳ 2 (có thể trùng tên nếu lớp quá nhỏ)
                        a, b = random.sample(available_students, 2)
                pairs_made.append([a, b])
                final_used.update([a, b])
                if a in reset_pool: reset_pool.discard(a)
                if b in reset_pool: reset_pool.discard(b)

            # now fill pairs_made into vsinh_queue slots
            pi = 0
            for i in range(5):
                if vsinh_queue[i] is None:
                    vsinh_queue[i] = pairs_made[pi]
                    pi += 1

            # TH2 yêu cầu: reset toàn bộ state["vsinh"] = danh sách 10 tên tuần này (không giữ lịch cũ)
            flattened = [n for pair in vsinh_queue for n in pair if n]
            state["vsinh"] = flattened[:]   # overwrite (reset)
            # cập nhật not_recent dựa trên available_students - final_used
            state["not_recent_vsinh"] = list(set(available_students) - final_used)
            final_used = set(final_used)
            state["vsinh_queue"] = [list(pair) for pair in vsinh_queue]
            save_state(state)

    # --- Sau khi lấp đầy tuần, xử lý vipham_done: chỉ thêm VIPHAM đã hết buổi (nếu chưa thêm) ---
    if vsinh_done_vipham:
        state.setdefault("vsinh", [])
        for n in vsinh_done_vipham:
            if n not in state["vsinh"]:
                state["vsinh"].append(n)
            # loại khỏi not_recent nếu còn
            if "not_recent_vsinh" in state:
                if isinstance(state["not_recent_vsinh"], list):
                    if n in state["not_recent_vsinh"]:
                        lst = list(state["not_recent_vsinh"])
                        lst.remove(n)
                        state["not_recent_vsinh"] = lst
                else:
                    try:
                        state["not_recent_vsinh"].remove(n)
                    except Exception:
                        pass

    # cập nhật vipham (những còn >0 buổi)
    # vipham_after đã được nhóm ra trong xử lý nhieu_buoi ở trên; nếu không, giữ những vipham không xử lý
    if not vipham_after:
        # nếu chưa set vipham_after từ nhieu_buoi, build từ vipham_in
        vipham_after = [ [n, b] for n, b in vipham_in if b > 0 ]

    state["vipham"] = [ [n, b] for n, b in vipham_after ]

    # đảm bảo lưu state cuối cùng
    save_state(state)
    return state.get("vsinh_queue", vsinh_queue), vipham_after



def random_two_xghe(state, data):
    """
    Chọn 2 học sinh nam XGHE từ not_recent_xghe.
    Reset nếu không đủ 2.
    Luôn loại 2 người vừa chọn ra khỏi not_recent_xghe.
    """
    exceptions = get_exception_set(data)
    all_male_students = candidates_from_data(data, only_male=True, exception_set=exceptions)

    if not all_male_students:
        raise ValueError("Không tìm thấy học sinh nam")

    # Khởi tạo not_recent_xghe nếu chưa có hoặc reset nếu còn <2
    if "not_recent_xghe" not in state or len(state["not_recent_xghe"]) < 2:
        state["not_recent_xghe"] = set(all_male_students)
    elif isinstance(state["not_recent_xghe"], list):
        state["not_recent_xghe"] = set(state["not_recent_xghe"])

    # Chọn 2 bạn ngẫu nhiên từ not_recent_xghe
    picks = random.sample(list(state["not_recent_xghe"]), 2)

    # Loại 2 người vừa chọn ra khỏi not_recent_xghe
    state["not_recent_xghe"] -= set(picks)

    # Cập nhật state
    state["xghe"] = picks
    save_state(state)

    # Chuyển not_recent_xghe về list để nhất quán
    state["not_recent_xghe"] = list(state["not_recent_xghe"])

    return picks

def next_to(state):
    if not state["to"]:
        return None
    nxt = state["to"] + 1
    if nxt > 4:
        nxt = 1
    state["to"] = nxt
    return nxt
@bot.tree.command(name="vsinh", description="Random VSINH (5 ngày) + XGHE + Tổ", guild=GUILD)
@check_verified_TN()
async def vsinh(interaction: discord.Interaction):
    try:
        # --- Load state và dữ liệu ---
        state = load_state()
        data = load_data_from_sheet()
        exception_set = get_exception_set(data)  # Set ngoại lệ (không random)

        # --- Nếu chưa có tổ, hỏi user ---
        if not state.get("to"):
            await interaction.followup.send("❓ Trực tổng vệ sinh đang ở tổ nào vậy? (Nhập số 1–4)")

            def check(msg: discord.Message):
                return (
                    msg.author == interaction.user
                    and msg.content.isdigit()
                    and 1 <= int(msg.content) <= 4
                )

            try:
                msg = await bot.wait_for("message", check=check, timeout=30)
                state["to"] = int(msg.content)
                save_state(state)
                await interaction.followup.send(f"✅ Đã lưu tổ {state['to']} làm tổ trực hiện tại.")
            except asyncio.TimeoutError:
                await interaction.followup.send("⌛ Bạn không trả lời kịp. Hãy gọi lại lệnh `/vsinh`.")
                return
            except Exception as e:
                await interaction.followup.send(f"❌ Lỗi khi nhập tổ: {e}")
                return

        # --- Khởi tạo not_recent từ sheet ---
        init_not_recent(state, data)

        # --- Lấy danh sách học sinh (loại ngoại lệ) ---
        all_students = candidates_from_data(data, exception_set=exception_set)

        # --- Random VSINH (5 ngày) ---
        vipham = state.get("vipham", [])
        vsinh_queue, vipham_after = random_vsinh(state, all_students, vipham)

        vsinh_all = []
        for day, picks in enumerate(vsinh_queue, start=1):
            if picks:
                vsinh_all.append(f"Ngày {day}: {', '.join(p for p in picks if p)}")

        messages = []
        messages.append("🧹 Trực Vệ Sinh:\n" + "\n".join(vsinh_all))

        # --- XGHE ---
        xghe_picks = random_two_xghe(state, data)
        messages.append(f"🪑 Xếp Ghế: {', '.join(p for p in xghe_picks if p)}")

        # --- Tổ trực (chuyển sang tổ tiếp theo) ---
        if not state.get("to"):
            state["to"] = 1
        current_to = state["to"] + 1
        if current_to > 4:
            current_to = 1
        state["to"] = current_to
        save_state(state)
        messages.append(f"👥 Tổ trực: Tổ {current_to}")

        # --- Ghi log ---
        vsinh_str = "; ".join(" & ".join(pair) for pair in vsinh_queue if pair)
        xghe_str = ", ".join(p for p in xghe_picks if p)
        detail = f"Danh sách trực nhật tuần này,{vsinh_str},{xghe_str},👥 Tổ trực: Tổ {current_to}"
        write_log_TN("Vsinh", str(interaction.user.id), detail)

        # --- Gửi kết quả ---
        await interaction.followup.send("\n".join(messages))

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi chạy lệnh /vsinh: {e}")


@bot.event
async def on_ready():
    print(f"✅ Bot {bot.user} đã online!")
    try:
        synced = await bot.tree.sync(guild=GUILD)
        #print(f"Đã sync {len(synced)} lệnh slash trong guild {GUILD_ID}")
    except Exception as e:
        print(f"Lỗi sync lệnh: {e}")

bot.run(TOKEN)