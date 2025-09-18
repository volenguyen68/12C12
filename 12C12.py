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


@bot.event
async def on_ready():
    print(f"✅ Bot {bot.user} đã online!")
    try:
        synced = await bot.tree.sync(guild=GUILD)
        #print(f"Đã sync {len(synced)} lệnh slash trong guild {GUILD_ID}")
    except Exception as e:
        print(f"Lỗi sync lệnh: {e}")


async def update_mark(interaction, sheet_name: str, stt: int, cot: int, value: str):
    try:
        sheet = sheetmon.worksheet(sheet_name)
    except Exception as e:
        await interaction.followup.send(f"❌ Không thể mở sheet '{sheet_name}': {e}")
        return

    try:
        # Tìm hàng theo STT
        data = sheet.col_values(1)
        row_index = None
        for i, val in enumerate(data, start=1):
            if str(val).strip() == str(stt):
                row_index = i
                break

        if row_index is None:
            await interaction.followup.send(f"❌ Không tìm thấy STT {stt} trong bảng '{sheet_name}'")
            return

        # Map cot -> cột trên sheet (cot=1 -> D, cot=2 -> E...)
        col_index = 3 + cot
        student_name = sheet.cell(row_index, 2).value or "<không có tên>"

        # Mapping đợt
        if cot in (1, 2, 3):
            dot = f"15 phút, đợt {cot}"
        elif cot == 4:
            dot = "đợt giữa kì"
        elif cot == 5:
            dot = "đợt cuối kì"
        else:
            dot = f"đợt {cot}"

        current_value = sheet.cell(row_index, col_index).value

        # --- Xử lý giá trị nhập ---
        try:
            numeric_value = float(value)
        except ValueError:
            await interaction.followup.send("❌ Giá trị không hợp lệ. Chỉ nhập số.", ephemeral=True)
            return

        if numeric_value == 8:
            # --- Đánh X ---
            if current_value == "X":
                await interaction.followup.send(
                    f"⚠️ [{sheet_name}] STT {stt}, {student_name} đã được đánh X trước đó ({dot}) !"
                )
                return
            sheet.update_cell(row_index, col_index, "X")
            detail = f"Đã đánh dấu X STT {stt}, {student_name}, {dot} trong [{sheet_name}]"
            write_log("Đánh dấu", str(interaction.user.id), detail)
            await interaction.followup.send(
                f"✅ [{sheet_name}] Đã đánh dấu X vào STT {stt}, {student_name}, {dot}"
            )

        elif numeric_value >= 9:
            # --- Ghi số vào ô ---
            sheet.update_cell(row_index, col_index, str(int(numeric_value)))
            detail = f"Đã ghi {int(numeric_value)} điểm STT {stt}, {student_name}, {dot} trong [{sheet_name}]"
            write_log("Ghi điểm", str(interaction.user.id), detail)
            await interaction.followup.send(
                f"✅ [{sheet_name}] Đã ghi {int(numeric_value)} điểm vào STT {stt}, {student_name}, {dot}"
            )
        else:
            # --- Số không hợp lệ ---
            await interaction.followup.send(
                "❌ Chỉ được đánh dấu X (nhập 8) hoặc ghi số ≥9",
                ephemeral=True
            )

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi cập nhật sheet: {e}")


# --- Slash command duy nhất /mark ---
@bot.tree.command(name="mark", description="Đánh dấu X vào bảng điểm")#,guild=GUILD)
@app_commands.describe(
    mon="Chọn môn học",
    stt="STT",
    cot="Đợt 1,2,3,4=1,2,3,4 GK=6 CK = 8",
    value="Điểm"
)
@app_commands.choices(mon=[
    app_commands.Choice(name="toan", value="Môn Toán"),
    app_commands.Choice(name="hoa", value="Môn Hóa"),
    app_commands.Choice(name="anh", value="Môn Anh"),
    app_commands.Choice(name="qp", value="Môn QP"),
    app_commands.Choice(name="tin", value="Môn Tin"),
    app_commands.Choice(name="li", value="Môn Lí"),
    app_commands.Choice(name="van", value="Môn Văn"),
    app_commands.Choice(name="sinh", value="Môn Sinh"),
    app_commands.Choice(name="su", value="Môn Sử")
])
@check_verified()
async def mark(interaction: discord.Interaction, mon: app_commands.Choice[str], stt: int, cot: int, value: str):
    await update_mark(interaction, mon.value, stt, cot, value)


# --- Redeem command (fixed, debug, full scan) ---

def normalize_text(text: str) -> str:
    """Chuẩn hóa chuỗi để so sánh"""
    if text is None:
        return ""
    return unicodedata.normalize("NFC", str(text)).strip().lower()

@bot.tree.command(name="redeem2", description="Nhập key để kích hoạt", guild=GUILD)
@app_commands.describe(
    key_code="Nhập key của bạn"
)
async def redeem(interaction: discord.Interaction, key_code: str):
    await interaction.response.defer(ephemeral=False)

    try:
        key_sheet = spreadsheet.sheet1
        key_values = key_sheet.get_all_values()
        if not key_values:
            await interaction.followup.send("❌ Sheet key trống hoặc không đọc được dữ liệu.")
            return

        found = False
        for i, row in enumerate(key_values[1:], start=2):  # bỏ header
            row_len = len(row)
            colA = row[0].strip() if row_len > 0 else ""
            colE = row[4].strip() if row_len > 4 else ""

            target_cols = None  # (ID_col, Name_col, STT_col)

            if colA == key_code.strip():
                found = True
                existing_id = row[1].strip() if row_len > 1 else ""
                if existing_id:
                    await interaction.followup.send("❌ Key đã được sử dụng.")
                    return
                target_cols = (2, 3, 4)  # B, C, D

            elif colE == key_code.strip():
                found = True
                existing_id = row[5].strip() if row_len > 5 else ""
                if existing_id:
                    await interaction.followup.send("❌ Key đã được sử dụng.")
                    return
                target_cols = (6, 7, 8)  # F, G, H

            if target_cols:
                # --- Key đúng, hỏi họ tên ---
                await interaction.followup.send("Vui lòng nhập HỌ VÀ TÊN đầy đủ của bạn:")

                def check(msg: discord.Message):
                    return msg.author.id == interaction.user.id and msg.channel.id == interaction.channel.id

                try:
                    msg = await bot.wait_for("message", check=check, timeout=60.0)
                    hoten_input = normalize_text(msg.content.strip())
                except asyncio.TimeoutError:
                    await interaction.followup.send("⏰ Bạn đã hết thời gian nhập họ tên.", ephemeral=False)
                    return

                # --- Lấy dữ liệu từ sheet check ---
                check_values = check_sheet.get_all_values()[1:]
                stt_found, matched_name = None, None

                for row_check in check_values:
                    if len(row_check) >= 2:
                        name_in_sheet = normalize_text(row_check[1])
                        if name_in_sheet == hoten_input:
                            stt_found = row_check[0].strip()
                            matched_name = row_check[1].strip()
                            break

                if not stt_found:
                    await interaction.followup.send(
                        "❌ Không tìm thấy họ và tên trong danh sách.",
                        ephemeral=True
                    )
                    return

                # --- Ghi dữ liệu vào sheet key ---
                key_sheet.update_cell(i, target_cols[0], str(interaction.user.id))
                key_sheet.update_cell(i, target_cols[1], matched_name)
                key_sheet.update_cell(i, target_cols[2], stt_found)

                await interaction.followup.send(
                    f"✅ Key kích hoạt thành công!\n👤 Họ và tên: {matched_name}\n🔢 STT: {stt_found}",
                    ephemeral=False
                )
                break

        if not found:
            await interaction.followup.send("❌ Key không hợp lệ", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi kiểm tra key: {e}", ephemeral=True)

@bot.tree.command(name="cong2", description="Cộng điểm thưởng cho học sinh",guild=GUILD)
@app_commands.describe(
    mon="Chọn môn học",
    stt="STT",
    diem="Số điểm muốn cộng (âm để trừ)"
)
@app_commands.choices(mon=[
    app_commands.Choice(name="toan", value="Môn Toán"),
    app_commands.Choice(name="hoa", value="Môn Hóa"),
    app_commands.Choice(name="anh", value="Môn Anh"),
    app_commands.Choice(name="qp", value="Môn QP"),
    app_commands.Choice(name="tin", value="Môn Tin"),
    app_commands.Choice(name="li", value="Môn Lí"),
    app_commands.Choice(name="van", value="Môn Văn"),
    app_commands.Choice(name="sinh", value="Môn Sinh"),
    app_commands.Choice(name="su", value="Môn Sử")
])
@check_verified()
async def cong(interaction: discord.Interaction, mon: app_commands.Choice[str], stt: int, diem: str):
    try:
        diem = float(diem.replace(",", "."))
        sheet = sheetmon.worksheet(mon.value)
        data = sheet.col_values(1)  # cột A
        row_index = None
        for i, val in enumerate(data, start=1):
            if str(val).strip() == str(stt):
                row_index = i
                break

        if row_index is None:
            await interaction.followup.send(
                f"❌ Không tìm thấy stt {stt} trong cột A của '{mon.value}'"
            )
            return

        # Cột M = điểm
        col_diem = 13

        current_value = sheet.cell(row_index, col_diem).value
        if not current_value or current_value.strip() == "":
            current_value = 0.0
        else:
            try:
                current_value = float(str(current_value).replace(",", ".").replace("+", "").strip())
            except ValueError:
                current_value = 0.0

        new_value = current_value + diem
        student_name = sheet.cell(row_index, 2).value or "<không có tên>"

        # Hàm format hiển thị điểm (giữ dấu + và bỏ số 0 thừa sau thập phân)

        def fmt(val: float) -> str:
            if val > 0:
                s = f"+{val:.2f}".rstrip("0").rstrip(".")
            elif val < 0:
                s = f"{val:.2f}".rstrip("0").rstrip(".")
            else:
                s = "0"
            return s.replace(".", ",")  # khi update sheet thì dùng dấu phẩy

        display_value = fmt(new_value)
        old_value_display = fmt(current_value)

        # --- Nếu là trừ điểm ---
        if new_value < current_value:
            await interaction.followup.send(
                f"⚠️ Có sự thay đổi về số điểm từ {old_value_display} thành {display_value}. "
                f"Vui lòng nhập **lý do** trong vòng 60 giây."
            )

            def check(msg: discord.Message):
                return msg.author.id == interaction.user.id and msg.channel == interaction.channel

            try:
                msg = await bot.wait_for("message", check=check, timeout=60.0)
                ly_do = msg.content.strip()

                # Update điểm
                sheet.update_cell(row_index, col_diem, display_value)

                await interaction.followup.send(
                    f"✅ [{mon.value}] Đã TRỪ {abs(diem)} điểm cho STT {stt}, {student_name}. "
                    f"Số điểm mới: {display_value}. "
                    f"Lý do đã ghi vào Log."
                )

                # --- Ghi log ---
                detail = (f"TRỪ {abs(diem)} điểm cho STT {stt}, {student_name}, "
                          f"điểm từ {old_value_display} -> {display_value}, "
                          f"Lý do: {ly_do}, [{mon.value}]")
                write_log("Trừ", str(interaction.user.id), detail)
                return

            except asyncio.TimeoutError:
                await interaction.followup.send("⏰ Hết thời gian nhập lý do. Thao tác bị hủy.")
                return

        # --- Nếu cộng hoặc giữ nguyên ---
        sheet.update_cell(row_index, col_diem, display_value)

        await interaction.followup.send(
            f"✅ [{mon.value}] Đã cộng {diem} điểm cho STT {stt}, {student_name}, môn {mon.value}. "
            f"Tổng điểm hiện tại: {display_value}"
        )

        # --- Ghi log ---
        detail = (f"CỘNG {diem} điểm cho STT {stt}, {student_name}, [{mon.value}], "
                  f"tổng điểm hiện tại: {display_value}")
        write_log("Cộng", str(interaction.user.id), detail)

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi cộng điểm: {e}")


@bot.tree.command(name="xoa", description="Xóa đánh dấu X khỏi bảng điểm")#,guild=GUILD)
@app_commands.describe(
    mon="Chọn môn học",
    stt="STT học sinh",
    dot="Đợt kiểm tra (1,2,3=15p; 4=GK; 5=CK)"
)
@app_commands.choices(mon=[
    app_commands.Choice(name="toan", value="Môn Toán"),
    app_commands.Choice(name="hoa", value="Môn Hóa"),
    app_commands.Choice(name="anh", value="Môn Anh"),
    app_commands.Choice(name="qp", value="Môn QP"),
    app_commands.Choice(name="tin", value="Môn Tin"),
    app_commands.Choice(name="li", value="Môn Lí"),
    app_commands.Choice(name="van", value="Môn Văn"),
    app_commands.Choice(name="sinh", value="Môn Sinh"),
    app_commands.Choice(name="su", value="Môn Sử")
])
@check_verified()
async def xoa(interaction: discord.Interaction, mon: app_commands.Choice[str], stt: int, dot: int):
    try:
        sheet = sheetmon.worksheet(mon.value)
        data = sheet.col_values(1)  # cột A
        row_index = None
        for i, val in enumerate(data, start=1):
            if str(val).strip() == str(stt):
                row_index = i
                break

        if row_index is None:
            await interaction.followup.send(
                f"❌ Không tìm thấy STT {stt} trong cột A của '{mon.value}'"
            )
            return

        # Map cột: dot=1 -> D (4), dot=2 -> E (5), dot=3 -> F (6), dot=4 -> G (7), dot=5 -> H (8)
        col_index = 3 + dot

        student_name = sheet.cell(row_index, 2).value or "<không có tên>"
        current_value = sheet.cell(row_index, col_index).value

        # Nếu ô đã có X thì xóa, nếu không thì báo
        if current_value == "X":
            sheet.update_cell(row_index, col_index, "")
            detail = f"Đã xóa dấu X STT {stt}, {student_name}, {dot} trong [{mon.value}]"
            write_log("Xóa", str(interaction.user.id), detail)
            await interaction.followup.send(
                f"🗑️ [{mon.value}] Đã xóa X ở STT {stt}, tên học sinh: {student_name}, đợt {dot}"
            )
        else:
            await interaction.followup.send(
                f"⚠️ [{mon.value}] STT {stt}, tên học sinh: {student_name} chưa có X ở đợt {dot}"
            )
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi xóa: {e}")


STATE_FILE = "vsinh.txt"
XGHE_MAX_MEMBERS = 28

# ================== FILE STATE HELPERS ==================
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
    # Trả về list duy nhất, đã strip và giữ thứ tự
    seen = set()
    out = []
    for s in all_students:
        if s is None:
            continue
        name = str(s).strip()
        if not name:
            continue
        if name not in seen:
            out.append(name)
            seen.add(name)
    return out

def _normalize_not_recent(raw, all_students_clean):
    """
    Chuyển raw state["not_recent_vsinh"] thành set of clean names.
    Nếu raw None/empty -> trả về toàn bộ all_students_clean as set.
    """
    if raw is None:
        return set(all_students_clean)
    if isinstance(raw, str):
        # có thể bị lưu thành string "A,B,C"
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return set(parts) if parts else set(all_students_clean)
    if isinstance(raw, (list, tuple, set)):
        parts = [str(x).strip() for x in raw if x and str(x).strip()]
        return set(parts) if parts else set(all_students_clean)
    # fallback
    return set(all_students_clean)

def random_two_vsinh(state, all_students):
    """
    Trả về 1 cặp ['A','B'] cho 1 ngày và cập nhật state.
    Nếu not_recent_vsinh >= 10: chọn 2 ngẫu nhiên.
    Nếu not_recent_vsinh < 10: làm theo quy trình 'pair_tempo' + reset + tạo đủ 5 cặp,
    lưu 5 cặp vào state['vsinh'] (flatten 10 tên) và state['vsinh_queue'] (list of [a,b]),
    rồi trả về cặp đầu tiên. Lần gọi tiếp sẽ trả các cặp còn lại từ queue.
    """
    # Chuẩn hoá all_students
    all_students_clean = _clean_all_students(all_students)
    if len(all_students_clean) < 2:
        raise ValueError("Danh sách học sinh không đủ để random.")

    # Lấy not_recent từ state, đảm bảo là set sạch
    raw_not_recent = state.get("not_recent_vsinh")
    not_recent = _normalize_not_recent(raw_not_recent, all_students_clean)

    # Nếu rỗng -> reset toàn bộ
    if not not_recent:
        not_recent = set(all_students_clean)

    # Nếu đã có queue (đợt reset trước) thì phục vụ từ queue trước
    queue = state.get("vsinh_queue")
    if isinstance(queue, list) and queue:
        pair = queue.pop(0)  # pair is list like ['A','B']
        # cập nhật queue vào state
        state["vsinh_queue"] = queue
        # lưu not_recent nếu cần (không thay đổi ở đây vì nó đã được cập nhật khi tạo queue)
        save_state(state)
        return [str(pair[0]).strip(), str(pair[1]).strip()]

    # TH1: đủ >=10 -> chọn 2 người cho 1 ngày
    if len(not_recent) >= 10:
        picks = random.sample(list(not_recent), 2)
        # cập nhật not_recent và lưu history
        not_recent -= set(picks)
        # giữ state["vsinh"] là danh sách tên (flatten) để save_state không bị sai định dạng
        state.setdefault("vsinh", [])
        state["vsinh"].extend(picks)
        state["not_recent_vsinh"] = not_recent
        save_state(state)
        return [picks[0], picks[1]]

    # TH2: not_recent < 10 -> xử lý pair_tempo -> reset -> tạo đủ 5 cặp -> lưu vào queue
    pool = list(not_recent)
    random.shuffle(pool)
    pair_tempo = []   # list of [a,b]
    used = set()

    # tạo cặp tạm đến khi còn <=1 người
    while len(pool) >= 2:
        a = pool.pop()
        b = pool.pop()
        pair_tempo.append([a, b])
        used.update([a, b])

    last_student = None
    if len(pool) == 1:
        last_student = pool.pop()

    # reset vsinh history (the requirement: reset vsinh khi reset not_recent)
    state["vsinh"] = []

    # reset_pool = all_students - (last_student if any) - used_in_pair_tempo
    reset_pool = set(all_students_clean) - used
    if last_student:
        reset_pool.discard(last_student)

    # Nếu reset_pool rỗng, fallback: reset_pool = all_students_clean - used (để có chọn)
    if not reset_pool and last_student:
        reset_pool = set(all_students_clean) - used
        reset_pool.discard(last_student)

    final_pairs = []
    final_used = set(used)  # bắt đầu với những người đã dùng trong pair_tempo (theo mô tạm)
    # Theo yêu cầu bạn: giữ pair_tempo, sau reset ghép last_student và tiếp tục
    # (pair_tempo vẫn tồn tại và sẽ được trở thành một phần của final_pairs)
    final_pairs.extend([list(p) for p in pair_tempo])
    # final_used đã có used

    # Nếu có last_student: chọn partner từ reset_pool
    if last_student:
        if not reset_pool:
            # fallback: lấy từ all_students_clean - final_used - {last_student}
            candidates = [x for x in all_students_clean if x not in final_used and x != last_student]
            if not candidates:
                # cuối cùng lấy bất kỳ khác last_student
                candidates = [x for x in all_students_clean if x != last_student]
            partner = random.choice(candidates)
        else:
            partner = random.choice(list(reset_pool))
        final_pairs.append([last_student, partner])
        final_used.update([last_student, partner])
        # loại partner khỏi reset_pool nếu có
        if partner in reset_pool:
            reset_pool.discard(partner)

    # Bây giờ bổ sung thêm cặp từ reset_pool cho đủ 5 cặp
    # Nếu không đủ trong reset_pool, refill từ all_students_clean - final_used
    while len(final_pairs) < 5:
        # nếu không đủ 2 người trong reset_pool -> refill
        if len(reset_pool) < 2:
            reset_pool = set(all_students_clean) - final_used
        if len(reset_pool) >= 2:
            a, b = random.sample(list(reset_pool), 2)
            final_pairs.append([a, b])
            final_used.update([a, b])
            reset_pool.discard(a); reset_pool.discard(b)
        else:
            # fallback: tìm trong all_students_clean những người chưa dùng
            candidates = [x for x in all_students_clean if x not in final_used]
            if len(candidates) >= 2:
                a, b = random.sample(candidates, 2)
                final_pairs.append([a, b])
                final_used.update([a, b])
            else:
                # cuối cùng cho phép tái sử dụng (nếu class quá nhỏ)
                a, b = random.sample(all_students_clean, 2)
                final_pairs.append([a, b])
                final_used.update([a, b])

    # Sau khi có final_pairs (5 cặp), lưu vào state:
    # - state["vsinh"] lưu flattened list tên (để save_state ghi file đúng)
    flattened = [name for pair in final_pairs for name in pair]
    state["vsinh"] = flattened[:]  # lưu toàn bộ 10 tên (theo thứ tự các cặp)
    # - state["vsinh_queue"] lưu list of pairs để trả dần từng ngày
    state["vsinh_queue"] = [list(pair) for pair in final_pairs]

    # Cập nhật not_recent_vsinh = all_students_clean - final_used
    remaining = set(all_students_clean) - final_used
    state["not_recent_vsinh"] = remaining

    # Lưu và trả về cặp đầu tiên (pop từ queue)
    first_pair = state["vsinh_queue"].pop(0)
    # cập nhật queue trong state (còn 4 cặp để gọi tiếp trong các ngày sau)
    state["vsinh_queue"] = state["vsinh_queue"]
    save_state(state)

    return [str(first_pair[0]).strip(), str(first_pair[1]).strip()]


import random

def tao_lich_vsinh(state, all_students, vipham, to=2):
    """
    Sinh lịch trực vệ sinh cho 5 ngày:
    - VIPHAM: trừ buổi, chỉ lưu vsinh_done nếu buổi = 0
    - Những ngày trống: dùng random_two_vsinh(state, all_students)
    - Xuất file vsinh.txt với vsinh=, xghe=, not_recent_vsinh=, not_recent_xghe=
    """

    all_students_clean = [s.strip().title() for s in all_students if s.strip()]
    vsinh_queue = [None] * 5

    # --- Xử lý VIPHAM ---
    vsinh_done_vipham = []
    if vipham:
        mot_buoi = [v[:] for v in vipham if v[1] == 1]
        nhieu_buoi = [v[:] for v in vipham if v[1] >= 2]

        # Ngày 2 (thứ 3)
        if len(mot_buoi) == 1:
            ten = mot_buoi[0][0]
            partner = random.choice([s for s in all_students_clean if s != ten])
            vsinh_queue[1] = [ten, partner]
            mot_buoi[0][1] = 0
        elif len(mot_buoi) == 2:
            vsinh_queue[1] = [mot_buoi[0][0], mot_buoi[1][0]]
            mot_buoi[0][1] = 0
            mot_buoi[1][1] = 0
        elif len(mot_buoi) >= 3:
            chon = random.sample(mot_buoi, 2)
            vsinh_queue[1] = [chon[0][0], chon[1][0]]
            for v in mot_buoi:
                if v[0] in [chon[0][0], chon[1][0]]:
                    v[1] = 0
                else:
                    nhieu_buoi.append(v)

        # Phân bổ nhieu_buoi
        ngay_con_lai = [0, 2, 3, 4]
        random.shuffle(ngay_con_lai)
        for v in nhieu_buoi:
            while v[1] >= 2 and ngay_con_lai:
                idx = ngay_con_lai.pop(0)
                partner = random.choice([s for s in all_students_clean if s != v[0]])
                vsinh_queue[idx] = [v[0], partner]
                v[1] -= 2

        # Cập nhật VIPHAM sau khi trừ
        vipham_after = []
        for v in mot_buoi + nhieu_buoi:
            if v[1] > 0:
                vipham_after.append(v)
            else:
                vsinh_done_vipham.append(v[0])
    else:
        vipham_after = []

    # --- Lấp các ngày trống bằng random_two_vsinh ---
    for i in range(5):
        if vsinh_queue[i] is None:
            pair = random_two_vsinh(state, all_students_clean)
            vsinh_queue[i] = pair

    # --- Tách vsinh_done từ queue và VIPHAM hết buổi ---
    random_names = [x for pair in vsinh_queue for x in pair if x]
    state["vsinh"] = list(set(random_names + vsinh_done_vipham))

    # --- Tách nam để xghe (giữ logic cũ) ---
    # Ví dụ: lấy những tên male chưa trực xghe, tạm để random
    all_male = [s for s in all_students_clean if s[-1].isalpha()]  # placeholder logic
    state["xghe"] = random.sample(all_male, k=min(to, len(all_male)))

    # --- Cập nhật not_recent ---
    # Cập nhật not_recent
    state["not_recent_vsinh"] = [s for s in all_students_clean if s not in state["vsinh"]]
    state["not_recent_xghe"] = [s for s in all_students_clean if s not in state["xghe"]]


    # --- Lưu state ---
    state["vsinh_queue"] = vsinh_queue
    state["vipham"] = vipham_after
    save_state(state)

    # --- Xuất file vsinh.txt ---
    with open("vsinh.txt", "w", encoding="utf-8") as f:
        f.write(f"vsinh={','.join(state['vsinh'])}\n")
        f.write(f"xghe={','.join(state['xghe'])}\n")
        f.write(f"to={to}\n")
        f.write(f"not_recent_vsinh={','.join(state['not_recent_vsinh'])}\n")
        f.write(f"not_recent_xghe={','.join(state['not_recent_xghe'])}\n")

    return vsinh_queue, vipham_after







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

# ================== SLASH COMMAND /VSINH ==================
@bot.tree.command(name="vsinh", description="Random VSINH (5 ngày) + XGHE + Tổ", guild=GUILD)
@check_verified_TN()
async def vsinh(interaction: discord.Interaction):
    try:
        # --- Load state và dữ liệu từ sheet ---
        state = load_state()
        data = load_data_from_sheet()
        exception_set = get_exception_set(data)  # Set các tên ngoại lệ

        # --- Nếu chưa có tổ, hỏi user ---
        if not state["to"]:
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

        # --- Lấy danh sách tất cả học sinh loại bỏ ngoại lệ ---
        all_students = candidates_from_data(data, exception_set=exception_set)

        messages = []

 # --- VSINH 5 ngày ---

        vsinh_all = []
        vsinh_pairs = []
        vipham = state.get("vipham", [])  # lấy danh sách vi phạm hiện tại
        vsinh_queue, vipham_moi = tao_lich_vsinh(state, all_students, vipham)
# cập nhật lại vipham sau khi trừ buổi
        state["vipham"] = vipham_moi
        save_state(state)

        for day, picks in enumerate(vsinh_queue, start=1):
            vsinh_pairs.append(picks)
            vsinh_all.append(f"Ngày {day}: {', '.join(p for p in picks if p)}")

        messages.append("🧹 Trực Vệ Sinh:\n" + "\n".join(vsinh_all))

        # --- XGHE ---
        xghe_picks = random_two_xghe(state, data)
        messages.append(f"🪑 Xếp Ghế: {', '.join(p for p in xghe_picks if p)}")

        # --- Tổ trực: chuyển sang tổ tiếp theo ---
        if not state.get("to"):
            state["to"] = 1
        current_to = state["to"] + 1
        if current_to > 4:
            current_to = 1
        state["to"] = current_to
        save_state(state)
        messages.append(f"👥 Tổ trực: Tổ {current_to}")

        vsinh_str = "; ".join(" & ".join(pair) for pair in vsinh_pairs if pair)
        xghe_str = ", ".join(p for p in xghe_picks if p)

        detail = f"Danh sách trực nhật tuần này,{vsinh_str},{xghe_str},👥 Tổ trực: Tổ {current_to}"
        write_log_TN("Vsinh", str(interaction.user.id), detail)
        # --- Gửi kết quả ---
        await interaction.followup.send("\n".join(messages))

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi chạy lệnh /vsinh: {e}")

@bot.tree.command(name="addex", description="Thêm ngoại lệ và cập nhật VSINH", guild=GUILD)
@check_verified_TN()
async def changvsinh(interaction: discord.Interaction):
    try:
        # --- B1: hỏi tên ---
        await interaction.followup.send("✏️ Nhập **họ và tên** học sinh muốn bỏ khỏi trực nhật:")

        def check_name(msg: discord.Message):
            return msg.author.id == interaction.user.id and msg.channel.id == interaction.channel.id

        try:
            msg_name = await bot.wait_for("message", check=check_name, timeout=60.0)
            hoten_input = normalize_text(msg_name.content.strip())
        except asyncio.TimeoutError:
            await interaction.followup.send("⏰ Bạn đã hết thời gian nhập tên.", ephemeral=False)
            return

        # --- B2: kiểm tra trong cột B ---
        sheet = client.open_by_url(CHECK_SHEET_URL).sheet1
        col_b = sheet.col_values(2)
        matched_name = None
        for name in col_b:
            if normalize_text(name) == hoten_input:
                matched_name = name
                break

        if not matched_name:
            await interaction.followup.send("❌ Không tìm thấy tên này trong cột B.", ephemeral=False)
            return

        # --- B3: hỏi lý do ---
        await interaction.followup.send(f"✅ Đã tìm thấy **{matched_name}**.\nNhập lý do muốn bỏ khỏi trực nhật:")

        try:
            msg_reason = await bot.wait_for("message", check=check_name, timeout=60.0)
            reason_input = msg_reason.content.strip()
        except asyncio.TimeoutError:
            await interaction.followup.send("⏰ Bạn đã hết thời gian nhập lý do.", ephemeral=False)
            return

         # --- B4: Ghi ngoại lệ vào cột E và F (theo hàng trống đầu tiên) ---
        col_e = sheet.col_values(5)
        first_empty_row = len(col_e) + 1
        for i, v in enumerate(col_e, start=1):
            if not v.strip():
                first_empty_row = i
                break

        sheet.update_cell(first_empty_row, 5, matched_name)   # cột E
        sheet.update_cell(first_empty_row, 6, reason_input)   # cột F


        # --- B5: Cập nhật state (xóa tên khỏi not_recent_vsinh, not_recent_xghe) ---
        state = load_state()
        name_norm = normalize_text(matched_name)

        removed_from_vsinh = False
        removed_from_xghe = False

        if name_norm in [normalize_text(n) for n in state.get("not_recent_vsinh", [])]:
            state["not_recent_vsinh"] = [
                n for n in state["not_recent_vsinh"] if normalize_text(n) != name_norm
            ]
            removed_from_vsinh = True

        if name_norm in [normalize_text(n) for n in state.get("not_recent_xghe", [])]:
            state["not_recent_xghe"] = [
                n for n in state["not_recent_xghe"] if normalize_text(n) != name_norm
            ]
            removed_from_xghe = True

        save_state(state)

        # --- B6: Log chi tiết ---
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        detail = (
            f"[{timestamp}] ChangVSINH\n"
            f"- Ngoại lệ thêm: {matched_name}\n"
            f"- Lý do: {reason_input}\n"
            f"- Đã xóa khỏi VSINH: {'Có' if removed_from_vsinh else 'Không'}\n"
            f"- Đã xóa khỏi XGHE: {'Có' if removed_from_xghe else 'Không'}\n"
        )
        write_log_TN("ChangVsinh", str(interaction.user.id), detail)

        # --- B7: trả lời ---
        msg = f"✅ Đã thêm **{matched_name}** vào ngoại lệ.\n📝 Lý do: {reason_input}"
        if not removed_from_vsinh and not removed_from_xghe:
            msg += "\n⚠️ Tên này đã không còn trong VSINH/XGHE từ trước."
        await interaction.followup.send(msg, ephemeral=False)

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi chạy changvsinh: {e}", ephemeral=False)

@bot.tree.command(name="upvsinh", description="Update lại danh sách trực nhật sau khi bỏ ngoại lệ",guild=GUILD)
@check_verified_TN()
async def upvsinh(interaction: discord.Interaction):
    try:
        state = load_state()

        # --- Đọc danh sách gốc từ vsinh.txt ---
        with open("vsinh.txt", "r", encoding="utf-8") as f:
            all_students = [normalize_text(line.strip()) for line in f if line.strip()]

        # --- Lấy danh sách ngoại lệ từ CHECK_SHEET_URL (cột E) ---
        sheet = client.open_by_url(CHECK_SHEET_URL).sheet1
        exceptions = [normalize_text(name) for name in sheet.col_values(5) if name]  # cột E

        if not exceptions:
            await interaction.followup.send("⚠️ Không có ngoại lệ nào để cập nhật.", ephemeral=False)
            return

        # --- Loại bỏ ngoại lệ khỏi not_recent_vsinh ---
        original_vsinh = list(state.get("not_recent_vsinh", []))  # ép về list
        updated_vsinh = [name for name in original_vsinh if normalize_text(name) not in exceptions]

        # --- Loại bỏ ngoại lệ khỏi not_recent_xghe ---
        original_xghe = list(state.get("not_recent_xghe", []))  # ép về list
        updated_xghe = [name for name in original_xghe if normalize_text(name) not in exceptions]

        # --- Tìm xem có tên ngoại lệ nào không nằm trong cả 2 list (đã mất từ trước) ---
        lost_exceptions = [
            name for name in exceptions
            if name not in [normalize_text(n) for n in (original_vsinh + original_xghe)]
        ]

        # --- Cập nhật lại state ---
        state["not_recent_vsinh"] = updated_vsinh
        state["not_recent_xghe"] = updated_xghe
        save_state(state)

        # --- Tạo detail log có timestamp ---
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        detail = (
            f"[{timestamp}] Update VSINH\n"
            f"- Ngoại lệ loại bỏ: {', '.join(exceptions) if exceptions else 'Không có'}\n"
            f"- VSINH còn lại: {', '.join(updated_vsinh) if updated_vsinh else 'Trống'}\n"
            f"- XGHE còn lại: {', '.join(updated_xghe) if updated_xghe else 'Trống'}\n"
        )
        if lost_exceptions:
            detail += f"- Ngoại lệ đã mất từ trước: {', '.join(lost_exceptions)}"

        write_log_TN("UpVsinh", str(interaction.user.id), detail)

        # --- Gửi tin nhắn ---
        msg = "✅ Đã cập nhật lại danh sách trực nhật sau khi bỏ ngoại lệ."
        if lost_exceptions:
            msg += "\n⚠️ Những tên ngoại lệ này đã bị mất trước đó: " + ", ".join(lost_exceptions)

        await interaction.followup.send(msg, ephemeral=False)

    except Exception as e:
        # Dùng followup để tránh lỗi InteractionResponded
        await interaction.followup.send(f"❌ Lỗi khi update danh sách trực nhật: {e}", ephemeral=False)


@bot.tree.command(name="changevsinh", description="Thay đổi tên trong ngoại lệ", guild=GUILD)
@check_verified_TN()
async def changvsinh(interaction: discord.Interaction):
    try:
        await interaction.followup.send("Vui lòng nhập tên bạn muốn đưa vào ngoại lệ:")

        def check_name(msg):
            return msg.author.id == interaction.user.id and msg.channel.id == interaction.channel.id

        # --- Bước 1: nhập tên mới ---
        msg = await bot.wait_for("message", check=check_name, timeout=60)
        name_to_add = normalize_text(msg.content.strip())
        print(f"[DEBUG] name_to_add = {name_to_add}")

        # --- Kiểm tra có trong cột B không ---
        check_values = check_sheet.get_all_values()[1:]
        print(f"[DEBUG] check_values (len={len(check_values)})")
        name_found = any(len(row) >= 2 and normalize_text(row[1]) == name_to_add for row in check_values)

        if not name_found:
            await interaction.followup.send("❌ Không tìm thấy tên trong danh sách.")
            return

        # --- Bước 2: hỏi tên muốn thay thế ---
        await interaction.followup.send("Tên nào trong ngoại lệ muốn bị thay thế?")

        msg2 = await bot.wait_for("message", check=check_name, timeout=60)
        old_name = normalize_text(msg2.content.strip())

        # --- Kiểm tra old_name có trong cột E không ---
        sheet_values = check_sheet.get_all_values()[1:]

        old_row_idx = None
        gender = None
        for i, row in enumerate(sheet_values, start=2):
            if len(row) >= 5:
                if normalize_text(row[4]) == old_name:
                    old_row_idx = i
                    # lấy giới tính từ cột C (nếu có)
                    if len(row) >= 3:
                        gender = normalize_text(row[2])
                    break

        if old_row_idx is None:
            await interaction.followup.send("❌ Tên này không có trong ngoại lệ.")
            return

        # --- Hoán đổi tên ---
        check_sheet.update_cell(old_row_idx, 5, name_to_add.title())  # cột E
        check_sheet.update_cell(old_row_idx, 6, "")                   # cột F (xóa lý do nếu có)

        # --- Ghi lại old_name ---
        with open("vsinh.txt", "a", encoding="utf-8") as f:
            f.write(f"{old_name}\n")

        # --- Cập nhật state ---
        state = load_state()
        state["not_recent_vsinh"].add(old_name.title())
        if gender == "nam":
            state["not_recent_xghe"].add(old_name.title())
        save_state(state)

        await interaction.followup.send(
            f"✅ Hoán đổi thành công: {name_to_add.title()} vào ngoại lệ, {old_name.title()} trở lại danh sách trực nhật."
        )

    except asyncio.TimeoutError:
        await interaction.followup.send("⏰ Bạn đã hết thời gian nhập thông tin.", ephemeral=True)

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        print(f"[ERROR] {err_msg}")
        await interaction.followup.send(f"❌ Lỗi khi thay đổi: {e}", ephemeral=True)

@bot.tree.command(name="vipham", description="Thêm học sinh vi phạm phải trực nhật", guild=GUILD)
@check_verified_TN()
async def vipham(interaction: discord.Interaction, hoten: str, buoi: int):
    try:
        state = load_state()

        # Normalize + viết hoa
        hoten_norm = normalize_text(hoten)
        hoten_title = hoten.title()

        # --- Check trong cột B ---
        sheet = client.open_by_url(CHECK_SHEET_URL).sheet1
        all_students = [normalize_text(n) for n in sheet.col_values(2) if n]  # cột B
        if hoten_norm not in all_students:
            await interaction.followup.send(
                f"❌ Không tìm thấy **{hoten_title}** trong danh sách học sinh (cột B)."
            )
            return

        # --- Kiểm tra nếu học sinh đã có trong vipham thì cộng dồn ---
        found = False
        for record in state["vipham"]:
            if normalize_text(record[0]) == hoten_norm:
                record[1] += buoi
                found = True
                break

        if not found:
            state["vipham"].append([hoten_title, buoi])

        # --- Xóa tên khỏi not_recent_vsinh và not_recent_xghe ---
        state["not_recent_vsinh"] = [name for name in state.get("not_recent_vsinh", []) if normalize_text(name) != hoten_norm]
        state["not_recent_xghe"] = [name for name in state.get("not_recent_xghe", []) if normalize_text(name) != hoten_norm]

        save_state(state)

        await interaction.followup.send(
            f"✅ Đã ghi nhận **{hoten_title}** phải trực tổng cộng {next(r[1] for r in state['vipham'] if normalize_text(r[0]) == hoten_norm)} buổi."
        )

    except Exception as e:
        if interaction.response.is_done():
            await interaction.followup.send(f"❌ Lỗi khi thêm vi phạm: {e}")
        else:
            await interaction.followup.send(f"❌ Lỗi khi thêm vi phạm: {e}")

# --- Run Bot ---
bot.run(TOKEN)
