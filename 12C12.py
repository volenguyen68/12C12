import os
import discord
from discord.ext import commands, tasks
from discord import app_commands
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from functools import wraps
import unicodedata
from difflib import get_close_matches
import re
import asyncio
import random
from datetime import datetime, timezone, timedelta

# --- Load .env ---
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
SHEET_KEY_URL = os.getenv("SHEET_KEY_URL")
SHEET_KEY = os.getenv("sheet_key")  # URL trong .env
GUILD_ID = int(os.getenv("GUILD_ID"))
GUILD = discord.Object(id=GUILD_ID)
CHECK_SHEET_URL = os.getenv("CHECK_SHEET_URL")
SHEET_PHONGTRAO= os.getenv("SHEET_PHONGTRAO")
TENBANG = os.getenv("TENBANG")
TRUCNHAT= os.getenv("TRUCNHAT")
PHONGTRAO= os.getenv("PHONGTRAO")
BANG_PHONGTRAO=os.getenv("BANG_PHONGTRAO")
ENV_MODE = os.getenv("ENV_MODE", "dev")
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
sheetphongtrao=client.open_by_url(SHEET_PHONGTRAO).worksheet(BANG_PHONGTRAO)
spreadsheet = client.open_by_url(SHEET_KEY)
guild_obj = discord.Object(id=int(GUILD_ID)) if GUILD_ID else None
# Ví dụ tạo một sheet riêng để lưu user đã xác minh
verify_sheet = spreadsheet.worksheet("Sheet1")


def slash_command(**kwargs):
    """
    Dùng thay cho @bot.tree.command
    - Nếu ENV_MODE=dev => đăng ký slash command trong guild test (có hiệu lực ngay)
    - Nếu ENV_MODE=production => đăng ký global command (OriHost)
    """
    if ENV_MODE == "dev" and guild_obj:
        return bot.tree.command(guild=guild_obj, **kwargs)
    else:
        return bot.tree.command(**kwargs)

def get_name_by_discord_id(user_id: str) -> str:
    try:
        key_values = safe_get_all_values(verify_sheet)[1:]  # bỏ header
        for row in key_values:
            # Lấy ID từ cột R (18) nếu có, nếu không thì lấy cột B (2)
            discord_id = ""
            if len(row) >= 18 and row[17].strip():
                discord_id = row[17].strip()
            elif len(row) >= 2 and row[1].strip():
                discord_id = row[1].strip()

            # So khớp ID với user_id
            if discord_id == str(user_id):
                # Ưu tiên lấy tên ở cột S (19), nếu trống thì lấy cột C (3)
                if len(row) >= 19 and row[18].strip():
                    return row[18].strip()
                elif len(row) >= 3 and row[2].strip():
                    return row[2].strip()
                else:
                    return "Không rõ tên"
    except Exception as e:
        print(f"get_name_by_discord_id error: {e}")
    return "Không rõ tên"


def get_name_by_discord_id_TN(user_id: str) -> str:
    try:
        key_values = safe_get_all_values(verify_sheet)[1:]  # bỏ header
        for row in key_values:
            # cần ít nhất 7 cột (tới G)
            if len(row) >= 7:
                discord_id = str(row[5]).strip()   # cột F
                hoten = str(row[6]).strip()       # cột G
                if discord_id == str(user_id):
                    return hoten
    except Exception as e:
        print(f"get_name_by_discord_id_TN error: {e}")
    return "<Không rõ tên>"

def get_name_by_discord_id_PT(user_id: str) -> str:
    try:
        key_values = safe_get_all_values(verify_sheet)[1:]  # bỏ header
        for row in key_values:
            # cần ít nhất 7 cột (tới G)
            if len(row) >= 15:
                discord_id = str(row[13]).strip()   # cột F
                hoten = str(row[14]).strip()       # cột G
                if discord_id == str(user_id):
                    return hoten
    except Exception as e:
        print(f"get_name_by_discord_id_TN error: {e}")
    return "<Không rõ tên>"

def write_log(action: str, executor_id: str, detail: str):
    try:
        log_sheet = spreadsheet.worksheet(TENBANG)  # sheet "Log" phải tồn tại
        executor_name = get_name_by_discord_id(executor_id)

        VN_TZ = timezone(timedelta(hours=7))
        timestamp = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")

        new_row = [timestamp, executor_name, str(executor_id), action, detail]
        log_sheet.append_row(new_row, value_input_option="RAW")
    except Exception as e:
        print(f"Lỗi ghi log: {e}")

def write_log_TN(action: str, executor_id: str, detail: str):
    try:
        log_sheet = spreadsheet.worksheet(TRUCNHAT)  # sheet "Log" phải tồn tại
        executor_name = get_name_by_discord_id_TN(executor_id)
        
        VN_TZ = timezone(timedelta(hours=7))
        timestamp = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")

        new_row = [timestamp, executor_name, str(executor_id), action, detail]
        log_sheet.append_row(new_row, value_input_option="RAW")
    except Exception as e:
        print(f"Lỗi ghi log: {e}")

def write_log_PT(action: str, executor_id: str, detail: str):
    try:
        log_sheet = spreadsheet.worksheet(PHONGTRAO)  # sheet "Log" phải tồn tại
        executor_name = get_name_by_discord_id_PT(executor_id)
        
        VN_TZ = timezone(timedelta(hours=7))
        timestamp = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")

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
def check_verified_ADMIN():
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

from functools import wraps
from datetime import datetime, timezone, timedelta
import time

VN_TZ = timezone(timedelta(hours=7))

from functools import wraps
from datetime import datetime, timezone, timedelta
import time

VN_TZ = timezone(timedelta(hours=7))

def check_verified():
    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            await interaction.response.defer(ephemeral=False)
            user_id = str(interaction.user.id)

            verified = False
            key_valid = False

            # --- 1️⃣ Kiểm tra verify_sheet ---
            try:
                key_values = safe_get_all_values(verify_sheet)[1:]  # bỏ header
                verified = any(
                    len(row) >= 2 and str(user_id) == str(row[1]).strip()
                    for row in key_values
                )
            except Exception as e:
                print(f"Lỗi: {e}")

            # --- 2️⃣ Kiểm tra key trong sheetkey ---
            try:
                key_ws = verify_sheet
                rows = key_ws.get_all_values()
                now_ts = int(time.time())

                for i, row in enumerate(rows[1:], start=2):  # dòng bắt đầu từ 2
                    if len(row) < 22:
                        continue

                    id_in_sheet = str(row[17]).strip()  # cột R
                    time_cell = str(row[20]).strip()    # cột U
                    key_name = str(row[16]).strip()     # cột Q

                    if id_in_sheet != user_id:
                        continue

                    expire_ts = None
                    if time_cell.isdigit():
                        expire_ts = int(time_cell)
                    else:
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
                            try:
                                dt = datetime.strptime(time_cell, fmt).replace(tzinfo=VN_TZ)
                                expire_ts = int(dt.timestamp())
                                break
                            except:
                                continue

                    # ✅ Còn hạn
                    if expire_ts and now_ts < expire_ts:
                        key_valid = True
                        break

                    # ❌ Hết hạn → tự động thu hồi quyền
                    elif expire_ts and now_ts >= expire_ts:
                        try:
                            key_ws.update_cell(i, 21, "")  # Xóa cột U (thời gian)
                            key_ws.update_cell(i, 22, "🔒 Đã bị thu hồi quyền hạn")  # Ghi chú

                            # Ghi log (nếu bạn có hàm write_log)
                            try:
                                write_log(
                                    "Tự động thu hồi key",
                                    user_id,
                                    f"Key [{key_name}] hết hạn, đã thu hồi quyền."
                                )
                            except:
                                pass

                            # Gửi DM cho người dùng (tùy chọn)
                            try:
                                user = await interaction.client.fetch_user(int(user_id))
                                await user.send(
                                    f" Bạn đã hết hạn được cấp quyền chỉnh sửa.**"
                                )
                            except:
                                pass

                            print(f"⏳ Key {key_name} (user {user_id}) đã hết hạn — thu hồi tự động.")
                        except Exception as e:
                            print(f"Lỗi khi thu hồi key hết hạn: {e}")

            except Exception as e:
                print(f"Lỗi check_verified (sheetkey): {e}")

            # --- 3️⃣ Kiểm tra quyền tổng hợp ---
            if not (verified or key_valid):
                await interaction.followup.send(
                    "❌ Bạn không có quyền sử dụng lệnh này.",
                    ephemeral=True
                )
                return

            # --- 4️⃣ Nếu hợp lệ ---
            return await func(interaction, *args, **kwargs)

        return wrapper
    return decorator




def check_verified_NHOM():
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
                    if len(row) > 9:  # phải có ít nhất 5 cột
                        cell = str(row[9]).strip()  # Cột E = index 4
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

def check_verified_PT():
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
                    if len(row) > 13:  # phải có ít nhất 5 cột
                        cell = str(row[13]).strip()  # Cột E = index 4
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

async def update_mark(interaction, sheet_name: str, stt_list: str, cot: int, diem_list: str):
    try:
        sheet = sheetmon.worksheet(sheet_name)
    except Exception as e:
        await interaction.followup.send(f"❌ Không thể mở sheet '{sheet_name}': {e}")
        return

    try:
        # --- Chuẩn hóa danh sách nhập ---
        stt_values = [s.strip() for s in str(stt_list).replace(",", " ").split() if s.strip()]
        diem_values = [d.strip() for d in str(diem_list).replace(",", " ").split() if d.strip()]

        # Kiểm tra độ dài khớp nhau
        if len(stt_values) != len(diem_values):
            await interaction.followup.send(
                f"❌ Số lượng STT ({len(stt_values)}) và điểm ({len(diem_values)}) không khớp!\n"
                f"➡️ Ví dụ đúng: `/mark mon:su stt:9,30,26 cot:2 value:8,9,10`",
                ephemeral=True
            )
            return

        data = sheet.col_values(1)
        messages = []

        # Xác định cột thực tế trên Google Sheet
        col_index = 3 + int(cot)

        # Mapping tên đợt
        if cot in (1, 2, 3, 4):
            dot = f"15 phút, đợt {cot}"
        elif cot == 6:
            dot = "giữa kì"
        elif cot == 8:
            dot = "cuối kì"
        else:
            dot = f"đợt {cot}"

        # --- Lặp qua từng STT ---
        for stt, val in zip(stt_values, diem_values):
            row_index = None
            for i, cell_val in enumerate(data, start=1):
                if str(cell_val).strip() == str(stt):
                    row_index = i
                    break

            if row_index is None:
                messages.append(f"❌ Không tìm thấy STT {stt}")
                continue

            student_name = sheet.cell(row_index, 2).value or "<không có tên>"

            try:
                numeric_value = float(val)
            except ValueError:
                messages.append(f"⚠️ STT {stt}: '{val}' không hợp lệ (không phải số)")
                continue

            # Kiểm tra phạm vi điểm
            if not (7.5 <= numeric_value <= 10):
                messages.append(f"⚠️ STT {stt}: {numeric_value} không hợp lệ (phải từ 7.5 → 10)")
                continue

            # --- Ghi điểm ---
            sheet.update_cell(row_index, col_index, str(round(numeric_value, 2)))
            messages.append(f"✅ STT {stt} ({student_name}) - {dot}: {numeric_value}")

            # --- Ghi log ---
            write_log(
                "Ghi điểm",
                str(interaction.user.id),
                f"[{sheet_name}] STT {stt} ({student_name}) ghi {numeric_value} điểm - {dot}"
            )

        # --- Tổng hợp kết quả ---
        result = "\n".join(messages)
        await interaction.followup.send(f"**Cập nhật điểm cho môn [{sheet_name}]**\n{result}")

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi cập nhật sheet: {e}")


@slash_command(name="mark", description="Nhập điểm cho nhiều học sinh cùng 1 đợt")
@app_commands.describe(
    mon="Chọn môn học",
    stt="Danh sách STT, cách nhau bằng dấu phẩy (VD: 9,30,26)",
    cot="Đợt 1,2,3,4=15p | 6=GK | 8=CK",
    value="Danh sách điểm tương ứng (VD: 8,9,10)"
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
    app_commands.Choice(name="su", value="Môn Sử"),
    app_commands.Choice(name="hdtn", value="Môn HĐTN")
])
@check_verified()
async def mark(interaction: discord.Interaction, mon: app_commands.Choice[str], stt: str, cot: int, value: str):
    await update_mark(interaction, mon.value, stt, cot, value)



# --- Redeem command (fixed, debug, full scan) ---

def normalize_text(text: str) -> str:
    """Chuẩn hóa chuỗi để so sánh"""
    if text is None:
        return ""
    return unicodedata.normalize("NFC", str(text)).strip().lower()

@slash_command(name="redeem", description="Nhập key để kích hoạt")
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
            colI = row[8].strip() if row_len > 8 else ""
            colM = row[12].strip() if row_len > 12 else ""
            colG = row[16].strip() if row_len > 16 else ""
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
                
            elif colI == key_code.strip():
                found = True
                existing_id = row[9].strip() if row_len > 9 else ""
                if existing_id:
                    await interaction.followup.send("❌ Key đã được sử dụng.")
                    return
                target_cols = (10, 11, 12)  # J, K, L

            elif colM == key_code.strip():
                found = True
                existing_id = row[13].strip() if row_len > 13 else ""
                if existing_id:
                    await interaction.followup.send("❌ Key đã được sử dụng.")
                    return
                target_cols = (14, 15, 16)  # N, O, P
            
            elif colG == key_code.strip():
                found = True
                existing_id = row[17].strip() if row_len > 17 else ""
                if existing_id:
                    await interaction.followup.send("❌ Key đã được sử dụng.")
                    return
                target_cols = (18, 19, 20)  # R, S, T

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

                for row_check in check_values:
                    if len(row_check) >= 6:  # cần đến cột F
                        stt_val = row_check[0].strip()
                        name_in_sheet = normalize_text(row_check[1])  # cột B = Họ tên
                        mien_truc_name = normalize_text(row_check[4])  # cột E
                        chuc_vu = row_check[5].strip() if row_check[5].strip() else "Thành Viên 12C12"  # cột F

                        if name_in_sheet == hoten_input:
                            stt_found = stt_val
                            matched_name = row_check[1].strip()
                            role_value = "Thành Viên 12C12"
                            break
            # nếu tên học sinh trùng với cột E thì lấy chức vụ, ngược lại giữ "Thành Viên 12C12"
                        
                        if mien_truc_name == hoten_input:
                            for r2 in check_values:
                                if len(r2) >= 2 and normalize_text(r2[1]) == mien_truc_name:
                                    stt_found = r2[0].strip()
                                    break
                            matched_name = row_check[4].strip()
                            role_value = chuc_vu
                            break

                if not stt_found:
                    await interaction.followup.send(
                        "❌ Không tìm thấy họ và tên trong danh sách học sinh 12C12",
                        ephemeral=True
                    )
                    return

                # --- Ghi dữ liệu vào sheet key ---
                key_sheet.update_cell(i, target_cols[0], str(interaction.user.id))
                key_sheet.update_cell(i, target_cols[1], matched_name)
                key_sheet.update_cell(i, target_cols[2], stt_found)

                await interaction.followup.send(
                    f"✅ Key kích hoạt thành công!\n👤 Xin Chào {role_value}: {matched_name}\n🔢 STT: {stt_found}\n"
                    f"Giờ đây bạn đã được quyền sử dụng bot",
                    ephemeral=False
                )
                break

        if not found:
            await interaction.followup.send("❌ Key không hợp lệ", ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi kiểm tra key: {e}", ephemeral=True)

@slash_command(name="cong", description="Cộng/trừ điểm linh hoạt cho nhiều học sinh")
@app_commands.describe(
    mon="Chọn môn học",
    stt="Danh sách STT hoặc 'Tổ n' (1–6)",
    diem="Danh sách điểm, cách nhau bằng dấu phẩy (có thể âm hoặc thêm 'đ')"
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
    app_commands.Choice(name="su", value="Môn Sử"),
    app_commands.Choice(name="hdtn", value="Môn HĐTN")
])
@check_verified()
async def cong(interaction: discord.Interaction, mon: app_commands.Choice[str], stt: str, diem: str):
    try:
        to_ten = None  # để biết người dùng có nhập "Tổ n" không
        to_mode = False

        # --- Xử lý trường hợp "Tổ n" ---
        to_match = re.match(r"t[oô]\s*(\d+)", stt.strip().lower())
        if to_match:
            to_so = int(to_match.group(1))
            if 1 <= to_so <= 6:
                to_ten = f"Tổ {to_so}"
                to_mode = True
                try:
                    to_sheet = sheetmon.worksheet(to_ten)
                    stt_list = [s for s in to_sheet.col_values(1) if s.strip().isdigit()]
                    if not stt_list:
                        await interaction.followup.send(f"❌ Không có STT nào trong sheet {to_ten}.")
                        return
                    await interaction.followup.send(f"📘 Đã nhận diện {to_ten}, gồm {len(stt_list)} học sinh.")
                except Exception as e:
                    await interaction.followup.send(f"❌ Không tìm thấy sheet {to_ten}: {e}")
                    return
            else:
                await interaction.followup.send("❌ Số tổ phải nằm trong khoảng 1–6.")
                return
        else:
            # --- STT thông thường ---
            stt_list = [s.strip() for s in stt.split(",") if s.strip().isdigit()]

        diem_list = [d.strip() for d in diem.split(",") if d.strip()]

        if not stt_list or not diem_list:
            await interaction.followup.send("❌ STT hoặc điểm không hợp lệ.")
            return

        # --- Trường hợp chỉ có 1 điểm nhưng nhiều STT ---
        if len(diem_list) == 1 and len(stt_list) > 1:
            diem_list = diem_list * len(stt_list)

        # --- Trường hợp nhiều điểm nhiều STT ---
        if len(diem_list) != len(stt_list):
            await interaction.followup.send("❌ Số lượng điểm và STT không khớp.")
            return

        # --- Xử lý giá trị điểm ---
        diem_values = []
        for d in diem_list:
            d_str = d.lower().replace(",", ".").strip()
            is_quydoi = "đ" in d_str
            d_str = d_str.replace("đ", "").strip()
            raw = float(d_str)
            if is_quydoi:
                def quy_doi(raw_val: float) -> float:
                    if raw_val == 10: return 2.0
                    elif raw_val == 9: return 1.5
                    elif raw_val == 8: return 1.0
                    else: return 0.0
                diem_values.append(-quy_doi(abs(raw)) if raw < 0 else quy_doi(raw))
            else:
                diem_values.append(raw)

        # --- Xác nhận với người dùng ---
        stt_display = ", ".join(stt_list)
        diem_display = ", ".join(map(str, diem_values))
        await interaction.followup.send(
            f"⚠️ Bạn sắp thao tác cộng/trừ điểm cho các STT: {stt_display} với điểm: {diem_display}.\n"
            f"Nhập 'yes' để xác nhận, hoặc 'no' để hủy."
        )

        def check_confirm(msg: discord.Message):
            return msg.author.id == interaction.user.id and msg.channel.id == interaction.channel.id and msg.content.lower() in ["yes", "no"]

        try:
            msg = await bot.wait_for("message", check=check_confirm, timeout=60.0)
            if msg.content.lower() != "yes":
                await interaction.followup.send("❌ Thao tác bị hủy theo yêu cầu của bạn.")
                return
        except asyncio.TimeoutError:
            await interaction.followup.send("⏰ Hết thời gian xác nhận. Thao tác bị hủy.")
            return

        # --- Mở sheet môn học ---
        sheet = sheetmon.worksheet(mon.value)
        data = sheet.col_values(1)
        col_diem = 13
        results = []
        tong_cong = 0
        tong_tru = 0

        for stt_item, diem_val in zip(stt_list, diem_values):
            # Tìm dòng STT
            row_index = None
            for i, val in enumerate(data, start=1):
                if str(val).strip() == stt_item:
                    row_index = i
                    break
            if row_index is None:
                results.append(f"❌ Không tìm thấy STT {stt_item}")
                continue

            current_value = sheet.cell(row_index, col_diem).value
            try:
                current_value = float(str(current_value).replace(",", ".").replace("+", "").strip())
            except:
                current_value = 0.0

            new_value = current_value + diem_val
            student_name = sheet.cell(row_index, 2).value or "<không có tên>"

            def fmt(val: float) -> str:
                if val > 0: s = f"+{val:.2f}".rstrip("0").rstrip(".")
                elif val < 0: s = f"{val:.2f}".rstrip("0").rstrip(".")
                else: s = "0"
                return s.replace(".", ",")

            display_value = fmt(new_value)

            # Nếu bị trừ điểm → yêu cầu nhập lý do
            if new_value < current_value:
                await interaction.followup.send(
                    f"⚠️ STT {stt_item}, {student_name} bị trừ điểm từ {fmt(current_value)} → {display_value}. "
                    f"Nhập lý do trong 60 giây."
                )
                def check(msg: discord.Message):
                    return msg.author.id == interaction.user.id and msg.channel.id == interaction.channel.id
                try:
                    msg = await bot.wait_for("message", check=check, timeout=60.0)
                    ly_do = msg.content.strip()
                    sheet.update_cell(row_index, col_diem, display_value)
                    results.append(f"✅ TRỪ {abs(diem_val)} điểm STT {stt_item}, {student_name}: {display_value}")
                    write_log("Trừ", str(interaction.user.id),
                              f"TRỪ {abs(diem_val)} điểm STT {stt_item}, {student_name}, từ {fmt(current_value)} → {display_value}, Lý do: {ly_do}, [{mon.value}]")
                    tong_tru += abs(diem_val)
                    continue
                except asyncio.TimeoutError:
                    results.append(f"⏰ Hết thời gian nhập lý do cho STT {stt_item}. Bỏ qua.")
                    continue

            # Cộng điểm
            sheet.update_cell(row_index, col_diem, display_value)
            results.append(f"✅ CỘNG {diem_val} điểm STT {stt_item}, {student_name}: {display_value}")
            write_log("Cộng", str(interaction.user.id),
                      f"CỘNG {diem_val} điểm STT {stt_item}, {student_name}, [{mon.value}], tổng điểm: {display_value}")
            tong_cong += diem_val

        # --- Log tổng nếu là "Tổ n" ---
        if to_mode and to_ten:
            tong_hs = len(stt_list)
            tong_diem = round(tong_cong - tong_tru, 2)
            write_log(
                "Cộng Tổ",
                str(interaction.user.id),
                f"Thực hiện cộng/trừ điểm cho toàn bộ {to_ten}, gồm {tong_hs} học sinh, tổng cộng {tong_diem:+.2f} điểm, [{mon.value}]"
            )

        await interaction.followup.send("\n".join(results))

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi cộng điểm: {e}")

@slash_command(name="xoa", description="Xóa đánh dấu X khỏi bảng điểm")#)
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
    app_commands.Choice(name="su", value="Môn Sử"),
    app_commands.Choice(name="hdtn", value="Môn HĐTN")
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
import json

def init_state():
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write("vsinh=\n")
            f.write("xghe=\n")
            f.write("to=\n")
            f.write("not_recent_vsinh=\n")
            f.write("not_recent_xghe=\n")
            f.write("vipham={}\n")


def format_name(name: str) -> str:
    """Chuẩn hóa tên: bỏ khoảng trắng thừa + viết hoa chữ cái đầu."""
    return str(name).strip().title()


def sanitize_state(state):
    """Đảm bảo state hợp lệ, reset giá trị sai về mặc định."""
    # vsinh & xghe: loại bỏ chuỗi rỗng, chuẩn hóa tên
    state["vsinh"] = [format_name(x) for x in state.get("vsinh", []) if str(x).strip()]
    state["xghe"] = [format_name(x) for x in state.get("xghe", []) if str(x).strip()]

    # to: chỉ cho phép 1–4, nếu sai thì reset None
    try:
        to_val = int(state.get("to")) if state.get("to") else None
        if to_val and 1 <= to_val <= 4:
            state["to"] = to_val
        else:
            state["to"] = None
    except Exception:
        state["to"] = None

    # not_recent_vsinh & not_recent_xghe: loại rỗng, chuẩn hóa tên, loại trùng
    state["not_recent_vsinh"] = list({
        format_name(x) for x in state.get("not_recent_vsinh", []) if str(x).strip()
    })
    state["not_recent_xghe"] = list({
        format_name(x) for x in state.get("not_recent_xghe", []) if str(x).strip()
    })

    # vipham: đảm bảo là dict {tên: số buổi}, số buổi >= 0
    vipham_clean = {}
    for k, v in (state.get("vipham") or {}).items():
        try:
            ngay = int(v)
            if ngay > 0:
                vipham_clean[format_name(k)] = ngay
        except Exception:
            continue
    state["vipham"] = vipham_clean

    return state


def load_state():
    init_state()
    state = {
        "vsinh": [],
        "xghe": [],
        "to": None,
        "all_vsinh": [],
        "not_recent_vsinh": [],
        "not_recent_xghe": [],
        "vipham": {}
    }

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue  

            key, val = line.split("=", 1)
            val = val.strip().strip('"')

            if key == "vsinh" and val:
                state["vsinh"] = [format_name(x) for x in val.split(",") if x.strip()]
            elif key == "xghe" and val:
                state["xghe"] = [format_name(x) for x in val.split(",") if x.strip()]
            elif key == "to":
                state["to"] = val  # để sanitize xử lý sau
            elif key == "not_recent_vsinh" and val:
                state["not_recent_vsinh"] = [format_name(x) for x in val.split(",") if x.strip()]
            elif key == "not_recent_xghe" and val:
                state["not_recent_xghe"] = [format_name(x) for x in val.split(",") if x.strip()]
            elif key == "vipham":
                try:
                    parsed = json.loads(val)
                    if isinstance(parsed, dict):
                        state["vipham"] = parsed
                    else:
                        state["vipham"] = {}
                except Exception:
                    state["vipham"] = {}
    return sanitize_state(state)


def save_state(state):
    state = sanitize_state(state)  # dọn trước khi lưu
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write("vsinh=" + ",".join(state.get("vsinh", [])) + "\n")
        f.write("xghe=" + ",".join(state.get("xghe", [])) + "\n")
        f.write("to=" + (str(state["to"]) if state.get("to") else "") + "\n")
        f.write("not_recent_vsinh=" + ",".join(state.get("not_recent_vsinh", [])) + "\n")
        f.write("not_recent_xghe=" + ",".join(state.get("not_recent_xghe", [])) + "\n")
        f.write("vipham=" + json.dumps(state.get("vipham", {}), ensure_ascii=False) + "\n")

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

# ================== RANDOM VSINH/XGHE ==================



from typing import List, Tuple, Dict, Set, Any
import asyncio

def normalize_name(name: str) -> str:
    """Chuẩn hóa tên để so sánh công bằng (strip + lowercase)."""
    return str(name).strip().casefold()

import random
import asyncio
from typing import Dict, Any, List, Tuple

async def random_vsinh_complete(
    bot,
    interaction,
    state: Dict[str, Any],
    all_students: List[str],
    exclusions: List[str] = None,
    vipham: Dict[str, int] = None,
    check_sheet: List[List[str]] = None
) -> Tuple[List[List[str]], Dict[str, int]]:
    """
    Random trực vệ sinh 5 ngày với VIPHAM ưu tiên.
    Người VIPHAM chỉ được trực 1 mình trong ngày đó.
    Bot sẽ hỏi user qua Discord (không qua terminal).
    """

    exclusions = exclusions or []
    all_students_clean = [s.strip() for s in all_students if s.strip()]
    if len(all_students_clean) < 2:
        raise ValueError("Danh sách học sinh không đủ để random.")

    # Chuẩn bị dữ liệu vipham
    vipham_working = [[k, int(v)] for k, v in (vipham or {}).items()]
    vipham_names = {v[0] for v in vipham_working}

    # Lấy lịch sử đã trực
    history_vsinh = set(state.get("vsinh", []))
    prev_not_recent = state.get("not_recent_vsinh", [])
    not_recent = set(all_students_clean) - set(exclusions) - vipham_names - history_vsinh

    vsinh_queue = [None] * 5
    used_names = set()
    slots_free = list(range(5))

    # 🟩 Hỏi người dùng có muốn chỉ định VIPHAM
    if vipham_working:
        await interaction.followup.send("Có muốn xếp **VIPHAM** nào trực ngày nào không? (y/n)")
        try:
            reply = await bot.wait_for(
                "message",
                timeout=60.0,
                check=lambda m: m.author == interaction.user and m.channel == interaction.channel
            )
            choice = reply.content.strip().lower()
        except asyncio.TimeoutError:
            choice = "n"

        if choice == "y":
            while True:
                vipham_list = "\n".join(
                    [f"{i}. {name} ({count} buổi còn lại)" for i, (name, count) in enumerate(vipham_working, start=1)]
                )
                await interaction.followup.send(f"**Danh sách VIPHAM hiện tại:**\n{vipham_list}")

                await interaction.followup.send("Nhập **tên hoặc STT** vipham muốn xếp (hoặc nhấn Enter để bỏ qua):")
                try:
                    selected_msg = await bot.wait_for(
                        "message",
                        timeout=60.0,
                        check=lambda m: m.author == interaction.user and m.channel == interaction.channel
                    )
                    selected = selected_msg.content.strip()
                except asyncio.TimeoutError:
                    break
                if not selected:
                    break

                # Nếu user nhập số → tra check_sheet để lấy tên
                if selected.isdigit() and check_sheet:
                    stt = int(selected)
                    found = None
                    for row in check_sheet:
                        if str(row[0]).strip() == str(stt):
                            found = row[1].strip()
                            break
                    if not found:
                        await interaction.followup.send("❌ Không tìm thấy STT đó trong check_sheet.")
                        continue
                    selected_name = found
                else:
                    selected_name = selected

                # Kiểm tra có trong vipham không
                if selected_name not in vipham_names:
                    await interaction.followup.send("❌ Tên không nằm trong danh sách VIPHAM.")
                    continue

                await interaction.followup.send("Nhập **thứ muốn xếp (2–6):**")
                try:
                    thu_msg = await bot.wait_for(
                        "message",
                        timeout=60.0,
                        check=lambda m: m.author == interaction.user and m.channel == interaction.channel
                    )
                    thu = thu_msg.content.strip()
                except asyncio.TimeoutError:
                    await interaction.followup.send("⏰ Hết thời gian, bỏ qua.")
                    continue

                if thu not in ["2", "3", "4", "5", "6"]:
                    await interaction.followup.send("❌ Giá trị không hợp lệ, chỉ nhập 2–6.")
                    continue

                idx = int(thu) - 2
                if vsinh_queue[idx] is not None:
                    await interaction.followup.send(f"⚠️ Thứ {thu} đã có người trực, bỏ qua.")
                    continue

                # Gán vipham đó trực
                vsinh_queue[idx] = [selected_name]
                used_names.add(selected_name)
                for v in vipham_working:
                    if v[0] == selected_name:
                        v[1] -= 1
                        break

                await interaction.followup.send(f"✅ Đã xếp **{selected_name}** trực **thứ {thu}**.")

                await interaction.followup.send("Tiếp tục chọn VIPHAM khác? (y/n)")
                try:
                    more_msg = await bot.wait_for(
                        "message",
                        timeout=60.0,
                        check=lambda m: m.author == interaction.user and m.channel == interaction.channel
                    )
                    more = more_msg.content.strip().lower()
                except asyncio.TimeoutError:
                    break
                if more != "y":
                    break

    # 🟧 1️⃣ Xếp các VIPHAM còn lại (tự động)
    for v in vipham_working:
        name, count = v
        while count > 0 and slots_free:
            idx = slots_free.pop(0)
            if vsinh_queue[idx] is None:
                vsinh_queue[idx] = [name]
                used_names.add(name)
                count -= 1
        v[1] = count

    vipham_after = {v[0]: v[1] for v in vipham_working if v[1] > 0}

    # --- 2️⃣ Slot còn trống (TH1/TH2/TH3) ---
    slots_free = [i for i, x in enumerate(vsinh_queue) if x is None]
    for idx in slots_free:
        if len(not_recent) >= 2:
            pair = random.sample(list(not_recent), 2)
            vsinh_queue[idx] = pair
            used_names.update(pair)
            not_recent -= set(pair)
        elif len(not_recent) == 1:
            last_student = not_recent.pop()
            state["vsinh"] = []
            reset_pool = set(all_students_clean) - set(exclusions) - used_names - vipham_names - {last_student}
            if not reset_pool:
                reset_pool = set(all_students_clean) - used_names - vipham_names
            partner = random.choice(list(reset_pool))
            vsinh_queue[idx] = [last_student, partner]
            used_names.update([last_student, partner])
        else:
            reset_pool = set(all_students_clean) - set(exclusions) - used_names - vipham_names
            state["vsinh"] = []
            if len(reset_pool) < 2:
                reset_pool = set(all_students_clean) - used_names - vipham_names
            if len(reset_pool) >= 2:
                pair = random.sample(list(reset_pool), 2)
            else:
                pair = random.sample(all_students_clean, 2)
            vsinh_queue[idx] = pair
            used_names.update(pair)

    # 🟨 3️⃣ Cập nhật state
    flattened = [n for pair in vsinh_queue for n in pair if n and n not in vipham_after.keys()]
    state["vsinh"] = state.get("vsinh", []) + flattened

    state["not_recent_vsinh"] = [
        s for s in all_students_clean if s not in used_names and s not in vipham_after.keys()
    ]
        # 🟪 4️⃣ Tạo bản sao lưu (backup)
    import os
    import glob
    from datetime import datetime

    backup_dir = "backups"
    os.makedirs(backup_dir, exist_ok=True)

    # Tạo tên file backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"vsinh_backup_{timestamp}.txt")

    # Ghi nội dung backup
    with open(backup_path, "w", encoding="utf-8") as f:
        f.write("🧹 Danh sách trực vệ sinh:\n")
        for day, pair in enumerate(vsinh_queue, start=2):
            if pair:
                f.write(f"Thứ {day}: {', '.join(pair)}\n")
        f.write("\nCòn lại VIPHAM:\n")
        for name, count in vipham_after.items():
            f.write(f"- {name}: {count} buổi\n")

    # Giữ lại tối đa 4 bản backup gần nhất
    backups = sorted(glob.glob(os.path.join(backup_dir, "vsinh_backup_*.txt")), reverse=True)
    if len(backups) > 4:
        for old_file in backups[4:]:
            try:
                os.remove(old_file)
            except Exception:
                pass

    return vsinh_queue, vipham_after


def normalize_name(name: str) -> str:
    """Chuẩn hóa tên để so sánh công bằng (strip + lowercase)."""
    return str(name).strip().casefold()

def random_two_xghe(state, data):
    """
    Random 6 học sinh nam chia thành 3 cặp XGHE (mỗi cặp 2 người).
    Có 3 trường hợp (TH1/TH2/TH3) tương tự random_vsinh_complete.
    Sau khi chọn xong, loại 6 người này khỏi cả not_recent_xghe và not_recent_vsinh.
    """
    exceptions = get_exception_set(data)
    all_male_students = candidates_from_data(data, only_male=True, exception_set=exceptions)

    if len(all_male_students) < 6:
        raise ValueError("Không đủ học sinh nam để chọn 6 người.")

    # --- Khởi tạo / đảm bảo not_recent_xghe là set ---
    if "not_recent_xghe" not in state or not state["not_recent_xghe"]:
        state["not_recent_xghe"] = set(all_male_students)
    elif isinstance(state["not_recent_xghe"], list):
        state["not_recent_xghe"] = set(state["not_recent_xghe"])
    
    # Lấy lịch sử đã trực
    history_vsinh = set(state.get("xghe", []))
    # not_recent ban đầu
    prev_not_recent = state.get("not_recent_xghe", [])
    not_recent = set(all_male_students) - history_vsinh
    used_names = set()

    xghe_pairs = [None] * 3  # có 3 slot = 3 cặp = 6 người

    # --- Random 3 cặp (TH1 / TH2 / TH3) ---
    slots_free = [i for i, x in enumerate(xghe_pairs) if x is None]
    for idx in slots_free:
        if len(not_recent) >= 2:
            # TH1 - đủ người trong not_recent
            pair = random.sample(list(not_recent), 2)
            xghe_pairs[idx] = pair
            used_names.update(pair)
            not_recent -= set(pair)

        elif len(not_recent) == 1:
            # TH2 - chỉ còn 1 người trong not_recent
            last_student = not_recent.pop()
            state["xghe"] = []  # reset tạm
            reset_pool = set(all_male_students) - set(exceptions) - used_names - {last_student}
            if not reset_pool:
                reset_pool = set(all_male_students) - used_names
            partner = random.choice(list(reset_pool))
            used_names.update([last_student, partner])
            xghe_pairs[idx] = [last_student, partner]

        else:
            # TH3 - hết người trong not_recent
            state["xghe"] = []  # reset tạm
            reset_pool = set(all_male_students) - set(exceptions) - used_names
            if len(reset_pool) < 2:
                reset_pool = set(all_male_students) - used_names
            if len(reset_pool) >= 2:
                pair = random.sample(list(reset_pool), 2)
            else:
                pair = random.sample(all_male_students, 2)
            xghe_pairs[idx] = pair
            used_names.update(pair)

        # Nếu đã đủ 6 người rồi thì dừng sớm
        flattened = [n for pair in xghe_pairs if pair for n in pair]
        if len(flattened) >= 6:
            break

    # --- Cập nhật lịch sử ---
    flattened = [n for pair in xghe_pairs if pair for n in pair]
    state["xghe"] = state.get("xghe", []) + flattened

    # --- 🔥 Loại 6 bạn này khỏi not_recent_vsinh ---
    if "not_recent_vsinh" in state:
        if isinstance(state["not_recent_vsinh"], list):
            state["not_recent_vsinh"] = set(state["not_recent_vsinh"])
        state["not_recent_vsinh"] = list(state["not_recent_vsinh"])

    # --- Cập nhật lại not_recent_xghe ---
    print("prev", prev_not_recent)
    if prev_not_recent and len(prev_not_recent) > 1:
        prev_norm = {normalize_name(x) for x in prev_not_recent}
        history_norm = {normalize_name(x) for x in state["xghe"]}
        ex = {normalize_name(x) for x in exceptions}
        new_not_recent_norm = prev_norm - history_norm

        # 🔁 Nếu rỗng → reset pool mới
        if not new_not_recent_norm:
            history_norm = {normalize_name(x) for x in state["xghe"]}
            new_not_recent_norm = (
                {normalize_name(x) for x in all_male_students}
                - ex
                - history_norm
            )
    else:
        history_norm = {normalize_name(x) for x in state["xghe"]}
        ex = {normalize_name(x) for x in exceptions}
        new_not_recent_norm = (
            {normalize_name(x) for x in all_male_students}
            - ex
            - history_norm
        )

    state["not_recent_xghe"] = [
        s for s in all_male_students if normalize_name(s) in new_not_recent_norm
    ]
    save_state(state)
    print("excl:", ex)
    print("history:", history_norm)
    print("new:", new_not_recent_norm)
    print("xghe:", state["xghe"])

    # --- Trả kết quả (danh sách 6 người phẳng, không cặp) ---
    return flattened

# ================== SLASH COMMAND /VSINH ==================
@slash_command(name="vsinh", description="Random XGHE trước rồi VSINH (5 ngày) + Tổ" )
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

        # --- 🪑 1️⃣ RANDOM XGHE TRƯỚC ---
        xghe_picks = random_two_xghe(state, data)

        # --- 🧹 2️⃣ RANDOM VSINH SAU ---
        all_students = candidates_from_data(data, exception_set=exception_set)

        # 👉 Gộp danh sách loại trừ (ngoại lệ + 6 bạn xếp ghế)
        full_exclusions = set(exception_set) | set(xghe_picks)

        vsinh_queue, vipham_after = random_vsinh_complete(
            state,
            all_students,
            exclusions=list(full_exclusions),
            vipham=state.get("vipham", {})
        )

        # --- 🔁 3️⃣ Đồng bộ loại trừ 2 chiều ---
        # Lấy tất cả học sinh được random trực vệ sinh trong tuần này
        vsinh_picks = [hs for pair in vsinh_queue if pair for hs in pair]

        # Xoá các bạn trực vệ sinh khỏi danh sách not_recent_xghe
        if "not_recent_xghe" in state:
            state["not_recent_xghe"] = [hs for hs in state["not_recent_xghe"] if hs not in vsinh_picks]

        # --- 💾 4️⃣ Cập nhật lại state ---
        state["vipham"] = vipham_after
        save_state(state)

        # --- 🧾 5️⃣ Tổng hợp kết quả ---
        vsinh_all = []
        for day, picks in enumerate(vsinh_queue, start=2):
            if picks:
                vsinh_all.append(f"Thứ {day}: {', '.join(p for p in picks)}")

        messages = []
        messages.append(f"🪑 Xếp Ghế: {', '.join(p for p in xghe_picks if p)}")
        messages.append("🧹 Trực Vệ Sinh:\n" + "\n".join(vsinh_all))

        # --- 👥 6️⃣ Cập nhật tổ trực ---
        current_to = state.get("to", 1) + 1
        if current_to > 4:
            current_to = 1
        state["to"] = current_to
        save_state(state)
        messages.append(f"👥 Tổng Vệ Sinh Là Các Học Sinh Trực Tuần Này (Tổ {current_to})")

        # --- 🧾 7️⃣ Ghi log ---
        vsinh_str = "; ".join(" & ".join(pair) for pair in vsinh_queue if pair)
        xghe_str = ", ".join(p for p in xghe_picks if p)
        detail = f"Danh sách trực nhật tuần này,{xghe_str},{vsinh_str},👥 Tổ trực: Tổ {current_to}"
        write_log_TN("Vsinh", str(interaction.user.id), detail)

        # --- 📨 8️⃣ Gửi kết quả ---
        await interaction.followup.send("\n".join(messages))

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi chạy lệnh /vsinh: {e}")

@slash_command(name="addex", description="Thêm ngoại lệ và cập nhật VSINH")
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

@slash_command(name="upvsinh", description="Update lại danh sách trực nhật sau khi bỏ ngoại lệ")
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


@slash_command(name="changevsinh", description="Thay đổi tên trong ngoại lệ")
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

@slash_command(name="vipham", description="Thêm học sinh vi phạm hoặc xem danh sách vi phạm" )
@check_verified_TN()
async def vipham(interaction: discord.Interaction, hoten: str, ngay: str = "1"):
    try:
        state = load_state()

        # --- Nếu người dùng chỉ gõ 'xem' ---
        if normalize_text(hoten) == "xem":
            vipham_data = state.get("vipham", {})
            if not vipham_data:
                await interaction.followup.send("📋 Hiện chưa có học sinh nào vi phạm.")
                return

            msg = "📋 **Danh sách học sinh vi phạm:**\n"
            sorted_data = sorted(vipham_data.items(), key=lambda x: (-x[1], x[0]))  # sắp xếp theo số buổi giảm dần
            for name, count in sorted_data:
                msg += f"• {name}: {count} ngày\n"

            await interaction.followup.send(msg)
            return

        # --- Lấy danh sách học sinh ---
        sheet = client.open_by_url(CHECK_SHEET_URL).sheet1
        all_students_raw = [n for n in sheet.col_values(2) if n]  # Cột B
        all_students_norm = [normalize_text(n) for n in all_students_raw]

        # --- Chuẩn hóa danh sách nhập ---
        hoten_inputs = [x.strip() for x in hoten.replace(";", ",").split(",") if x.strip()]
        ngay_inputs = [x.strip() for x in ngay.replace(";", ",").split(",") if x.strip()]

        # Nếu số lượng buổi < số học sinh → lấy giá trị cuối cùng cho các học sinh còn lại
        if len(ngay_inputs) < len(hoten_inputs):
            ngay_inputs += [ngay_inputs[-1]] * (len(hoten_inputs) - len(ngay_inputs))

        updated, not_found, invalid = [], [], []

        for i, item in enumerate(hoten_inputs):
            try:
                ngay_value = int(ngay_inputs[i])
            except ValueError:
                invalid.append(f"{item} (ngày '{ngay_inputs[i]}')")
                continue

            # --- Nếu là STT ---
            if item.isdigit():
                stt = int(item)
                if 1 <= stt <= len(all_students_raw):
                    hoten_title = all_students_raw[stt - 1].title()
                    hoten_norm = normalize_text(hoten_title)
                else:
                    invalid.append(item)
                    continue
            else:
                hoten_norm = normalize_text(item)
                if hoten_norm in all_students_norm:
                    idx = all_students_norm.index(hoten_norm)
                    hoten_title = all_students_raw[idx].title()
                else:
                    not_found.append(item)
                    continue

            # --- Cộng dồn ---
            current = state["vipham"].get(hoten_title, 0)
            state["vipham"][hoten_title] = current + ngay_value
            updated.append((hoten_title, ngay_value))

            # --- Xóa khỏi danh sách không gần đây ---
            state["not_recent_vsinh"] = [
                name for name in state.get("not_recent_vsinh", [])
                if normalize_text(name) != hoten_norm
            ]
            state["not_recent_xghe"] = [
                name for name in state.get("not_recent_xghe", [])
                if normalize_text(name) != hoten_norm
            ]

        save_state(state)

        # --- Tạo phản hồi ---
        msg = ""
        if updated:
            msg += "✅ **Đã ghi nhận:**\n" + "\n".join(
                [f"• {name} (+{b} buổi, tổng {state['vipham'][name]} buổi)" for name, b in updated]
            ) + "\n"
        if not_found:
            msg += "\n❌ **Không tìm thấy:** " + ", ".join(not_found)
        if invalid:
            msg += "\n⚠️ **Lỗi định dạng/STT không hợp lệ:** " + ", ".join(invalid)

        if not msg:
            msg = "❌ Không có học sinh nào hợp lệ để ghi nhận."

        await interaction.followup.send(msg)

    except Exception as e:
        msg = f"❌ Lỗi khi thêm vi phạm: {e}"
        if interaction.response.is_done():
            await interaction.followup.send(msg)
        else:
            await interaction.response.send_message(msg)

@slash_command(name="datruc", description="xoá học sinh đã trực vệ khỏi danh sách học sinh vệ sinh hoặc xếp ghế (nhiều người cùng lúc)")
@check_verified_TN()
async def datru(interaction: discord.Interaction):
    """
    /datru → bot sẽ hỏi bạn:
    1️⃣ Loại trực: vệ sinh / xếp ghế  
    2️⃣ Nhập danh sách số thứ tự hoặc tên học sinh (vd: 3 5 7 hoặc Nguyễn Văn A, Trần Thị B)
    """
    try:
        state = load_state()
        data = load_data_from_sheet()
        all_students = candidates_from_data(data)
        all_students_norm = [normalize_name(s) for s in all_students]

        # --- Bước 1: Hỏi loại trực ---
        await interaction.followup.send(
            "❓ Bạn muốn ép trực **vệ sinh** hay **xếp ghế**?\n"
            "Gõ `vsinh` hoặc `xghe` để chọn."
        )

        def check_type(msg: discord.Message):
            return (
                msg.author == interaction.user
                and msg.content.lower().strip() in {"vsinh", "xghe"}
            )

        try:
            type_msg = await bot.wait_for("message", check=check_type, timeout=30)
            truc_type = type_msg.content.lower().strip()
        except asyncio.TimeoutError:
            await interaction.followup.send("⌛ Hết thời gian! Hãy gọi lại lệnh `/datru`.")
            return

        # --- Bước 2: Hỏi danh sách ---
        await interaction.followup.send(
            f"✏️ Nhập **số thứ tự hoặc tên học sinh** cần xoá khỏi chưa trực ({'vệ sinh' if truc_type == 'vsinh' else 'xếp ghế'}).\n"
            "Bạn có thể nhập nhiều, cách nhau bằng dấu cách hoặc dấu phẩy.\n"
            "Ví dụ: `3 5 9` hoặc `Nguyễn Văn A, Trần Thị B`"
        )

        def check_list(msg: discord.Message):
            return msg.author == interaction.user and msg.content.strip()

        try:
            list_msg = await bot.wait_for("message", check=check_list, timeout=60)
            danh_sach = list_msg.content
        except asyncio.TimeoutError:
            await interaction.followup.send("⌛ Hết thời gian! Hãy gọi lại lệnh `/datru`.")
            return

        # --- Xử lý danh sách nhập ---
        raw_inputs = [s.strip() for s in danh_sach.replace(",", " ").split() if s.strip()]
        updated = []
        not_found = []

        for item in raw_inputs:
            real_name = None

            if item.isdigit():
                idx = int(item) - 1
                if 0 <= idx < len(all_students):
                    real_name = all_students[idx]
                else:
                    not_found.append(item)
                    continue
            else:
                target = normalize_name(item)
                if target in all_students_norm:
                    real_name = all_students[all_students_norm.index(target)]
                else:
                    not_found.append(item)
                    continue

            # --- Cập nhật tùy loại trực ---
            if truc_type == "vsinh":
                not_recent = set(state.get("not_recent_vsinh", []))
                not_recent.discard(real_name)
                state["not_recent_vsinh"] = list(not_recent)
                state.setdefault("vsinh", [])
                if real_name not in state["vsinh"]:
                    state["vsinh"].append(real_name)
            else:  # xghe
                not_recent = set(state.get("not_recent_xghe", []))
                not_recent.discard(real_name)
                state["not_recent_xghe"] = list(not_recent)
                state.setdefault("xghe", [])
                if real_name not in state["xghe"]:
                    state["xghe"].append(real_name)

            updated.append(real_name)

        save_state(state)

        # --- Log ---
        if updated:
            write_log_TN(
                "Datru",
                str(interaction.user.id),
                f"{'VSINH' if truc_type == 'vsinh' else 'XGHE'}: {', '.join(updated)}"
            )

        # --- Phản hồi ---
        msg = []
        if updated:
            msg.append(
                f"✅ Đã xoá khỏi chưa trực **{('vệ sinh' if truc_type == 'vsinh' else 'xếp ghế')}** cho:\n- "
                + "\n- ".join(updated)
            )
        if not_found:
            msg.append(f"⚠️ Không tìm thấy:\n- " + "\n- ".join(not_found))

        await interaction.followup.send("\n".join(msg) or "❌ Không có học sinh hợp lệ.")

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi chạy lệnh /datru: {e}")

@slash_command(name="resetvsinh", description="Reset dữ liệu VSINH về mặc định")
@check_verified_TN()
async def resetvsinh(interaction: discord.Interaction):
    try:
        # --- Load dữ liệu từ sheet ---
        data = load_data_from_sheet()
        exceptions = get_exception_set(data)  # Set ngoại lệ (không random)

        # --- Lấy danh sách học sinh ---
        all_students = candidates_from_data(data, exception_set=exceptions)
        all_male_students = candidates_from_data(data, only_male=False, exception_set=exceptions)

        # --- Tạo format mặc định ---
        state = {
            "vsinh": [],
            "xghe": [],
            "to": 2,  # giữ mặc định to=2
            "not_recent_vsinh": all_students[:],         # tất cả HS trừ ngoại lệ
            "not_recent_xghe": list(set(all_male_students)),  # chỉ nam trừ ngoại lệ
            "vipham": {}
        }

        # --- Lưu file ---
        save_state(state)

        # --- Log ---
        write_log_TN("ResetVsinh", str(interaction.user.id), "Reset toàn bộ VSINH về mặc định")

        await interaction.followup.send("✅ Đã reset VSINH về trạng thái mặc định.")

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi reset VSINH: {e}")
        
import re
from datetime import datetime
import discord
from discord import app_commands

# Hàm parse ngày nhập dạng dd/mm/yy
def parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%d/%m/%y").date()
    except ValueError:
        return None



@slash_command(name="tongket", description="Tổng kết điểm cộng trong ngày cho một tổ")
@app_commands.describe(nhom="Nhập số tổ", ngay="Ngày/tháng/năm (dd/mm/yy)")
@check_verified_NHOM()
async def tongket(interaction: discord.Interaction, nhom: int, ngay: str):

    # Parse ngày nhập (dd/mm/yy)
    try:
        ngay_dt = datetime.strptime(ngay, "%d/%m/%y").date()
    except ValueError:
        await interaction.followup.send("❌ Sai định dạng ngày. Vui lòng nhập dd/mm/yy (VD: 25/09/25).")
        return

    # 1. Lấy danh sách thành viên từ sheet "Tổ số X"
    sheet_name = f"Tổ {nhom}"
    try:
        to_sheet = sheetmon.worksheet(sheet_name).get_all_values()
    except Exception as e:
        await interaction.followup.send(f"❌ Không tìm thấy sheet {sheet_name}. Lỗi: {e}")
        return

    # Cột B chứa tên học sinh
    to_members = [row[1].strip() for row in to_sheet[1:] if len(row) > 1 and row[1] and row[1].strip().lower() != "họ và tên"]
    if not to_members:
        await interaction.followup.send(f"❌ Không có thành viên nào trong {sheet_name}.")
        return

    # 2. Lấy log từ sheet "Logs"
    try:
        log_sheet = spreadsheet.worksheet("Logs").get_all_values()
    except Exception as e:
        await interaction.followup.send(f"❌ Không tìm thấy sheet log. Lỗi: {e}")
        return

    results = {name: [] for name in to_members}  # {Tên: [(Môn, Loại, Điểm, Lý do)]}

    for row in log_sheet:
        if len(row) > 4:
            time_str = row[0]  # Cột A = thời gian
            content = row[4]   # Cột E = log

            # Parse ngày trong log
            try:
                log_date = datetime.strptime(time_str.split()[0], "%Y-%m-%d").date()
            except Exception:
                continue

            if log_date != ngay_dt:
                continue

            # Nếu tên học sinh có trong log
            for name in to_members:
                if name in content:
                    # CỘNG hoặc TRỪ
                    loai = "CỘNG" if "CỘNG" in content.upper() else "TRỪ" if "TRỪ" in content.upper() else "KHÁC"

                    # Môn học
                    m = re.search(r"\[Môn\s*([^\]]+)\]", content, re.IGNORECASE)
                    mon = m.group(1) if m else "Chưa rõ"

                    # Điểm
                    p = re.search(r"(CỘNG|TRỪ)\s*([\d.,]+)", content, re.IGNORECASE)
                    diem = float(p.group(2).replace(",", ".")) if p else 0

                    # Lý do
                    ly_do = ""
                    l = re.search(r"Lý do[:：]\s*(.+)", content, re.IGNORECASE)
                    if l:
                        ly_do = l.group(1).strip()

                    results[name].append((mon, loai, diem, ly_do))

    # 3. Xuất kết quả ra Embed
    embed = discord.Embed(
        title=f"📊 Tổng kết điểm cộng - {sheet_name} ({ngay})",
        color=discord.Color.green()
    )

    for name, logs in results.items():
        if logs:
            chunks = []
            value = ""
            for mon, loai, diem, ly_do in logs:
                sign = "+" if loai == "CỘNG" else "-"
                entry = f"📘 {mon}: {sign}{diem}\n"
                if ly_do:
                    entry += f"   📝 Lý do: {ly_do}\n"

                # Nếu vượt quá 1024 ký tự thì tách chunk
                if len(value) + len(entry) > 1024:
                    chunks.append(value)
                    value = ""
                value += entry

            if value:
                chunks.append(value)

            for i, chunk in enumerate(chunks):
                field_name = name if i == 0 else f"{name} (tiếp)"
                embed.add_field(name=field_name, value=chunk, inline=False)
        else:
            embed.add_field(name=name, value="Không có điểm cộng", inline=False)

    await interaction.followup.send(embed=embed)


from itertools import zip_longest

@slash_command(name="tongthang", description="Tổng kết điểm của một nhóm trong tháng")
@app_commands.describe(
    nhom="Nhập số nhóm",
    thang="Nhập tháng (1-12)"
)
@check_verified_NHOM()
async def tongthang(interaction: discord.Interaction, nhom: int, thang: int):
    try:
        sheet_name = f"Tổ {nhom}"
        try:
            to_sheet = sheetmon.worksheet(sheet_name).get_all_values()
        except Exception as e:
            await interaction.followup.send(f"❌ Không tìm thấy sheet {sheet_name}. Lỗi: {e}")
            return

        to_members = [
            row[1].strip()
            for row in to_sheet[1:]
            if len(row) > 1 and row[1] and row[1].strip().lower() != "họ và tên"
        ]
        if not to_members:
            await interaction.followup.send(f"❌ Nhóm {nhom} không có thành viên nào.")
            return

        # --- 1. Điểm các môn ---
        mon_list = [
            ws.title
            for ws in sheetmon.worksheets()
            if ws.title not in [f"Tổ {i}" for i in range(1, 10)] and ws.title not in ["Logs", "Phong Trào"]
        ]
        results = {name: {} for name in to_members}
        cong_kiem_tra = {name: [] for name in to_members}
        total_mon = 0.0
        total_bonus = 0.0

        for mon in mon_list:
            ws = sheetmon.worksheet(mon)
            data = ws.get_all_values()
            for row in data[1:]:
                if len(row) >= 13:
                    name = row[1].strip()
                    if name not in to_members:
                        continue

                    # --- CỘNG KIỂM TRA ---
                    # D,E,F,G = cột 4-7 (15 phút)
                    # I = cột 9 (Giữa kỳ), K = cột 11 (Cuối kỳ)
                    bonus_sum = 0.0
                    bonus_lines = []

                    # 15 phút - cột 4,5,6,7
                    for idx, cot in enumerate([4, 5, 6, 7], start=1):
                        try:
                            diem = float(row[cot - 1].replace(",", "."))
                        except:
                            continue
                        cong = 0.0
                        if diem == 10:
                            cong = 2.0
                            bonus_lines.append(f"15 phút đợt {idx}: 💯 (+{cong})")
                        elif diem >= 9:
                            cong = 1.0
                            bonus_lines.append(f"15 phút đợt {idx}: {diem} (+{cong})")
                        elif diem >= 8:
                            cong = 0.5
                            bonus_lines.append(f"15 phút đợt {idx}: {diem} (+{cong})")
                        if cong > 0:
                            bonus_sum += cong

                    # Giữa kỳ - cột 9 (I)
                    try:
                        diem_gk = float(row[8].replace(",", "."))
                        cong_gk = 0.0
                        if diem_gk == 10:
                            cong_gk = 2.0
                            bonus_lines.append(f"Giữa kỳ: 💯 (+{cong_gk})")
                        elif diem_gk >= 9:
                            cong_gk = 1.0
                            bonus_lines.append(f"Giữa kỳ: {diem_gk} (+{cong_gk})")
                        elif diem_gk >= 7.5:
                            cong_gk = 0.5
                            bonus_lines.append(f"Giữa kỳ: {diem_gk} (+{cong_gk})")
                        if cong_gk > 0:
                            bonus_sum += cong_gk
                    except:
                        pass

                    # Cuối kỳ - cột 11 (K)
                    try:
                        diem_ck = float(row[10].replace(",", "."))
                        cong_ck = 0.0
                        if diem_ck == 10:
                            cong_ck = 2.0
                            bonus_lines.append(f"Cuối kỳ: 💯 (+{cong_ck})")
                        elif diem_ck >= 9:
                            cong_ck = 1.0
                            bonus_lines.append(f"Cuối kỳ: {diem_ck} (+{cong_ck})")
                        elif diem_ck >= 7.5:
                            cong_ck = 0.5
                            bonus_lines.append(f"Cuối kỳ: {diem_ck} (+{cong_ck})")
                        if cong_ck > 0:
                            bonus_sum += cong_ck
                    except:
                        pass

                    # Nếu có điểm cộng
                    if bonus_sum > 0:
                        cong_kiem_tra[name].append(f"📘 {mon}:")
                        for line in bonus_lines:
                            cong_kiem_tra[name].append(f" ┗ {line}")
                        cong_kiem_tra[name].append(f" ➕ Tổng điểm cộng kiểm tra: {bonus_sum}\n")
                        total_bonus += bonus_sum

                    # --- Điểm tổng của môn ---
                    try:
                        diem_mon = float(str(row[12]).replace(",", ".").replace("+", "").strip())
                    except:
                        diem_mon = 0.0
                    results[name][mon] = diem_mon
                    total_mon += diem_mon

        # --- 2. Nhật ký bị trừ điểm ---
        try:
            log_sheet = spreadsheet.worksheet(TENBANG).get_all_values()
        except Exception as e:
            await interaction.followup.send(f"❌ Không tìm thấy sheet Logs. Lỗi: {e}")
            return

        deductions = {name: {} for name in to_members}
        for row in log_sheet:
            if len(row) > 4:
                time_str = row[0]
                content = row[4]
                try:
                    log_date = datetime.strptime(time_str.split()[0], "%Y-%m-%d").date()
                except:
                    continue
                if log_date.month != thang:
                    continue
                for name in to_members:
                    if name in content and "TRỪ" in content:
                        m = re.search(r"\[(Môn .+?)\]", content)
                        mon = m.group(1) if m else "Chưa rõ"
                        p = re.search(r"TRỪ\s+([0-9.,]+)", content)
                        diem_tru = p.group(1) if p else "?"
                        ly_do = ""
                        r = re.search(r"Lý do:\s*(.*)", content)
                        if r:
                            ly_do = r.group(1)
                        if log_date not in deductions[name]:
                            deductions[name][log_date] = []
                        deductions[name][log_date].append(f"-{diem_tru} điểm {mon}, lý do: {ly_do}")

        # --- 3. Phong trào ---
        try:
            pt_sheet = sheetphongtrao.get_all_values()
        except Exception as e:
            await interaction.followup.send(f"❌ Không tìm thấy sheet Phong Trào. Lỗi: {e}")
            return

        normalized_members = {n.lower().strip(): n for n in to_members}
        phong_trao_data = {name: [] for name in to_members}
        total_phong_trao = 0.0

        for row in pt_sheet[5:]:
            if len(row) < 10:
                continue
            if not row[0].strip().isdigit():
                continue
            raw_name = row[1].strip()
            if not raw_name:
                continue
            key = raw_name.lower().strip()
            if key not in normalized_members:
                continue
            true_name = normalized_members[key]
            try:
                tong_diem_pt = float(row[5].replace(",", "."))
            except:
                tong_diem_pt = 0.0
            phong_trao_data[true_name].append(f"➡️ Tổng điểm phong trào: {tong_diem_pt}")
            total_phong_trao += tong_diem_pt

        total_group = total_mon + total_phong_trao + total_bonus

                # --- 4. Xuất kết quả (phân trang an toàn không lỗi interaction) ---
        from discord.ui import View, Button

        class PageView(View):
            def __init__(self, embeds):
                super().__init__(timeout=180)
                self.embeds = embeds
                self.current = 0
                self.total = len(embeds)
                self.prev_button = Button(label="⬅️ Trước", style=discord.ButtonStyle.primary)
                self.next_button = Button(label="➡️ Sau", style=discord.ButtonStyle.primary)
                self.prev_button.callback = self.prev_page
                self.next_button.callback = self.next_page
                self.add_item(self.prev_button)
                self.add_item(self.next_button)
                self.update_buttons()

            def update_buttons(self):
                self.prev_button.disabled = self.current == 0
                self.next_button.disabled = self.current == self.total - 1

            async def prev_page(self, interaction: discord.Interaction):
                self.current -= 1
                self.update_buttons()
                embed = self.embeds[self.current]
                await interaction.response.edit_message(embed=embed, view=self)

            async def next_page(self, interaction: discord.Interaction):
                self.current += 1
                self.update_buttons()
                embed = self.embeds[self.current]
                await interaction.response.edit_message(embed=embed, view=self)


        embeds = []

        # --- Tạo embed riêng cho từng thành viên ---
        for idx, name in enumerate(to_members, start=1):
            mon_data = results.get(name, {})
            value = ""

            # --- Điểm môn ---
            if mon_data:
                value += "\n".join([f"📘 {mon}: {diem}" for mon, diem in mon_data.items()])
            else:
                value += "Không có điểm môn."

            # --- Cộng kiểm tra ---
            if cong_kiem_tra.get(name):
                value += "\n\n🧮 **Điểm cộng kiểm tra:**\n" + "\n".join(cong_kiem_tra[name])

            # --- Phong trào ---
            if phong_trao_data.get(name):
                value += "\n\n🏅 **Phong trào:**\n" + "\n".join(phong_trao_data[name])

            # --- Trừ điểm ---
            if deductions.get(name):
                value += "\n\n❌ **Bị trừ điểm:**\n"
                for ngay, ds in deductions[name].items():
                    value += f"📅 {ngay}:\n" + "\n".join([f"   {d}" for d in ds]) + "\n"

            # --- Embed cho từng học sinh ---
            embed = discord.Embed(
                title=f"📊 Tổng kết tháng {thang} - {sheet_name} ({idx}/{len(to_members)})",
                description=f"👤 **{name}**",
                color=discord.Color.blue()
            )
            embed.add_field(name="Chi tiết", value=value[:1024] if len(value) > 1024 else value, inline=False)
            embeds.append(embed)

        # --- Embed tổng kết nhóm ---
        embed_summary = discord.Embed(
            title=f"📌 Tổng kết nhóm {sheet_name} - Tháng {thang}",
            color=discord.Color.gold()
        )
        embed_summary.add_field(
            name="Tổng điểm nhóm",
            value=(
                f"📘 Tổng điểm môn: **{total_mon}**\n"
                f"🧮 Tổng điểm cộng kiểm tra: **{total_bonus}**\n"
                f"🏅 Tổng điểm phong trào: **{total_phong_trao}**\n"
                f"📊 Tổng điểm nhóm: **{total_group}**"
            ),
            inline=False
        )
        embeds.append(embed_summary)

        # --- Gửi phân trang ---
        view = PageView(embeds)
        await interaction.followup.send(embed=embeds[0], view=view)
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi tổng kết: {e}")



def chuan_hoa_text(text: str) -> str:
    return text.strip().title() if text else ""

def merge_keep_none(old_val: str, new_val: str) -> str:
    old_val = str(old_val).strip()
    new_val = str(new_val).strip()
    if not old_val:
        return new_val or "_NONE_"
    if not new_val:
        return old_val
    return f"{old_val},{new_val}"

import asyncio
import discord
from discord import app_commands
from discord.ui import View, Button
from collections import OrderedDict

# --- Hàm hỏi xác nhận bằng button ---
async def ask_confirm(interaction: discord.Interaction, question: str, user: discord.User, timeout: int = 60):
    class ConfirmView(View):
        def __init__(self):
            super().__init__(timeout=timeout)
            self.value = None

        @discord.ui.button(label="✅ Có", style=discord.ButtonStyle.green)
        async def yes(self, i: discord.Interaction, button: Button):
            if i.user.id != user.id:
                await i.response.send_message("Không phải lệnh của bạn.", ephemeral=True)
                return
            self.value = True
            await i.response.edit_message(content="✅ Có", view=None)
            self.stop()

        @discord.ui.button(label="❌ Không", style=discord.ButtonStyle.red)
        async def no(self, i: discord.Interaction, button: Button):
            if i.user.id != user.id:
                await i.response.send_message("Không phải lệnh của bạn.", ephemeral=True)
                return
            self.value = False
            await i.response.edit_message(content="❌ Không", view=None)
            self.stop()

    view = ConfirmView()
    await interaction.followup.send(question, view=view)
    await view.wait()
    return view.value


@slash_command(name="phongtrao", description="Cộng điểm phong trào cho học sinh")
@app_commands.describe(
    stt="Danh sách STT (ngăn cách bằng dấu phẩy, ví dụ: 12,3,4,5)"
)
@check_verified_PT()
async def phongtrao(interaction: discord.Interaction, stt: str):
    try:
        stt_list = [s.strip() for s in stt.split(",") if s.strip()]
        if not stt_list:
            await interaction.followup.send("❌ Bạn chưa nhập STT hợp lệ.")
            return

        # --- Hỏi tên phong trào ---
        await interaction.followup.send("📌 Nhập **tên phong trào**:")

        def check(msg: discord.Message):
            return msg.author.id == interaction.user.id and msg.channel.id == interaction.channel.id

        msg = await bot.wait_for("message", check=check, timeout=120.0)
        phong_trao_name = chuan_hoa_text(msg.content.strip())

        # --- Hỏi số điểm cộng ---
        await interaction.followup.send(
            "📌 Nhập **số điểm cộng**.\n"
            "- Nếu 1 giá trị áp dụng cho tất cả STT: nhập 2\n"
            "- Nếu muốn từng STT 1 giá trị: nhập dạng `2,1,3,0` (số lượng phải bằng số STT)"
        )
        msg = await bot.wait_for("message", check=check, timeout=120.0)
        diem_input = msg.content.strip()
        diem_list_raw = [d.strip() for d in diem_input.split(",") if d.strip()]

        if len(diem_list_raw) == 0:
            await interaction.followup.send("❌ Điểm nhập không hợp lệ.")
            return

        if len(diem_list_raw) == 1 and len(stt_list) > 1:
            diem_list_raw = diem_list_raw * len(stt_list)

        if len(diem_list_raw) != len(stt_list):
            await interaction.followup.send("❌ Số lượng điểm và STT không khớp.")
            return

        diem_values = []
        try:
            for d in diem_list_raw:
                dnum = float(d.replace(",", "."))
                diem_values.append(dnum)
        except Exception:
            await interaction.followup.send("❌ Có giá trị điểm không hợp lệ (phải là số).")
            return

        # --- Hỏi có ai đạt giải không (button) ---
        co_giai = await ask_confirm(interaction, "📌 Có ai đạt giải không?", interaction.user)
        if co_giai is None:
            await interaction.followup.send("⏰ Hết thời gian xác nhận. Hủy thao tác.")
            return

        giai_dict = OrderedDict()
        sheetphongtrao = client.open_by_url(SHEET_PHONGTRAO).worksheet(BANG_PHONGTRAO)
        data = sheetphongtrao.col_values(1)

        if co_giai:
            await interaction.followup.send(
                "📌 Nhập STT và giải theo định dạng: `stt,giải` (mỗi dòng 1 người).\n"
                "Khi xong gõ: `xong`."
            )
            while True:
                msg = await bot.wait_for("message", check=check, timeout=180.0)
                text = msg.content.strip()
                if text.lower() == "xong":
                    break
                if "," not in text:
                    await interaction.followup.send("⚠️ Sai định dạng. Nhập lại theo `stt,giải`.")
                    continue
                stt_g, giai_ten = text.split(",", 1)
                stt_g = stt_g.strip()
                giai_ten = giai_ten.strip()
                if stt_g not in stt_list:
                    await interaction.followup.send(f"❌ STT {stt_g} không có trong danh sách bạn vừa nhập.")
                    continue

                row_index = None
                for i, val in enumerate(data, start=1):
                    if str(val).strip() == stt_g:
                        row_index = i
                        break
                if not row_index:
                    await interaction.followup.send(f"❌ Không tìm thấy STT {stt_g} trong sheet.")
                    continue
                student_name = sheetphongtrao.cell(row_index, 2).value or "<không có tên>"

                await interaction.followup.send(f"📌 Nhập phần thưởng cho {student_name} (STT {stt_g}):")
                msg2 = await bot.wait_for("message", check=check, timeout=120.0)
                phan_thuong = msg2.content.strip()

                giai_dict[stt_g] = {"giai": giai_ten, "phan_thuong": phan_thuong}
                await interaction.followup.send(f"✅ Đã ghi: STT {stt_g} — {giai_ten} — phần thưởng: {phan_thuong}")

        # --- Preview kết quả ---
        stt_to_diem = {s: v for s, v in zip(stt_list, diem_values)}
        preview_lines = []
        for s in stt_list:
            row_index = None
            for i, val in enumerate(data, start=1):
                if str(val).strip() == s:
                    row_index = i
                    break
            name = sheetphongtrao.cell(row_index, 2).value if row_index else "<không tìm thấy>"
            g = giai_dict.get(s, {})
            preview_lines.append(
                f"STT {s} — {name} — +{stt_to_diem[s]} điểm — Giải: {g.get('giai','-')} — Phần thưởng: {g.get('phan_thuong','-')}"
            )

        preview_text = "**Xác nhận trước khi ghi vào sheet:**\n" + "\n".join(preview_lines)

        # --- Xác nhận bằng button ---
        confirm = await ask_confirm(interaction, preview_text + "\n\nBạn có muốn ghi vào sheet không?", interaction.user)
        if confirm is None:
            await interaction.followup.send("⏰ Hết thời gian xác nhận. Hủy thao tác.")
            return
        if not confirm:
            await interaction.followup.send("❌ Đã hủy theo yêu cầu.")
            return

        # --- Ghi vào sheet ---
        results = []
        for idx, s in enumerate(stt_list):
            row_index = None
            for i, val in enumerate(data, start=1):
                if str(val).strip() == s:
                    row_index = i
                    break
            if not row_index:
                results.append(f"❌ STT {s}: không tìm thấy row, bỏ qua.")
                continue

            try:
                cur_pt = sheetphongtrao.cell(row_index, 4).value or "_NONE_"
                cur_diem = sheetphongtrao.cell(row_index, 6).value or "0"
                cur_giai = sheetphongtrao.cell(row_index, 8).value or "_NONE_"
                cur_prize = sheetphongtrao.cell(row_index, 10).value or "_NONE_"

                try:
                    cur_diem_num = float(str(cur_diem).replace(",", "."))
                except:
                    cur_diem_num = 0.0
                new_diem = cur_diem_num + float(stt_to_diem[s])

                pt_with_score = f"{chuan_hoa_text(phong_trao_name)} (+{stt_to_diem[s]})"
                new_phongtrao = merge_keep_none(cur_pt, pt_with_score)

                if s in giai_dict:
                    g = giai_dict[s]
                    new_giai = merge_keep_none(cur_giai, chuan_hoa_text(g["giai"]))
                    new_prize = merge_keep_none(cur_prize, chuan_hoa_text(g["phan_thuong"]))
                else:
                    new_giai = merge_keep_none(cur_giai, "_NONE_")
                    new_prize = merge_keep_none(cur_prize, "_NONE_")

                sheetphongtrao.update_cell(row_index, 4, new_phongtrao or "_NONE_")
                sheetphongtrao.update_cell(row_index, 6, str(new_diem))
                sheetphongtrao.update_cell(row_index, 8, new_giai or "_NONE_")
                sheetphongtrao.update_cell(row_index, 10, new_prize or "_NONE_")

                student_name = sheetphongtrao.cell(row_index, 2).value or "_NONE_"
                results.append(f"✅ STT {s} ({student_name}): +{stt_to_diem[s]} -> tổng {new_diem}")

                write_log_PT("Cộng điểm", str(interaction.user.id),
                    f"STT {s} ({student_name}) được cộng {stt_to_diem[s]} điểm tại phong trào {phong_trao_name}, tổng {new_diem}"
                )
                if s in giai_dict:
                    g = giai_dict[s]
                    write_log_PT("Thêm giải thưởng", str(interaction.user.id),
                        f"STT {s} ({student_name}) đạt giải {g['giai']} — phần thưởng: {g['phan_thuong']}"
                    )

            except Exception as ex_row:
                results.append(f"❌ STT {s}: lỗi khi cập nhật: {ex_row}")

        await interaction.followup.send("\n".join(results))
        await interaction.followup.send("🔄 Đang tạo bản sao trong 24h...")

        try:
            sheet_src = sheetphongtrao
            sheet_dst = sheetmon.worksheet("Phong Trào")

            all_values = sheet_src.get_all_values()

            def clean_value(val: str) -> str:
                if not val:
                    return ""
                parts = [p.strip() for p in val.split(",") if p.strip() and p.strip() != "_NONE_"]
                cleaned = ",".join(parts)
                while ",," in cleaned:
                    cleaned = cleaned.replace(",,", ",")
                return cleaned.strip(",")

            cleaned_data = [
                [clean_value(c) for c in row] for row in all_values
            ]

            sheet_dst.update(cleaned_data)
            await interaction.followup.send("✅ Đã tạo bản sao có hiệu lực trong 24h.")

        except Exception as sync_err:
            await interaction.followup.send(f"⚠️ Lỗi khi đồng bộ: {sync_err}")

    except asyncio.TimeoutError:
        await interaction.followup.send("⏰ Hết thời gian nhập. Thao tác bị hủy.")
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi cộng điểm phong trào: {e}")


import requests
import io
from discord import File
from datetime import datetime, timedelta, timezone

from google.auth.transport.requests import Request
import google.auth

from google.oauth2 import service_account
from google.auth.transport.requests import Request

creds = service_account.Credentials.from_service_account_file(
    "service_account.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"]
)

def export_gsheet_backup(spreadsheet_id, creds, filename):
    creds.refresh(Request())  # tự sinh token
    token = creds.token

    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        return filename
    else:
        raise Exception(f"Lỗi export: {response.status_code} - {response.text}")


@slash_command(name="reset", description="Lưu file backup và reset dữ liệu điểm")
@check_verified()
async def reset(interaction: discord.Interaction):
    try:
        VN_TZ = timezone(timedelta(hours=7))
        timestamp = datetime.now(VN_TZ).strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_{timestamp}.xlsx"

        # 1. Backup trực tiếp từ Google Sheets (giữ format)
        backup_filename = f"backup_{timestamp}.xlsx"
        export_gsheet_backup(sheetmon.id, creds, backup_filename)

        # 2. Reset dữ liệu trong các sheet môn học (xóa cột D từ dòng 6 trở xuống)
        for ws in sheetmon.worksheets():
            if ws.title != "Phong Trào":  # bỏ qua sheet Phong Trào
                last_row = len(ws.col_values(13))  # cột D
                if last_row > 5:  # chỉ reset từ dòng 6
                    ws.batch_clear([f"M5:D{last_row}"])

        # 3. Reset dữ liệu trong bảng PHONGTRAO (cột D → J)
        sheet_PT = sheetmon.worksheet("Phong Trào")
        last_row = len(sheet_PT.col_values(2))  # dựa vào cột B (Họ và Tên)
        if last_row > 5:
            sheet_PT.batch_clear([f"D6:J{last_row}"])

        # 4. Gửi file backup lên Discord
        with open(backup_filename, "rb") as f:
            await interaction.followup.send(
                content=f"✅ Đã reset dữ liệu. File backup `{backup_filename}` được đính kèm:",
                file=File(fp=f, filename=backup_filename),
                ephemeral=True
            )

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi reset: {e}", ephemeral=True)


@slash_command(name="pdf", description="Xuất toàn bộ sheetmon thành file PDF")
@check_verified()
async def export_pdf(interaction: discord.Interaction):
    try:
        VN_TZ = timezone(timedelta(hours=7))
        timestamp = datetime.now(VN_TZ).strftime("%Y%m%d_%H%M%S")
        filename = f"sheetmon_{timestamp}.pdf"

        # --- Lấy token ---
        creds.refresh(Request())
        token = creds.token

        # --- ID của sheetmon ---
        spreadsheet_id = sheetmon.id  

        # --- URL export PDF (Google Sheets hỗ trợ nhiều query param tuỳ chỉnh layout) ---
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=pdf"

        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Lỗi export PDF: {response.status_code} - {response.text}")

        # --- Xuất file PDF ra memory ---
        pdf_bytes = io.BytesIO(response.content)

        # --- Gửi file lên Discord ---
        await interaction.followup.send(
            content=f"📄 Xuất file PDF thành công: `{filename}`",
            file=File(fp=pdf_bytes, filename=filename),
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi export PDF: {e}", ephemeral=True)

@slash_command(name="xghe", description="🎯 Random 6 học sinh nam xếp ghế (3 cặp)" )
@check_verified_TN()
async def xghe(interaction: discord.Interaction):
    try:
         

        # --- Load state và dữ liệu ---
        state = load_state()
        data = load_data_from_sheet()

        # --- Random 6 bạn xếp ghế ---
        picks = random_two_xghe(state, data)

        # --- 🔁 Đồng bộ loại trừ chéo ---
        # Khi đã xếp ghế thì loại họ khỏi not_recent_vsinh
        #vxghe_picks = [hs for pair in picks if pair for hs in pair]
        #if "not_recent_vsinh" in state:
            #if isinstance(state["not_recent_vsinh"], list):
                #state["not_recent_vsinh"] = set(state["not_recent_vsinh"])
            #state["not_recent_vsinh"] -= set(vxghe_picks)
            #state["not_recent_vsinh"] = list(state["not_recent_vsinh"])

        save_state(state)

        # --- Hiển thị kết quả ---
        messages = []
        messages.append(f"🪑 **Danh sách xếp ghế (6 người):**")
        for i in range(0, len(picks), 2):
            pair = picks[i:i+2]
            messages.append(f"  • {', '.join(pair)}")

        # --- Ghi log ---
        write_log_TN("XepGhe", str(interaction.user.id), ", ".join(picks))

        await interaction.followup.send("\n".join(messages))

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi chạy lệnh /xghe: {e}")
    
@slash_command(name="trucvsinh", description="🧹 Random trực vệ sinh (5 ngày)" )
@check_verified_TN()
async def trucvsinh(interaction: discord.Interaction):
    try:
        # --- Load state và dữ liệu ---
        state = load_state()
        data = load_data_from_sheet()
        exception_set = get_exception_set(data)

        # --- Random trực vệ sinh ---
        all_students = candidates_from_data(data, exception_set=exception_set)
        vsinh_queue, vipham_after = await random_vsinh_complete(
        bot,                # thêm dòng này
        interaction,        # thêm dòng này
        state,
        all_students,
        exclusions=list(exception_set),
        vipham=state.get("vipham", {}),
        check_sheet=data    # nếu bạn muốn tra STT -> tên
        )


        # --- 🔁 Đồng bộ loại trừ chéo ---
        # Khi đã trực vệ sinh thì loại khỏi not_recent_xghe
        #vsinh_picks = [hs for pair in vsinh_queue if pair for hs in pair]
        #if "not_recent_xghe" in state:
            #if isinstance(state["not_recent_xghe"], list):
                #state["not_recent_xghe"] = set(state["not_recent_xghe"])
            #state["not_recent_xghe"] -= set(vsinh_picks)
            #state["not_recent_xghe"] = list(state["not_recent_xghe"])

        state["vipham"] = vipham_after
        save_state(state)

        # --- Hiển thị kết quả ---
        vsinh_all = []
        for day, pair in enumerate(vsinh_queue, start=2):
            if pair:
                vsinh_all.append(f"Thứ {day}: {', '.join(pair)}")

        messages = ["🧹 **Danh sách trực vệ sinh:**", "\n".join(vsinh_all)]
        write_log_TN("TrucVsinh", str(interaction.user.id), "; ".join(" & ".join(p) for p in vsinh_queue if p))

        await interaction.followup.send("\n".join(messages))

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi chạy lệnh /trucvsinh: {e}")
        
@slash_command(name="doivsinh", description="🔁 Đổi người trực vệ sinh trong danh sách.")
@check_verified_TN()
async def doivsinh(interaction: discord.Interaction):
    import os, glob, random
    from datetime import datetime

    user_method = {}  # Dùng dict để lưu phương thức mà user chọn theo ID

    def check(msg):
        return msg.author.id == interaction.user.id and msg.channel.id == interaction.channel.id

    await interaction.followup.send(
        "🧹 Nhập **tên hoặc STT muốn đổi**, cách nhau bằng dấu phẩy (,):"
    )

    try:
        msg = await bot.wait_for("message", timeout=60, check=check)
    except:
        return await interaction.followup.send("⏰ Hết thời gian nhập tên.")

    # --- Đọc file vsinh.txt ---
    vsinh_path = "vsinh.txt"
    if not os.path.exists(vsinh_path):
        return await interaction.followup.send("❌ Không tìm thấy file `vsinh.txt`.")

    with open(vsinh_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    vsinh_line = next((l for l in lines if l.startswith("vsinh=")), "")
    not_recent_line = next((l for l in lines if l.startswith("not_recent_vsinh=")), "")

    vsinh = [n.strip() for n in vsinh_line.replace("vsinh=", "").split(",") if n.strip()]
    not_recent = [n.strip() for n in not_recent_line.replace("not_recent_vsinh=", "").split(",") if n.strip()]

    # --- Chuyển tất cả STT sang tên từ Google Sheet ---
    raw_inputs = [n.strip() for n in msg.content.split(",") if n.strip()]
    if not raw_inputs:
        return await interaction.followup.send("❌ Không nhập giá trị hợp lệ.")

    names_to_change = []
    for entry in raw_inputs:
        if entry.isdigit():  # nếu là số thứ tự
            stt = int(entry)
            try:
                cell = check_sheet.find(str(stt), in_column=1)
                name_from_sheet = check_sheet.cell(cell.row, 2).value.strip()
                names_to_change.append(name_from_sheet)
            except Exception:
                return await interaction.followup.send(f"❌ Không tìm thấy STT `{stt}` trong sheet.")
        else:
            names_to_change.append(entry)

    # --- Kiểm tra tên có tồn tại trong danh sách vsinh ---
    invalid = [n for n in names_to_change if n not in vsinh]
    if invalid:
        return await interaction.followup.send(f"❌ Không tìm thấy trong danh sách: {', '.join(invalid)}")

    # Bây giờ names_to_change đã có tên chuẩn, không cần await message lần 2
    # --- Hỏi user có muốn chỉ định không ---
    view = discord.ui.View()

    async def specify_callback(btn_inter: discord.Interaction):
        if btn_inter.user.id != interaction.user.id:
            return await btn_inter.response.send_message("⛔ Không thể xác nhận lệnh của người khác.", ephemeral=True)

        user_method[interaction.user.id] = "Chỉ định"
        await btn_inter.response.defer(ephemeral=True)
        await btn_inter.message.edit(view=None)

        pairs = []

        for old_name in names_to_change:
            while True:
                await btn_inter.channel.send(
                    f"👉 Nhập **tên hoặc STT người muốn đổi với `{old_name}`** "
                    f"(hoặc gõ `huỷ` để random ngẫu nhiên):"
                )

                try:
                    msg2 = await bot.wait_for(
                        "message",
                        timeout=60,
                        check=lambda m: m.author.id == interaction.user.id and m.channel == interaction.channel
                    )
                except asyncio.TimeoutError:
                    await btn_inter.channel.send("⏰ Hết thời gian nhập. Hủy thao tác chỉ định.")
                    return

                user_input = msg2.content.strip()

                # Nếu user chọn huỷ → random
                if user_input.lower() in ["huỷ", "hủy"]:
                    if not_recent:
                        chosen = random.choice(not_recent)
                        pairs.append((old_name, chosen))
                    else:
                        await btn_inter.channel.send("⚠️ Danh sách not_recent đang rỗng, không thể random.")
                        return
                    break

                # Kiểm tra nếu nhập số (STT) → lấy tên từ Google Sheet
                if user_input.isdigit():
                    stt = int(user_input)
                    try:
                        cell = check_sheet.find(str(stt), in_column=1)  # tìm STT ở cột A
                        new_name = check_sheet.cell(cell.row, 2).value.strip()  # lấy tên cột B
                    except Exception:
                        await btn_inter.channel.send(f"❌ Không tìm thấy STT `{stt}` trong sheet.")
                        continue
                else:
                    # nhập tên trực tiếp
                    new_name = user_input

                # Kiểm tra tên có hợp lệ trong not_recent
                if new_name not in not_recent:
                    await btn_inter.channel.send(
                        f"❌ `{new_name}` đã trực hoặc không hợp lệ. Vui lòng nhập tên khác hoặc gõ `huỷ` để random."
                    )
                    continue

                # Nếu hợp lệ → thêm vào pairs
                pairs.append((old_name, new_name))
                break

        # Build preview và gọi confirm flow
        preview_text = "\n".join([f"- {old} ↔ {new}" for old, new in pairs])
        await send_confirm_view(interaction, pairs, vsinh, not_recent, lines, vsinh_path, preview_text, user_method)


    async def random_callback(btn_inter: discord.Interaction):
        if btn_inter.user.id != interaction.user.id:
            return await btn_inter.response.send_message("⛔ Không thể xác nhận lệnh của người khác.", ephemeral=True)
        user_method[interaction.user.id] = "Ngẫu nhiên"  # 🪶 Ghi nhớ lựa chọn
        await btn_inter.response.defer(ephemeral=True)
        await btn_inter.message.edit(view=None)

        random_new = random.sample(not_recent, len(names_to_change))
        pairs = list(zip(names_to_change, random_new))
        preview_text = "\n".join([f"- {old} ↔ {new}" for old, new in pairs])
        await send_confirm_view(interaction, pairs, vsinh, not_recent, lines, vsinh_path, preview_text, user_method)

    btn_specify = discord.ui.Button(label="Chỉ định", style=discord.ButtonStyle.blurple)
    btn_random = discord.ui.Button(label="Ngẫu nhiên", style=discord.ButtonStyle.green)
    btn_specify.callback = specify_callback
    btn_random.callback = random_callback
    view.add_item(btn_specify)
    view.add_item(btn_random)

    await interaction.followup.send(
        "🔧 Vui lòng chọn cách đổi",
        view=view
    )


async def send_confirm_view(interaction, pairs, vsinh, not_recent, lines, vsinh_path, preview_text, user_method):
    import os, glob
    from datetime import datetime

    view = discord.ui.View()

    async def confirm_callback(btn_inter: discord.Interaction):
        nonlocal lines  # ✅ thêm dòng này
        if btn_inter.user.id != interaction.user.id:
            return await btn_inter.response.send_message("⛔ Không thể xác nhận lệnh của người khác.", ephemeral=True)

        # --- Hoán đổi ---
        for old, new in pairs:
            idx = vsinh.index(old)
            vsinh[idx] = new
            if new in not_recent:
                not_recent.remove(new)
            not_recent.append(old)

        # --- 📁 Lưu file backup mới ---
        BACKUP_FOLDER = "backups"
        os.makedirs(BACKUP_FOLDER, exist_ok=True)

        # Tìm file backup gần nhất
        backups = sorted(
            [os.path.join(BACKUP_FOLDER, f) for f in os.listdir(BACKUP_FOLDER) if f.endswith(".txt")],
            key=lambda x: os.path.getmtime(x),
            reverse=True
        )
        latest_backup = backups[0] if backups else None

        # Lấy phần "Còn lại VIPHAM" từ file gần nhất
        vipham_text = "Còn lại VIPHAM:"
        if latest_backup:
            with open(latest_backup, "r", encoding="utf-8") as f:
                old_content = f.read()
                if "Còn lại VIPHAM:" in old_content:
                    vipham_text = "Còn lại VIPHAM:\n" + old_content.split("Còn lại VIPHAM:")[1].strip()

        # --- Đọc danh sách trực vệ sinh từ backup gần nhất ---
        vsinh_per_day = []
        thu_labels = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6"]

        if latest_backup:
            with open(latest_backup, "r", encoding="utf-8") as f:
                backup_lines = f.read().splitlines()

            for label in thu_labels:
                line = next((l for l in backup_lines if l.startswith(label + ":")), None)
                if line and ":" in line:
                    names = [n.strip() for n in line.split(":", 1)[1].split(",") if n.strip()]
                else:
                    names = []
                vsinh_per_day.append(names)
        else:
            vsinh_per_day = [[] for _ in range(5)]

        # --- Hoán đổi tên trong vsinh_per_day ---
        for old, new in pairs:
            for day in vsinh_per_day:
                if old in day:
                    day[day.index(old)] = new
                    break

        # --- Tạo nội dung backup chính xác ---
        new_duty_text = "🧹 Danh sách trực vệ sinh:\n"
        for label, day in zip(thu_labels, vsinh_per_day):
            new_duty_text += f"{label}: {', '.join(day)}\n"

        final_backup_text = f"{new_duty_text.strip()}\n\n{vipham_text}"

        # --- Ghi file backup mới ---
        filename = f"vsinh_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(os.path.join(BACKUP_FOLDER, filename), "w", encoding="utf-8") as f:
            f.write(final_backup_text)

        # --- 🧹 Giới hạn số lượng file backup ---
        backups = sorted(
            [os.path.join(BACKUP_FOLDER, f) for f in os.listdir(BACKUP_FOLDER) if f.endswith(".txt")],
            key=lambda x: os.path.getmtime(x),
            reverse=True
        )
        if len(backups) > 4:
            for old_file in backups[4:]:
                try:
                    os.remove(old_file)
                    print(f"🗑️ Đã xoá file backup cũ: {old_file}")
                except Exception as e:
                    print(f"⚠️ Không thể xoá file {old_file}: {e}")

        # --- Hiển thị danh sách trực vệ sinh hiện tại ---
        current_duty = "\n".join([f"{label}: {', '.join(day)}" for label, day in zip(thu_labels, vsinh_per_day)])
        await btn_inter.response.edit_message(
            content=f"✅ **Đã hoán đổi thành công!**\n{preview_text}\n\n🧹 **Danh sách trực vệ sinh hiện tại:**\n{current_duty}",
            view=None
        )

        # --- Cập nhật file chính ---
        new_lines = []
        for line in lines:
            if line.startswith("vsinh="):
                new_lines.append("vsinh=" + ",".join(vsinh) + "\n")
            elif line.startswith("not_recent_vsinh="):
                new_lines.append("not_recent_vsinh=" + ",".join(not_recent) + "\n")
            else:
                new_lines.append(line)

        with open(vsinh_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
            
        # --- 📋 In lại danh sách trực vệ sinh mới nhất ---
        backups = sorted(
            [os.path.join(BACKUP_FOLDER, f) for f in os.listdir(BACKUP_FOLDER) if f.endswith(".txt")],
            key=lambda x: os.path.getmtime(x),
            reverse=True
        )
        newest_backup = backups[0] if backups else None

        if newest_backup:
            with open(newest_backup, "r", encoding="utf-8") as f:
                content = f.read()

            # Bỏ phần "Còn lại VIPHAM"
            if "Còn lại VIPHAM:" in content:
                content = content.split("Còn lại VIPHAM:")[0].strip()

            # Bỏ dòng đầu "🧹 Danh sách trực vệ sinh:"
            lines = content.splitlines()
            if lines and lines[0].startswith("🧹 Danh sách trực vệ sinh"):
                lines = lines[1:]  # Bỏ dòng tiêu đề đầu tiên
            content = "\n".join(lines).strip()

            #await btn_inter.channel.send(f"🧹 **Danh sách trực vệ sinh hiện tại:**\n{content}")

    async def cancel_callback(btn_inter: discord.Interaction):
        if btn_inter.user.id != interaction.user.id:
            return await btn_inter.response.send_message("⛔ Không thể hủy lệnh của người khác.", ephemeral=True)
        await btn_inter.response.edit_message(content="❌ Đã hủy thao tác.", view=None)

    btn_ok = discord.ui.Button(label="Xác nhận", style=discord.ButtonStyle.green)
    btn_cancel = discord.ui.Button(label="Huỷ", style=discord.ButtonStyle.red)
    btn_ok.callback = confirm_callback
    btn_cancel.callback = cancel_callback
    view.add_item(btn_ok)
    view.add_item(btn_cancel)

    await interaction.followup.send(
        f"🔁 **Dự kiến hoán đổi:**\n{preview_text}\n\nBạn có chắc muốn thực hiện?",
        view=view
    )
    # --- 🪶 Ghi log sau khi xác nhận hoán đổi ---
    try:
        method = user_method.get(interaction.user.id, "Không xác định")
        detail_text = "\n".join([f"{old} ↔ {new}" for old, new in pairs])
        write_log_TN(
            action="HoanDoiTrucVeSinh",
            executor_id=str(interaction.user.id),
            detail=f"Phương thức: {method}\n{detail_text}"
        )
    except Exception as e:
        print(f"Lỗi ghi log sau hoán đổi: {e}")

@bot.command()
@commands.is_owner()
async def clear_commands(ctx, mode: str = None):
    """
    🧹 Xóa slash commands
    Cách dùng:
      !clear_commands global  → xóa toàn bộ slash command GLOBAL
      !clear_commands guild   → xóa slash command trong GUILD test
    """
    if mode not in ["global", "guild"]:
        await ctx.send("⚠️ Dùng đúng cú pháp: `!clear_commands global` hoặc `!clear_commands guild`")
        return

    if mode == "global":
        bot.tree.clear_commands(guild=None)
        await bot.tree.sync()
        await ctx.send("🧹 Đã xóa toàn bộ slash command GLOBAL.")

    elif mode == "guild":
        if not GUILD_ID:
            await ctx.send("❌ Thiếu GUILD_ID trong .env.")
            return
        guild = discord.Object(id=int(GUILD_ID))
        bot.tree.clear_commands(guild=guild)
        await bot.tree.sync(guild=guild)
        await ctx.send(f"🧹 Đã xóa toàn bộ slash command GUILD {GUILD_ID}.")

VN_TZ = timezone(timedelta(hours=7))


VN_TZ = timezone(timedelta(hours=7))

@slash_command(name="grantkey", description="Gia hạn thời gian hiệu lực cho key đã cấp")
@app_commands.describe(
    key="Tên key hoặc STT trong sheetkey (có thể nằm ở cột Q hoặc T)",
    duration="Thời lượng (VD: 2h, 1d, 30m)"
)
@check_verified_ADMIN()
async def grantkey(interaction: discord.Interaction, key: str, duration: str):
    try:
        sheet = verify_sheet
        data = sheet.get_all_values()
        key_row = None
            # 🔍 Tìm trong 2 cột: Q (17) và T (20)
        for i, row in enumerate(data, start=1):
                col_q = row[16].strip() if len(row) > 16 else ""
                col_t = row[19].strip() if len(row) > 19 else ""
                if key.strip() in (col_q, col_t):
                    key_row = i
                    break

        # ❌ Không tìm thấy key
        if not key_row:
            await interaction.followup.send(f"❌ Không tìm thấy key `{key}` trong cột Q hoặc T.")
            return

        # 👤 Lấy ID người được cấp quyền trong cột R (18)
        user_id = data[key_row - 1][17].strip() if len(data[key_row - 1]) > 17 else None

        # 🧮 Phân tích thời lượng nhập vào
        unit = duration[-1].lower()
        value = int(duration[:-1])
        add_seconds = 0
        if unit == "h":
            add_seconds = value * 3600
        elif unit == "d":
            add_seconds = value * 86400
        elif unit == "m":
            add_seconds = value * 60
        else:
            await interaction.followup.send("❌ Sai định dạng thời gian! VD hợp lệ: `2h`, `1d`, `30m`")
            return

        # 🕒 Kiểm tra thời gian cũ (cột U)
        now = datetime.now(VN_TZ)
        expire_timestamp_old = 0
        if len(data[key_row - 1]) > 20 and data[key_row - 1][20].strip():
            try:
                expire_timestamp_old = int(data[key_row - 1][20].strip())
            except:
                expire_timestamp_old = 0

        # 🧩 Nếu thời gian cũ còn hạn → cộng thêm
        if expire_timestamp_old > int(now.timestamp()):
            expire_timestamp_new = expire_timestamp_old + add_seconds
        else:
            expire_timestamp_new = int(now.timestamp()) + add_seconds

        # 🗓️ Định dạng lại thời gian mới
        expire_time = datetime.fromtimestamp(expire_timestamp_new, VN_TZ)
        expire_str = expire_time.strftime("%d/%m/%Y %H:%M")

        # ✏️ Cập nhật thời gian hết hạn và người gia hạn
        sheet.update_cell(key_row, 21, str(expire_timestamp_new))  # Cột U = timestamp
        sheet.update_cell(key_row, 22, f"Gia hạn bởi {interaction.user.name}")  # Cột V = người cập nhật
        sheet.update_cell(key_row, 23, expire_str)  # Cột W = định dạng thời gian dễ đọc

        # 🧾 Ghi log
        write_log(
            "Gia hạn key",
            str(interaction.user.id),
            f"Gia hạn key [{key}] thêm {duration} (đến {expire_str})"
        )

        # 💌 Gửi DM cho người dùng nếu có ID
        if user_id:
            try:
                user = await interaction.client.fetch_user(int(user_id))
                dm_msg = (
                    f"💡 **Key của bạn đã được gia hạn!**\n\n"
                    f"🔑 Key: **{key}**\n"
                    f"⏰ Hiệu lực mới đến: **{expire_str}**\n"
                    f"👤 Người gia hạn: {interaction.user.mention}"
                )
                await user.send(dm_msg)
            except:
                pass

        # ✅ Thông báo trên Discord
        await interaction.followup.send(
            f"✅ Đã gia hạn key `{key}` thêm {duration} (đến **{expire_str}**) "
            f"{'(⏫ Cộng dồn thời gian cũ)' if expire_timestamp_old > int(now.timestamp()) else '(🔁 Tạo mới thời gian)'}"
        )

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi gia hạn key: `{e}`")




@slash_command(name="revokekey", description="Thu hồi quyền của một key bằng STT trong sheetkey")
@app_commands.describe(
    stt="Số thứ tự "
)
@check_verified_ADMIN()
async def revokekey(interaction: discord.Interaction, stt: int):
    try:
        sheet = verify_sheet
        data = sheet.get_all_values()

        key_row = None
        for i, row in enumerate(data, start=1):
            # Cột T là cột thứ 20 (A=1 → T=20)
            if len(row) >= 20 and str(row[19]).strip() == str(stt):
                key_row = i
                break

        if not key_row:
            await interaction.followup.send(f"❌ Không tìm thấy STT `{stt}` trong sheetkey.")
            return

        key = sheet.cell(key_row, 17).value  # Cột Q
        id_in_sheet = str(sheet.cell(key_row, 18).value).strip()  # Cột R
        expire_time = sheet.cell(key_row, 21).value  # Cột U
        note = sheet.cell(key_row, 22).value  # Cột V

        if not id_in_sheet:
            await interaction.followup.send(f"⚠️ Key `{key}` (STT {stt}) hiện không được cấp cho ai.")
            return

        # Xóa quyền
        sheet.update_cell(key_row, 21, "")  # Xóa thời gian
        sheet.update_cell(key_row, 22, f"🔒 Đã bị thu hồi bởi {interaction.user.name}")  # Ghi chú

        # Ghi log
        write_log(
            "Thu hồi key",
            str(interaction.user.id),
            f"Đã thu hồi key [{key}] (STT {stt}) từ ID {id_in_sheet}"
        )

        # Gửi DM nếu tìm được người dùng
        try:
            user = await bot.fetch_user(int(id_in_sheet))
            await user.send(
                f"🔒 **Key `{key}` của bạn (STT {stt}) đã bị thu hồi quyền sử dụng.**\n"
                f"👤 Người thu hồi: {interaction.user.mention}"
            )
        except Exception as e:
            print(f"Không thể gửi DM: {e}")

        await interaction.followup.send(f"✅ Đã thu hồi key `{key}` (STT {stt}) thành công.")

    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi thu hồi key: {e}")


# --- Run Bot ---
@bot.event
async def on_ready():
    print(f"✅ Đăng nhập: {bot.user}")

    if ENV_MODE == "production":
        # 🧹 XÓA GUILD COMMAND (tránh bị trùng)
        for guild in bot.guilds:
            bot.tree.clear_commands(guild=guild)

        # 🔍 Kiểm tra nếu có thay đổi lệnh thì mới sync lại
        existing = [cmd.name for cmd in await bot.tree.fetch_commands()]
        local_cmds = [cmd.name for cmd in bot.tree.get_commands()]
        if set(existing) != set(local_cmds):
            print("🔁 Có thay đổi lệnh — tiến hành sync global...")
            await bot.tree.sync()
        else:
            print("✅ Không có thay đổi lệnh — bỏ qua sync global")

        print("🌍 Slash commands đang chạy ở chế độ PRODUCTION")

    else:
        # 🧪 Chạy local (test nhanh)
        guild = discord.Object(id=int(GUILD_ID))
        await bot.tree.sync(guild=guild)
        print(f"🏠 Slash commands đang chạy test trong guild {guild.id}")
bot.run(TOKEN)