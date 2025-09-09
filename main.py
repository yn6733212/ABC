import requests
import asyncio
import edge_tts
import os
import subprocess
import speech_recognition as sr
import pandas as pd
import yfinance as yf
import re
import shutil
import tarfile
import logging
import warnings
from requests_toolbelt import MultipartEncoder
from flask import Flask, request, jsonify, Response
from rapidfuzz import process, fuzz  # מודול התאמה חכמה

# ------------ לוגים (קצר ונקי) ------------
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")
log = logging.getLogger(__name__)

# צבעים ללוגים
RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"

def info_log(msg):
    print(f"{GREEN}{msg}{RESET}")

def error_log(msg):
    print(f"{RED}{msg}{RESET}")

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=ResourceWarning)

# --- הגדרות מערכת ימות המשיח ---
USERNAME = "0733181201"
PASSWORD = "6714453"
TOKEN = f"{USERNAME}:{PASSWORD}"
UPLOAD_FOLDER_FOR_OUTPUT = "7"  # ברירת מחדל: שלוחה 7 היא שלוחת התוצאות

# --- הגדרות קבצים ---
CSV_FILE_PATH = "stock_data.csv"
TEMP_MP3_FILE = "temp_output.mp3"
TEMP_INPUT_WAV = "temp_input.wav"
OUTPUT_AUDIO_FILE_BASE = "000"
OUTPUT_INI_FILE_NAME = "ext.ini"

# --- נתיב להרצת ffmpeg ---
FFMPEG_EXECUTABLE = "ffmpeg"

# --- Flask App ---
app = Flask(__name__)

def ensure_ffmpeg():
    log.info("בודק FFmpeg...")
    global FFMPEG_EXECUTABLE
    if not shutil.which("ffmpeg"):
        log.info("FFmpeg לא נמצא, מתקין...")
        ffmpeg_bin_dir = "ffmpeg_bin"
        os.makedirs(ffmpeg_bin_dir, exist_ok=True)
        ffmpeg_url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
        archive_path = os.path.join(ffmpeg_bin_dir, "ffmpeg.tar.xz")
        try:
            r = requests.get(ffmpeg_url, stream=True, timeout=60)
            r.raise_for_status()
            with open(archive_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            with tarfile.open(archive_path, 'r:xz') as tar_ref:
                tar_ref.extractall(ffmpeg_bin_dir)
            os.remove(archive_path)

            found_ffmpeg_path = None
            for root, _, files in os.walk(ffmpeg_bin_dir):
                if "ffmpeg" in files:
                    found_ffmpeg_path = os.path.join(root, "ffmpeg")
                    break
            if found_ffmpeg_path:
                FFMPEG_EXECUTABLE = found_ffmpeg_path
                os.environ["PATH"] += os.pathsep + os.path.dirname(FFMPEG_EXECUTABLE)
                if os.name == 'posix':
                    os.chmod(FFMPEG_EXECUTABLE, 0o755)
                log.info(f"FFmpeg הותקן: {FFMPEG_EXECUTABLE}")
            else:
                error_log("❌ לא נמצא קובץ ffmpeg לאחר חילוץ.")
                FFMPEG_EXECUTABLE = "ffmpeg"
        except Exception as e:
            error_log(f"❌ שגיאה בהתקנת FFmpeg: {e}")
            FFMPEG_EXECUTABLE = "ffmpeg"
    else:
        log.info("FFmpeg זמין במערכת.")

# --- n-best זיהוי דיבור ---
def transcribe_audio(filename):
    r = sr.Recognizer()
    try:
        with sr.AudioFile(filename) as source:
            audio = r.record(source)

        result = r.recognize_google(audio, language="he-IL", show_all=True)
        alternatives = []
        if isinstance(result, dict) and "alternative" in result:
            for alt in result["alternative"]:
                text = alt.get("transcript")
                if text:
                    alternatives.append(text)

        if not alternatives:
            return []

        info_log("-------------------------------------")
        info_log(f"🗣️ זוהה דיבור: {alternatives[0]}")
        return alternatives
    except sr.UnknownValueError:
        error_log("❌ לא זוהה דיבור ברור.")
        return []
    except sr.RequestError as e:
        error_log(f"❌ שגיאת חיבור לשירות זיהוי דיבור: {e}")
        return []
    except Exception as e:
        error_log(f"❌ שגיאה בתמלול: {e}")
        return []

# --- התאמה חכמה בעזרת RapidFuzz ---
def get_best_match(hypotheses, stock_dict):
    best_match = None
    best_score = -1
    for h in hypotheses:
        normalized = normalize_text(h)
        match = process.extractOne(normalized, stock_dict.keys(), scorer=fuzz.token_set_ratio, score_cutoff=60)
        if match and match[1] > best_score:
            best_match, best_score = match[0], match[1]
    return best_match

def normalize_text(text):
    if not isinstance(text, str):
        if pd.isna(text):
            text = ""
        else:
            text = str(text)
    return re.sub(r'[^א-תa-zA-Z0-9 ]', '', text).lower().strip()

def load_stock_data(path):
    try:
        df = pd.read_csv(path)
        stock_data = {}
        for _, row in df.iterrows():
            name = row.get("name")
            symbol = row.get("symbol")
            display_name = row.get("display_name", name)
            type_ = row.get("type")
            has_dedicated_folder = str(row.get("has_dedicated_folder", "false")).lower() == 'true'
            target_path = row.get("target_path", "")
            if name and symbol and type_:
                stock_data[normalize_text(name)] = {
                    "symbol": symbol,
                    "display_name": display_name,
                    "type": type_,
                    "has_dedicated_folder": has_dedicated_folder,
                    "target_path": target_path if has_dedicated_folder and pd.notna(target_path) else ""
                }
        log.info(f"נטענו נתוני מניות ({len(stock_data)} פריטים).")
        return stock_data
    except Exception as e:
        error_log(f"❌ שגיאה בטעינת נתוני מניות: {e}")
        return {}

def get_stock_price_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="7d")
        if hist.empty or len(hist) < 2:
            return None
        current_price = hist["Close"].iloc[-1]
        day_before_price = hist["Close"].iloc[-2]
        day_change_percent = (current_price - day_before_price) / day_before_price * 100 if day_before_price else 0
        return {"current": round(current_price, 2), "day_change_percent": round(day_change_percent, 2)}
    except Exception as e:
        error_log(f"❌ שגיאה באחזור נתונים: {e}")
        return None

def create_ext_ini_file(action_type, value):
    try:
        with open(OUTPUT_INI_FILE_NAME, 'w', encoding='windows-1255') as f:
            if action_type == "go_to_folder":
                f.write("type=go_to_folder\n")
                relative_path = value.replace("ivr2:", "").rstrip('/')
                f.write(f"go_to_folder={relative_path}\n")
            elif action_type == "play_file":
                f.write("type=playfile\n")
                f.write("playfile_end_goto=/7\n")  # לאחר ההשמעה מעביר לשלוחה 7
        return True
    except Exception as e:
        error_log(f"❌ שגיאה ביצירת INI: {e}")
        return False

def upload_file_to_yemot(file_path, yemot_file_name_or_path_on_yemot):
    full_upload_path = f"ivr2:/{UPLOAD_FOLDER_FOR_OUTPUT}/{yemot_file_name_or_path_on_yemot}"
    try:
        m = MultipartEncoder(fields={
            "token": TOKEN,
            "path": full_upload_path,
            "upload": (os.path.basename(file_path), open(file_path, 'rb'),
                       'audio/wav' if file_path.endswith('.wav') else 'text/plain')
        })
        r = requests.post("https://www.call2all.co.il/ym/api/UploadFile",
                          data=m, headers={'Content-Type': m.content_type}, timeout=30)
        r.raise_for_status()
        return True
    except Exception as e:
        error_log(f"❌ שגיאה בהעלאת קובץ: {e}")
        return False

def convert_mp3_to_wav(mp3_file, wav_file):
    try:
        subprocess.run(
            [FFMPEG_EXECUTABLE, "-loglevel", "error", "-y", "-i", mp3_file,
             "-ar", "8000", "-ac", "1", "-acodec", "pcm_s16le", wav_file],
            check=True
        )
        return True
    except Exception as e:
        error_log(f"❌ שגיאת FFmpeg: {e}")
        return False

async def create_audio_file_from_text(text, filename):
    try:
        comm = edge_tts.Communicate(text, voice="he-IL-AvriNeural")
        await comm.save(filename)
        return True
    except Exception as e:
        error_log(f"❌ שגיאת TTS: {e}")
        return False

def _cleanup_files(paths):
    for f in paths:
        try:
            if f and os.path.exists(f):
                os.remove(f)
        except:
            pass

def _api_path_from_target(target_path: str) -> str:
    if not target_path:
        return ""
    p = target_path.replace("ivr2:", "")
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/")

# --- פונקציית העיבוד המרכזית ---
async def process_yemot_recording(audio_file_path):
    stock_data = load_stock_data(CSV_FILE_PATH)
    if not stock_data:
        error_log("❌ אין נתוני מניות.")
        return jsonify({"success": False})

    # זיהוי דיבור עם n-best
    hypotheses = transcribe_audio(audio_file_path)
    if not hypotheses:
        error_log("❌ לא זוהה דיבור.")
        return jsonify({"success": False})

    # התאמה חכמה
    best_match_key = get_best_match(hypotheses, stock_data)
    if not best_match_key:
        error_log("❌ לא נמצאה התאמה לרשימת המניות.")
        return jsonify({"success": False})

    stock_info = stock_data[best_match_key]
    info_log(f"🔎 נמצאה התאמה: {stock_info['display_name']}")

    # אם יש שלוחה ייעודית
    if stock_info["has_dedicated_folder"] and stock_info["target_path"]:
        api_path = _api_path_from_target(stock_info["target_path"])
        _cleanup_files([audio_file_path])
        info_log("🎉 הסתיים בהצלחה")
        return Response(f"go_to_folder={api_path}", mimetype="text/plain; charset=utf-8")

    # שליפת נתוני מניה
    data = get_stock_price_data(stock_info["symbol"])
    if data:
        direction = "עלייה" if data["day_change_percent"] > 0 else "ירידה"
        response_text = (
            f"מחיר מניית {stock_info['display_name']} עומד כעת על {data['current']} דולר. "
            f"מתחילת היום נרשמה {direction} של {abs(data['day_change_percent'])} אחוז."
        )
    else:
        response_text = f"לא נמצאו נתונים עבור מניית {stock_info['display_name']}."

    # הפקת קובץ קול
    output_yemot_wav_name = f"{OUTPUT_AUDIO_FILE_BASE}.wav"
    if await create_audio_file_from_text(response_text, TEMP_MP3_FILE):
        if convert_mp3_to_wav(TEMP_MP3_FILE, output_yemot_wav_name):
            upload_file_to_yemot(output_yemot_wav_name, output_yemot_wav_name)

    _cleanup_files([audio_file_path, TEMP_MP3_FILE, output_yemot_wav_name])
    info_log("🎉 הסתיים בהצלחה")
    return Response(f"go_to_folder=/7", mimetype="text/plain; charset=utf-8")

# --- API Endpoint ---
@app.route('/process_audio', methods=['GET'])
def process_audio_endpoint():
    try:
        stockname = request.args.get('stockname')
        if not stockname:
            return jsonify({"error": "Missing 'stockname' parameter"}), 400

        yemot_download_url = "https://www.call2all.co.il/ym/api/DownloadFile"
        file_path_on_yemot = f"ivr2:/{stockname.lstrip('/')}"
        params = {"token": TOKEN, "path": file_path_on_yemot}

        response = requests.get(yemot_download_url, params=params, timeout=30)
        response.raise_for_status()
        with open(TEMP_INPUT_WAV, 'wb') as f:
            f.write(response.content)

        result = asyncio.run(process_yemot_recording(TEMP_INPUT_WAV))
        return result

    except Exception as e:
        error_log(f"❌ שגיאה כללית: {e}")
        return jsonify({"error": "Failed to process audio"}), 500

if __name__ == "__main__":
    ensure_ffmpeg()
    _ = load_stock_data(CSV_FILE_PATH)
    log.info("השרת עלה. ממתין לבקשות...")
    app.run(host='0.0.0.0', port=5000, use_reloader=False)
