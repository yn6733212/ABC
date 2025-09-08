import requests
import asyncio
import edge_tts
import os
import subprocess
import speech_recognition as sr
import pandas as pd
import yfinance as yf
from difflib import get_close_matches
import re
import shutil
import tarfile
import logging
import warnings
from requests_toolbelt import MultipartEncoder
from flask import Flask, request, jsonify, Response

# ------------ לוגים (קצר ונקי) ------------
LOG_LEVEL = logging.INFO
def setup_logging():
    fmt = "%(asctime)s | %(levelname).1s | %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(level=LOG_LEVEL, format=fmt, datefmt=datefmt)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("edge_tts").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)
    warnings.filterwarnings("ignore", category=ResourceWarning)

setup_logging()
log = logging.getLogger(__name__)

# --- הגדרות מערכת ימות המשיח ---
USERNAME = "0733181201"
PASSWORD = "6714453"
TOKEN = f"{USERNAME}:{PASSWORD}"
UPLOAD_FOLDER_FOR_OUTPUT = "7"

# --- הגדרות קבצים ---
CSV_FILE_PATH = "stock_data.csv"
TEMP_MP3_FILE = "temp_output.mp3"
TEMP_INPUT_WAV = "temp_input.wav"
OUTPUT_AUDIO_FILE_BASE = "000"
OUTPUT_INI_FILE_NAME = "ext.ini"

# --- נתיב להרצת ffmpeg ---
FFMPEG_EXECUTABLE = "ffmpeg"

# --- הגדרת Flask App ---
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
                log.error("לא נמצא קובץ ffmpeg לאחר חילוץ.")
                FFMPEG_EXECUTABLE = "ffmpeg"
        except Exception as e:
            log.error(f"שגיאה בהתקנת FFmpeg: {e}")
            FFMPEG_EXECUTABLE = "ffmpeg"
    else:
        log.info("FFmpeg זמין במערכת.")

def transcribe_audio(filename):
    log.info("תמלול ההקלטה...")
    r = sr.Recognizer()
    try:
        with sr.AudioFile(filename) as source:
            audio = r.record(source)
        recognized_text = r.recognize_google(audio, language="he-IL")
        log.info(f"זוהה דיבור: '{recognized_text}'")
        return recognized_text
    except sr.UnknownValueError:
        log.warning("דיבור לא ברור (לא זוהה).")
        return ""
    except sr.RequestError as e:
        log.error(f"שגיאת חיבור לשירות זיהוי דיבור: {e}")
        return ""
    except Exception as e:
        log.error(f"שגיאה בתמלול: {e}")
        return ""

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
    except FileNotFoundError:
        log.error(f"קובץ לא נמצא: {path}")
        return {}
    except Exception as e:
        log.error(f"שגיאה בטעינת נתוני מניות: {e}")
        return {}

def get_best_match(query, stock_dict):
    matches = get_close_matches(normalize_text(query), stock_dict.keys(), n=1, cutoff=0.7)
    if not matches:
        matches = get_close_matches(normalize_text(query), stock_dict.keys(), n=1, cutoff=0.5)
    return matches[0] if matches else None

def get_stock_price_data(ticker):
    log.info(f"אחזור נתונים: {ticker}")
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="7d")
        if hist.empty or len(hist) < 2:
            log.warning(f"אין מספיק היסטוריה עבור {ticker}.")
            return None
        current_price = hist["Close"].iloc[-1]
        day_before_price = hist["Close"].iloc[-2]
        day_change_percent = (current_price - day_before_price) / day_before_price * 100 if day_before_price else 0
        return {"current": round(current_price, 2), "day_change_percent": round(day_change_percent, 2)}
    except Exception as e:
        log.error(f"שגיאה באחזור {ticker}: {e}")
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
                f.write("playfile_end_goto=/1/2\n")
        log.info("נוצר ext.ini")
        return True
    except Exception as e:
        log.error(f"שגיאה ביצירת INI: {e}")
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
        log.info(f"הועלה: {os.path.basename(file_path)} → {full_upload_path}")
        return True
    except requests.exceptions.RequestException as e:
        log.error(f"שגיאה בהעלאה ({os.path.basename(file_path)}): {e}")
        return False
    except Exception as e:
        log.error(f"שגיאה בהעלאה ({os.path.basename(file_path)}): {e}")
        return False

def convert_mp3_to_wav(mp3_file, wav_file):
    try:
        subprocess.run(
            [FFMPEG_EXECUTABLE, "-loglevel", "error", "-y", "-i", mp3_file,
             "-ar", "8000", "-ac", "1", "-acodec", "pcm_s16le", wav_file],
            check=True
        )
        log.info(f"נוצר WAV: {wav_file}")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"שגיאת FFmpeg: {e}")
    except FileNotFoundError:
        log.error("FFmpeg לא נמצא.")
    except Exception as e:
        log.error(f"שגיאה בהמרה: {e}")
    return False

async def create_audio_file_from_text(text, filename):
    try:
        comm = edge_tts.Communicate(text, voice="he-IL-AvriNeural")
        await comm.save(filename)
        log.info("נוצר MP3 זמני.")
        return True
    except Exception as e:
        log.error(f"שגיאת TTS: {e}")
        return False

def _cleanup_files(paths):
    for f in paths:
        try:
            if f and os.path.exists(f):
                os.remove(f)
                log.info(f"נמחק זמני: {f}")
        except Exception:
            pass

def _api_path_from_target(target_path: str) -> str:
    """
    המרה של 'ivr2:/1/2/2/07/' -> '/1/2/2/07'
    """
    if not target_path:
        return ""
    p = target_path.replace("ivr2:", "")
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/")

# --- פונקציית העיבוד המרכזית ---
async def process_yemot_recording(audio_file_path):
    log.info("עיבוד הקלטה חדשה...")
    stock_data = load_stock_data(CSV_FILE_PATH)
    if not stock_data:
        # מסלול רגיל: יצירת הודעה קולית שגיאה
        response_text = "לא ניתן להמשיך ללא נתוני מניות."
        action_type = "play_file"
        action_value = f"{OUTPUT_AUDIO_FILE_BASE}.wav"
    else:
        recognized_text = transcribe_audio(audio_file_path)
        response_text = ""
        action_type = "play_file"
        action_value = f"{OUTPUT_AUDIO_FILE_BASE}.wav"

        if recognized_text:
            log.info("חיפוש התאמה ברשימת המניות...")
            best_match_key = get_best_match(recognized_text, stock_data)
            if best_match_key:
                log.info(f"התאמה: {best_match_key}")
                stock_info = stock_data[best_match_key]

                # --- שינוי: אם יש שלוחה ייעודית → מחזירים תשובת API טקסטואלית ומעבירים ---
                if stock_info["has_dedicated_folder"] and stock_info["target_path"]:
                    api_path = _api_path_from_target(stock_info["target_path"])
                    log.info(f"החזרת תשובת API למעבר לשלוחה: {api_path}")

                    # ניקוי הקלט לפני החזרה
                    _cleanup_files([audio_file_path])

                    # תשובת טקסט פשוטה כדי שיעבור מיידית
                    return Response(f"go_to_folder={api_path}", mimetype="text/plain; charset=utf-8")

                # --- אחרת: מסלול רגיל (יוצר קבצים ומשמיע) ---
                data = get_stock_price_data(stock_info["symbol"])
                if data:
                    direction = "עלייה" if data["day_change_percent"] > 0 else "ירידה"
                    response_text = (
                        f"מחיר מניית {stock_info['display_name']} עומד כעת על {data['current']} דולר. "
                        f"מתחילת היום נרשמה {direction} של {abs(data['day_change_percent'])} אחוז."
                    )
                    log.info("נתונים הוחזרו בהצלחה.")
                else:
                    response_text = f"מצטערים, לא הצלחנו למצוא נתונים עבור מניית {stock_info['display_name']}."
                    log.warning("לא נמצאו נתונים למניה.")
            else:
                response_text = "לא הצלחנו לזהות את נייר הערך שביקשת. אנא נסה שנית."
                log.warning("לא נמצאה התאמה לרשימה.")
        else:
            response_text = "לא זוהה דיבור ברור בהקלטה. אנא נסה לדבר באופן ברור יותר."
            log.warning("לא זוהה דיבור.")

    # ===== מסלול רגיל (ללא שלוחה ייעודית): ייצור קובץ שמע + ext.ini =====
    generated_audio_success = False
    uploaded_ext_ini = False
    output_yemot_wav_name = f"{OUTPUT_AUDIO_FILE_BASE}.wav"

    if response_text and action_type == "play_file":
        if await create_audio_file_from_text(response_text, TEMP_MP3_FILE):
            if convert_mp3_to_wav(TEMP_MP3_FILE, output_yemot_wav_name):
                if upload_file_to_yemot(output_yemot_wav_name, output_yemot_wav_name):
                    generated_audio_success = True
                else:
                    log.error("כשל בהעלאת קובץ השמע.")
            else:
                log.error("כשל בהמרת MP3 ל-WAV.")
        else:
            log.error("כשל ביצירת קובץ אודיו מטקסט.")
    elif action_type == "go_to_folder":
        generated_audio_success = True

    if generated_audio_success or action_type == "go_to_folder":
        if create_ext_ini_file(action_type, action_value):
            if upload_file_to_yemot(OUTPUT_INI_FILE_NAME, OUTPUT_INI_FILE_NAME):
                uploaded_ext_ini = True
            else:
                log.error(f"כשל בהעלאת {OUTPUT_INI_FILE_NAME}.")
        else:
            log.error(f"כשל ביצירת {OUTPUT_INI_FILE_NAME}.")
    else:
        log.warning("לא נוצרה תגובה קולית/הפניה.")
        _cleanup_files([audio_file_path, TEMP_MP3_FILE])  # ניקוי מינימלי
        return jsonify({"success": False, "message": "Failed to create response"})

    # ניקוי קבצים זמניים
    local_files_to_clean = [audio_file_path, TEMP_MP3_FILE, OUTPUT_INI_FILE_NAME]
    if output_yemot_wav_name and os.path.exists(output_yemot_wav_name) and action_type == "play_file":
        local_files_to_clean.append(output_yemot_wav_name)
    _cleanup_files(local_files_to_clean)

    log.info("סיום עיבוד והעלאה.")
    return jsonify({"success": True})

# --- נקודת קצה של ה-API שתקבל קבצים ---
@app.route('/process_audio', methods=['GET'])
def process_audio_endpoint():
    log.info("בקשה נכנסת (GET /process_audio)")
    stockname = request.args.get('stockname')
    if not stockname:
        log.error("חסר פרמטר 'stockname'.")
        return jsonify({"error": "Missing 'stockname' parameter"}), 400

    yemot_download_url = "https://www.call2all.co.il/ym/api/DownloadFile"
    file_path_on_yemot = f"ivr2:/{stockname.lstrip('/')}"
    params = {"token": TOKEN, "path": file_path_on_yemot}

    try:
        log.info(f"הורדת אודיו: {file_path_on_yemot}")
        response = requests.get(yemot_download_url, params=params, timeout=30)
        response.raise_for_status()

        file_path = TEMP_INPUT_WAV
        with open(file_path, 'wb') as f:
            f.write(response.content)

        log.info("ההורדה הושלמה.")
        # שים לב: הפונקציה עלולה להחזיר Response טקסטואלי (go_to_folder=...) או JSON
        result = asyncio.run(process_yemot_recording(file_path))
        return result

    except requests.exceptions.RequestException as e:
        log.error(f"שגיאה בהורדה מימות: {e}")
        return jsonify({"error": "Failed to download audio file"}), 500
    except Exception as e:
        log.error(f"שגיאה בעיבוד: {e}")
        return jsonify({"error": "Failed to process audio"}), 500

if __name__ == "__main__":
    ensure_ffmpeg()
    _ = load_stock_data(CSV_FILE_PATH)
    log.info("השרת עלה. ממתין לבקשות...")
    app.run(host='0.0.0.0', port=5000, use_reloader=False)
