import requests
import asyncio
import edge_tts
import os
import subprocess
import speech_recognition as sr
import pandas as pd
import yfinance as yf
import re
import logging
import warnings
from requests_toolbelt import MultipartEncoder
from flask import Flask, request, jsonify, Response
from difflib import get_close_matches

# ===== קונפיגורציית לוגים =====
# מציגים רק שגיאות מספריות (אדום) מהמערכת/ספריות.
logging.basicConfig(level=logging.ERROR, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("werkzeug").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("edge_tts").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.ERROR)
logging.getLogger("yfinance").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# לוג ירוק קצר (כאשר הכול תקין)
GREEN = "\033[32m"
RESET = "\033[0m"
def glog(msg: str):
    print(f"{GREEN}{msg}{RESET}")

log = logging.getLogger(__name__)

# --- הגדרות מערכת ימות המשיח ---
USERNAME = "0733181201"
PASSWORD = "6714453"
TOKEN = f"{USERNAME}:{PASSWORD}"

# **שלוחה יעד ברירת מחדל (ללא שלוחה ייעודית)**
UPLOAD_FOLDER_FOR_OUTPUT = "7"

# --- הגדרות קבצים ---
CSV_FILE_PATH = "stock_data.csv"
TEMP_MP3_FILE = "temp_output.mp3"
TEMP_INPUT_WAV = "temp_input.wav"
OUTPUT_AUDIO_FILE_BASE = "000"

# --- נתיב להרצת ffmpeg ---
FFMPEG_EXECUTABLE = "ffmpeg"

# --- Flask App ---
app = Flask(__name__)

# ----------------------- פונקציות עזר -----------------------
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
        return stock_data
    except Exception as e:
        log.exception("שגיאה בטעינת נתוני מניות")
        return {}

def get_best_match(query, stock_dict):
    """
    התאמה חלקית בלבד: difflib עם cutoff נמוך יחסית.
    """
    matches = get_close_matches(normalize_text(query), stock_dict.keys(), n=1, cutoff=0.6)
    return matches[0] if matches else None

def transcribe_audio(filename):
    r = sr.Recognizer()
    try:
        with sr.AudioFile(filename) as source:
            audio = r.record(source)
        recognized_text = r.recognize_google(audio, language="he-IL")
        if recognized_text:
            glog(f"🗣️ זוהה דיבור: \"{recognized_text}\"")
        return recognized_text
    except Exception:
        log.exception("שגיאה בתמלול")
        return ""

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
    except Exception:
        log.exception("שגיאה באחזור נתוני מניה")
        return None

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
    except Exception:
        log.exception("שגיאה בהעלאת קובץ לימות")
        return False

def convert_mp3_to_wav(mp3_file, wav_file):
    try:
        subprocess.run(
            [FFMPEG_EXECUTABLE, "-loglevel", "error", "-y", "-i", mp3_file,
             "-ar", "8000", "-ac", "1", "-acodec", "pcm_s16le", wav_file],
            check=True
        )
        return True
    except Exception:
        log.exception("שגיאה בהמרת MP3 ל-WAV")
        return False

async def create_audio_file_from_text(text, filename):
    try:
        comm = edge_tts.Communicate(text, voice="he-IL-AvriNeural")
        await comm.save(filename)
        return True
    except Exception:
        log.exception("שגיאת TTS")
        return False

def _cleanup_files(paths):
    for f in paths:
        try:
            if f and os.path.exists(f):
                os.remove(f)
        except Exception:
            # ניקוי שקט
            pass

def _api_path_from_target(target_path: str) -> str:
    if not target_path:
        return ""
    p = target_path.replace("ivr2:", "")
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/")

# ----------------------- פונקציית העיבוד הראשית -----------------------
async def process_yemot_recording(audio_file_path):
    try:
        stock_data = load_stock_data(CSV_FILE_PATH)
        default_api_path = f"/{UPLOAD_FOLDER_FOR_OUTPUT}"

        if not stock_data:
            _cleanup_files([audio_file_path])
            glog("🎉 הסתיים בהצלחה")
            return Response(f"go_to_folder={default_api_path}", mimetype="text/plain; charset=utf-8")

        recognized_text = transcribe_audio(audio_file_path)
        response_text = ""
        best_match_key = None

        if recognized_text:
            best_match_key = get_best_match(recognized_text, stock_data)

        if best_match_key:
            glog("🔎 נמצאה התאמה")
            stock_info = stock_data[best_match_key]
            glog(f"✅ {stock_info['display_name']}")

            if stock_info["has_dedicated_folder"] and stock_info["target_path"]:
                api_path = _api_path_from_target(stock_info["target_path"])
                _cleanup_files([audio_file_path])
                glog("🎉 הסתיים בהצלחה")
                return Response(f"go_to_folder={api_path}", mimetype="text/plain; charset=utf-8")

            data = get_stock_price_data(stock_info["symbol"])
            if data:
                direction = "עלייה" if data["day_change_percent"] > 0 else "ירידה"
                response_text = (
                    f"מחיר מניית {stock_info['display_name']} עומד כעת על {data['current']} דולר. "
                    f"מתחילת היום נרשמה {direction} של {abs(data['day_change_percent'])} אחוז."
                )
            else:
                response_text = f"לא נמצאו נתונים עבור מניית {stock_info['display_name']}."
        else:
            if recognized_text:
                response_text = "לא נמצאה התאמה לנייר הערך שביקשת. נסה שוב."
            else:
                response_text = "לא זוהה דיבור ברור בהקלטה. נסה לדבר ברור יותר."

        # הפקת קובץ אודיו ושליחה לשלוחה (בלי לוג ירוק על ההפקה)
        output_yemot_wav_name = f"{OUTPUT_AUDIO_FILE_BASE}.wav"
        if response_text:
            if await create_audio_file_from_text(response_text, TEMP_MP3_FILE):
                if convert_mp3_to_wav(TEMP_MP3_FILE, output_yemot_wav_name):
                    upload_file_to_yemot(output_yemot_wav_name, output_yemot_wav_name)

        _cleanup_files([audio_file_path, TEMP_MP3_FILE, output_yemot_wav_name if os.path.exists(output_yemot_wav_name) else None])
        glog("🎉 הסתיים בהצלחה")
        return Response(f"go_to_folder={default_api_path}", mimetype="text/plain; charset=utf-8")

    except Exception:
        # אם יש שגיאה — כן מציגים את השגיאה (אדום), ולא מוסיפים לוגים ירוקים נוספים
        log.exception("שגיאה בעיבוד הקלטה")
        return jsonify({"error": "Failed to process audio"}), 500

# ----------------------- API Endpoint -----------------------
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

    except Exception:
        log.exception("שגיאה ב-endpoint /process_audio")
        return jsonify({"error": "Failed to process audio"}), 500

if __name__ == "__main__":
    # כאן תראה לוגים “בהתחלה” אם יש הורדות/התקנות חיצוניות כחלק מהרצה בסביבה שלך
    print("השרת עלה. ממתין לבקשות...")
    app.run(host='0.0.0.0', port=5000, use_reloader=False)
