import requests
import asyncio
import edge_tts
import os
import subprocess
import speech_recognition as sr
import pandas as pd
import yfinance as yf
from difflib import get_close_matches
from requests_toolbelt.multipart.encoder import MultipartEncoder
import re
import shutil
import tarfile
from flask import Flask, request, jsonify

# --- הגדרות מערכת ימות המשיח ---
USERNAME = "0733181201"
PASSWORD = "6714453"
TOKEN = f"{USERNAME}:{PASSWORD}"
UPLOAD_FOLDER_FOR_OUTPUT = "11"

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
    """מוודא ש-FFmpeg מותקן ונגיש."""
    print("⏳ בודק זמינות FFmpeg...")
    global FFMPEG_EXECUTABLE
    if not shutil.which("ffmpeg"):
        print("⬇️ מתקין ffmpeg...")
        ffmpeg_bin_dir = "ffmpeg_bin"
        os.makedirs(ffmpeg_bin_dir, exist_ok=True)
        ffmpeg_url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
        archive_path = os.path.join(ffmpeg_bin_dir, "ffmpeg.tar.xz")
        try:
            r = requests.get(ffmpeg_url, stream=True)
            r.raise_for_status()
            with open(archive_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("✅ הורדת ffmpeg הושלמה.")
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
                print(f"✅ ffmpeg הותקן והוגדר בהצלחה: {FFMPEG_EXECUTABLE}")
            else:
                print("❌ שגיאה: לא נמצא קובץ הפעלה של ffmpeg לאחר החילוץ.")
                FFMPEG_EXECUTABLE = "ffmpeg"
        except Exception as e:
            print(f"❌ שגיאה בהתקנת ffmpeg: {e}")
            FFMPEG_EXECUTABLE = "ffmpeg"
    else:
        print("⏩ ffmpeg כבר קיים ב-PATH של המערכת.")
        FFMPEG_EXECUTABLE = "ffmpeg"

def transcribe_audio(filename):
    """מתמלל קובץ אודיו באמצעות Google Speech Recognition."""
    print("🎤 מתחיל בתמלול ההקלטה...")
    r = sr.Recognizer()
    try:
        with sr.AudioFile(filename) as source:
            audio = r.record(source)
        recognized_text = r.recognize_google(audio, language="he-IL")
        print(f"👂 זיהה דיבור בהצלחה: '{recognized_text}'")
        return recognized_text
    except sr.UnknownValueError:
        print("❌ זיהוי דיבור נכשל: לא זוהה דיבור ברור.")
        return ""
    except sr.RequestError as e:
        print(f"❌ שגיאה בחיבור לשירות זיהוי הדיבור של גוגל: {e}.")
        return ""
    except Exception as e:
        print(f"❌ שגיאה בלתי צפויה בתמלול: {e}")
        return ""

def normalize_text(text):
    """מנרמל טקסט להשוואה."""
    if not isinstance(text, str):
        if pd.isna(text):
            text = ""
        else:
            text = str(text)
    return re.sub(r'[^א-תa-zA-Z0-9 ]', '', text).lower().strip()

def load_stock_data(path):
    """טוען נתוני מניות מקובץ CSV."""
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
        print(f"✅ נתוני מניות נטענו בהצלחה מ- {path}")
        return stock_data
    except FileNotFoundError:
        print(f"❌ שגיאה: הקובץ {path} לא נמצא. לא ניתן להמשיך.")
        return {}
    except Exception as e:
        print(f"❌ שגיאה בטעינת נתוני מניות: {e}")
        return {}

def get_best_match(query, stock_dict):
    """מוצא את ההתאמה הטובה ביותר לשאילתה מתוך רשימת המניות."""
    matches = get_close_matches(normalize_text(query), stock_dict.keys(), n=1, cutoff=0.7)
    if not matches:
        matches = get_close_matches(normalize_text(query), stock_dict.keys(), n=1, cutoff=0.5)
    return matches[0] if matches else None

def get_stock_price_data(ticker):
    """מביא נתוני מחיר ושינוי יומי עבור מניה."""
    print(f"📈 מאחזר נתונים עדכניים עבור {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="7d")
        if hist.empty or len(hist) < 2:
            print(f"⚠️ אין מספיק נתוני היסטוריה עבור {ticker}.")
            return None
        current_price = hist["Close"].iloc[-1]
        day_before_price = hist["Close"].iloc[-2]
        day_change_percent = (current_price - day_before_price) / day_before_price * 100 if day_before_price else 0
        return {"current": round(current_price, 2), "day_change_percent": round(day_change_percent, 2)}
    except Exception as e:
        print(f"❌ שגיאה באחזור נתונים עבור {ticker}: {e}")
        return None

def create_ext_ini_file(action_type, value):
    """יוצר קובץ ext.ini להפנייה בימות המשיח."""
    print(f"📝 יוצר קובץ ext.ini...")
    try:
        with open(OUTPUT_INI_FILE_NAME, 'w', encoding='windows-1255') as f:
            if action_type == "go_to_folder":
                f.write(f"type=go_to_folder\n")
                relative_path = value.replace("ivr2:", "").rstrip('/')
                f.write(f"go_to_folder={relative_path}\n")
            elif action_type == "play_file":
                f.write(f"type=playfile\n")
                f.write(f"playfile_end_goto=/1/2\n")
        print("✅ קובץ ext.ini נוצר בהצלחה.")
        return True
    except Exception as e:
        print(f"❌ שגיאה ביצירת קובץ INI: {e}")
        return False

def upload_file_to_yemot(file_path, yemot_file_name_or_path_on_yemot):
    """מעלה קובץ (אודיו או INI) לימות המשיח."""
    print(f"⬆️ מעלה קובץ '{os.path.basename(file_path)}' לימות המשיח...")
    full_upload_path = f"ivr2:/{UPLOAD_FOLDER_FOR_OUTPUT}/{yemot_file_name_or_path_on_yemot}"
    m = MultipartEncoder(fields={
        "token": TOKEN,
        "path": full_upload_path,
        "upload": (os.path.basename(file_path), open(file_path, 'rb'), 'audio/wav' if file_path.endswith('.wav') else 'text/plain')
    })
    try:
        r = requests.post("https://www.call2all.co.il/ym/api/UploadFile", data=m, headers={'Content-Type': m.content_type})
        r.raise_for_status()
        print(f"✅ הקובץ '{os.path.basename(file_path)}' הועלה בהצלחה.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ שגיאה בהעלאת קובץ לימות המשיח ({os.path.basename(file_path)}): {e}")
        return False
    except Exception as e:
        print(f"❌ שגיאה בלתי צפויה בהעלאת קובץ לימות המשיח ({os.path.basename(file_path)}): {e}")
        return False

def convert_mp3_to_wav(mp3_file, wav_file):
    """ממיר קובץ MP3 ל-WAV באמצעות FFmpeg."""
    print("🔄 ממיר קובץ MP3 ל-WAV...")
    try:
        result = subprocess.run(
            [FFMPEG_EXECUTABLE, "-loglevel", "error", "-y", "-i", mp3_file, "-ar", "8000", "-ac", "1", "-acodec", "pcm_s16le", wav_file],
            check=True
        )
        print(f"✅ קובץ שמע נוצר בהצלחה: {wav_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ שגיאה בהמרה (FFmpeg): {e}.")
    except FileNotFoundError:
        print(f"❌ שגיאה בהמרה (FFmpeg): ffmpeg לא נמצא.")
    except Exception as e:
        print(f"❌ שגיאה כללית בהמרה: {e}")
    return False

async def create_audio_file_from_text(text, filename):
    """יוצר קובץ אודיו (MP3 זמני) מטקסט באמצעות Edge TTS."""
    print("🎙️ יוצר תגובה קולית מהטקסט...")
    try:
        comm = edge_tts.Communicate(text, voice="he-IL-AvriNeural")
        await comm.save(filename)
        print(f"✅ קובץ טקסט-לקול זמני נוצר בהצלחה: {filename}")
        return True
    except Exception as e:
        print(f"❌ שגיאה ביצירת קובץ אודיו מטקסט: {e}")
        return False

# --- פונקציית העיבוד המרכזית ---
async def process_yemot_recording(audio_file_path):
    """מעבד את הקלטת האודיו ומגיב בהתאם."""
    print("--- מתחיל עיבוד הקלטה חדשה ---")
    stock_data = load_stock_data(CSV_FILE_PATH)
    if not stock_data:
        response_text = "לא ניתן להמשיך ללא נתוני מניות."
        action_type = "play_file"
        action_value = f"{OUTPUT_AUDIO_FILE_BASE}.wav"
    else:
        recognized_text = transcribe_audio(audio_file_path)
        response_text = ""
        action_type = "play_file"
        action_value = f"{OUTPUT_AUDIO_FILE_BASE}.wav"

        if recognized_text:
            print("🔍 מנסה למצוא התאמה בנתוני המניות...")
            best_match_key = get_best_match(recognized_text, stock_data)
            if best_match_key:
                print(f"🎯 נמצאה התאמה: {best_match_key}")
                stock_info = stock_data[best_match_key]
                if stock_info["has_dedicated_folder"] and stock_info["target_path"]:
                    response_text = f"מפנה לשלוחת {stock_info['display_name']}."
                    action_type = "go_to_folder"
                    action_value = stock_info["target_path"]
                    print(f"💡 זוהתה הפניה לשלוחה ייעודית: {stock_info['target_path']}")
                else:
                    data = get_stock_price_data(stock_info["symbol"])
                    if data:
                        direction = "עלייה" if data["day_change_percent"] > 0 else "ירידה"
                        response_text = (
                            f"מחיר מניית {stock_info['display_name']} עומד כעת על {data['current']} דולר. "
                            f"מתחילת היום נרשמה {direction} של {abs(data['day_change_percent'])} אחוז."
                        )
                        print(f"✅ נתונים עבור {stock_info['display_name']} נקלטו בהצלחה.")
                    else:
                        response_text = f"מצטערים, לא הצלחנו למצוא נתונים עבור מניית {stock_info['display_name']}."
                        print(f"❌ לא נמצאו נתונים עבור מניית {stock_info['display_name']}.")
            else:
                response_text = "לא הצלחנו לזהות את נייר הערך שביקשת. אנא נסה שנית."
                print(f"❌ לא זוהה נייר ערך תואם ברשימה עבור: '{recognized_text}'")
        else:
            response_text = "לא זוהה דיבור ברור בהקלטה. אנא נסה לדבר באופן ברור יותר."
            print("❌ לא זוהה דיבור ברור בהקלטה.")

    generated_audio_success = False
    uploaded_ext_ini = False
    output_yemot_wav_name = f"{OUTPUT_AUDIO_FILE_BASE}.wav"

    if response_text and action_type == "play_file":
        if await create_audio_file_from_text(response_text, TEMP_MP3_FILE):
            if convert_mp3_to_wav(TEMP_MP3_FILE, output_yemot_wav_name):
                if upload_file_to_yemot(output_yemot_wav_name, output_yemot_wav_name):
                    generated_audio_success = True
                else:
                    print("❌ נכשלה העלאת קובץ השמע.")
            else:
                print("❌ נכשלה המרת MP3 ל-WAV.")
        else:
            print("❌ נכשלה יצירת קובץ אודיו מטקסט.")
    elif action_type == "go_to_folder":
        generated_audio_success = True

    if generated_audio_success or action_type == "go_to_folder":
        if create_ext_ini_file(action_type, action_value):
            if upload_file_to_yemot(OUTPUT_INI_FILE_NAME, OUTPUT_INI_FILE_NAME):
                uploaded_ext_ini = True
            else:
                print(f"❌ נכשלה העלאת קובץ {OUTPUT_INI_FILE_NAME}.")
        else:
            print(f"❌ נכשלה יצירת קובץ {OUTPUT_INI_FILE_NAME}.")
    else:
        print("⚠️ לא נוצרה תגובה קולית או הפניה לשלוחה.")
        return jsonify({"success": False, "message": "Failed to create response"}) # החזרת תשובה שלילית לימות המשיח

    local_files_to_clean = [audio_file_path, TEMP_MP3_FILE, OUTPUT_INI_FILE_NAME]
    if output_yemot_wav_name and os.path.exists(output_yemot_wav_name) and action_type == "play_file":
        local_files_to_clean.append(output_yemot_wav_name)

    for f in local_files_to_clean:
        if os.path.exists(f):
            os.remove(f)
            print(f"🧹 נמחק קובץ זמני: {f}")

    print("--- סיום עיבוד בהצלחה והעלאת התוצאות לשלוחה ---")
    return jsonify({"success": True}) # החזרת תשובה חיובית לימות המשיח

# --- נקודת קצה של ה-API שתקבל קבצים ---
@app.route('/process_audio', methods=['POST'])
def process_audio_endpoint():
    print("--- 📥 התקבלה הקלטה חדשה דרך ה-API ---")
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file:
        file_path = TEMP_INPUT_WAV
        file.save(file_path)
        try:
            return asyncio.run(process_yemot_recording(file_path))
        except Exception as e:
            print(f"❌ שגיאה קריטית בעיבוד: {e}")
            return jsonify({"error": "Failed to process audio"}), 500

if __name__ == "__main__":
    ensure_ffmpeg()
    stock_data = load_stock_data(CSV_FILE_PATH)
    print("🚀 השרת הופעל בהצלחה. ממתין להקלטה...")
    app.run(host='0.0.0.0', port=5000)
