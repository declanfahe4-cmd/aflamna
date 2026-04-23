# File: process_videos.py
import asyncio
import requests
import re
import pandas as pd
import json
import subprocess
import os
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import time

def load_links(file_path):
    """تحميل الروابط من ملف النص"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            post_links = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return post_links
    except FileNotFoundError:
        print(f"خطأ: الملف {file_path} غير موجود!")
        return []
    except Exception as e:
        print(f"خطأ أثناء قراءة {file_path}: {e}")
        return []

def load_existing_data():
    """تحميل البيانات الموجودة مسبقاً"""
    json_data = {}
    csv_data = []
    
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            json_data = json.load(f)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"خطأ في تحميل data.json: {e}")
    
    try:
        df = pd.read_csv("results.csv", encoding="utf-8")
        csv_data = df.to_dict('records')
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"خطأ في تحميل results.csv: {e}")
    
    return json_data, csv_data

def generate_archive_identifier(title, episode_id):
    """توليد اسم معرف للأرشيف من أول حرف من كل كلمة + رقم الحلقة"""
    words = re.findall(r'[a-zA-Z\u0600-\u06FF]+', title)
    if not words:
        return f"video_{episode_id}"
    
    acronym = ''.join([word[0] for word in words if word]).upper()
    return f"{acronym}_{episode_id}"

def get_embed_url(post_url):
    """استخراج رابط المشغل من صفحة الحلقة"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        print(f"   [~] جاري طلب الصفحة: {post_url}")
        resp = requests.get(post_url, headers=headers, timeout=20)
        resp.raise_for_status()
        
        # البحث عن روابط CDN مباشرة
        cdn_links = re.findall(r"https://cdnplus\.cyou/embed-[\w\d]+\.html", resp.text)
        if cdn_links:
            print(f"   [+] تم العثور على روابط CDN: {cdn_links[0]}")
            return cdn_links[0]
            
        # البحث عن معرفات مشغلة
        embed_ids = re.findall(r'embed-([a-zA-Z0-9]+)', resp.text)
        if embed_ids:
            embed_id = embed_ids[0]
            full_url = f"https://cdnplus.cyou/embed-{embed_id}.html"
            print(f"   [+] تم إنشاء رابط مشغل: {full_url}")
            return full_url
                
        return None
    except Exception as e:
        print(f"خطأ في استخراج رابط المشغل: {e}")
        return None

def get_m3u8_url_with_playwright(embed_url):
    """استخراج رابط M3U8 باستخدام Playwright لمراقبة الطلبات الشبكية"""
    try:
        print(f"   [~] جاري فتح صفحة المشغل مع مراقبة الطلبات: {embed_url}")
        
        with sync_playwright() as p:
            # زيادة مهلة البدء
            browser = p.chromium.launch(headless=True, timeout=60000)
            page = browser.new_page()
            
            # متغير لتخزين رابط M3U8
            m3u8_url = None
            
            # دالة لمعالجة الطلبات الشبكية
            def handle_response(response):
                nonlocal m3u8_url
                if ".m3u8" in response.url and "master" in response.url:
                    m3u8_url = response.url
                    print(f"   [+] تم التقاط رابط M3U8: {m3u8_url}")
            
            # ربط دالة معالجة الطلبات
            page.on("response", handle_response)
            
            # فتح الصفحة مع مهلة أطول
            page.goto(embed_url, wait_until="networkidle", timeout=60000)
            
            # انتظار إضافي للتأكد من تحميل جميع الطلبات
            page.wait_for_timeout(15000)
            
            browser.close()
            
            if m3u8_url:
                return m3u8_url
            else:
                print("   [!] لم يتم العثور على رابط M3U8 في الطلبات الشبكية")
                return None
                
    except Exception as e:
        print(f"خطأ في استخراج رابط M3U8 باستخدام Playwright: {e}")
        return None

def download_m3u8_video(m3u8_url, output_prefix):
    """تحميل الفيديو من رابط M3U8 بصيغ متعددة بدون اقتصاص"""
    downloaded_files = []
    formats = [
        {'height': 480, 'name': f'{output_prefix}_480p.mp4'},   # 480p أولاً لأنه أقل استهلاكاً للموارد
        {'height': 720, 'name': f'{output_prefix}_720p.mp4'},
        {'height': 360, 'name': f'{output_prefix}_360p.mp4'},
        {'height': 1080, 'name': f'{output_prefix}_1080p.mp4'}
    ]
    
    print(f"   [~] جاري اختبار صحة رابط M3U8...")
    try:
        # اختبار الرابط أولاً
        cmd_test = ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', m3u8_url]
        result_test = subprocess.run(cmd_test, capture_output=True, text=True, timeout=30)
        if result_test.returncode == 0 and result_test.stdout.strip():
            print(f"   [+] رابط M3U8 صالح، المدة: {result_test.stdout.strip()} ثانية")
        else:
            print("   [!] قد يكون الرابط غير صالح")
    except:
        print("   [~] لم يتم اختبار صحة الرابط")
    
    successful_downloads = 0
    
    for fmt in formats:
        try:
            print(f"جاري تنزيل: {fmt['name']}")
            
            # محاولة بسيطة أولاً
            cmd_simple = [
                'ffmpeg', '-y', '-i', m3u8_url,
                '-c', 'copy',  # نسخ بدون ترميز لتوفير الوقت
                '-movflags', '+faststart',
                '-timeout', '30',
                fmt['name']
            ]
            
            result_simple = subprocess.run(cmd_simple, capture_output=True, text=True, timeout=1800)
            
            if result_simple.returncode == 0 and os.path.exists(fmt['name']) and os.path.getsize(fmt['name']) > 10000:
                downloaded_files.append(fmt['name'])
                successful_downloads += 1
                size_mb = os.path.getsize(fmt['name']) / (1024 * 1024)
                print(f"[+] تم تنزيل (نسخ مباشر): {fmt['name']} ({size_mb:.2f} MB)")
            else:
                # إذا فشل النسخ المباشر، نحاول الترميز
                print(f"   [~] محاولة الترميز لـ: {fmt['name']}")
                cmd_encode = [
                    'ffmpeg', '-y', 
                    '-i', m3u8_url,
                    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '28',
                    '-c:a', 'aac', '-b:a', '128k',
                    '-vf', f'scale=-1:{fmt["height"]}:flags=lanczos',
                    '-movflags', '+faststart',
                    '-reconnect', '1', '-reconnect_at_eof', '1',
                    '-reconnect_streamed', '1', '-reconnect_delay_max', '30',
                    '-rw_timeout', '60000000',
                    '-analyzeduration', '2147483647',
                    '-probesize', '2147483647',
                    fmt['name']
                ]
                
                result_encode = subprocess.run(cmd_encode, capture_output=True, text=True, timeout=3600)
                
                if result_encode.returncode == 0 and os.path.exists(fmt['name']) and os.path.getsize(fmt['name']) > 10000:
                    downloaded_files.append(fmt['name'])
                    successful_downloads += 1
                    size_mb = os.path.getsize(fmt['name']) / (1024 * 1024)
                    print(f"[+] تم تنزيل (ترميز): {fmt['name']} ({size_mb:.2f} MB)")
                else:
                    error_msg = result_encode.stderr if result_encode.stderr else "No detailed error"
                    print(f"[-] فشل تنزيل: {fmt['name']} - Error: {error_msg[:500]}")
                    
        except subprocess.TimeoutExpired:
            print(f"[!] انتهت مهلة تنزيل: {fmt['name']}")
            # التحقق مما إذا كان الملف موجوداً وله حجم معقول
            if os.path.exists(fmt['name']) and os.path.getsize(fmt['name']) > 10000:
                downloaded_files.append(fmt['name'])
                successful_downloads += 1
                size_mb = os.path.getsize(fmt['name']) / (1024 * 1024)
                print(f"[~] تم تنزيل جزء من: {fmt['name']} ({size_mb:.2f} MB)")
        except Exception as e:
            print(f"[!] خطأ في تنزيل {fmt['name']}: {e}")
    
    if successful_downloads > 0:
        print(f"   [+] نجح تنزيل {successful_downloads} ملفات")
        return downloaded_files
    else:
        print("   [x] فشل تنزيل جميع الصيغ")
        return []

def upload_to_archive(identifier, files, access_key, secret_key):
    """رفع الملفات إلى Internet Archive"""
    try:
        from internetarchive import upload
        
        identifier = identifier.replace(' ', '_').replace('-', '_')
        
        print(f"جاري رفع الملفات إلى الأرشيف: {identifier}")
        print(f"الملفات المراد رفعها: {files}")
        
        # التحقق من وجود الملفات
        valid_files = [f for f in files if os.path.exists(f) and os.path.getsize(f) > 10000]
        if not valid_files:
            print("   [!] لا توجد ملفات صالحة للرفع")
            return None
            
        print(f"   [~] الملفات الصالحة للرفع: {valid_files}")
        
        r = upload(
            identifier,
            files=valid_files,
            access_key=access_key,
            secret_key=secret_key,
            verbose=True,
            metadata={
                'mediatype': 'movies',
                'collection': 'opensource_movies',
                'title': identifier,
                'creator': 'Auto Generated Video Archive'
            }
        )
        
        archive_url = f"https://archive.org/details/{identifier}"
        print(f"[+] تم رفع الملفات إلى: {archive_url}")
        return archive_url
    except Exception as e:
        print(f"[!] خطأ في الرفع: {e}")
        return None

def process_single_video(post_url, access_key, secret_key, existing_json_data):
    """معالجة فيديو واحد"""
    try:
        episode_id = post_url.split('/')[-1]
        print(f"\n[*] معالجة: {episode_id}")

        if episode_id in existing_json_data:
            print(f"   [~] الفيديو موجود مسبقاً: {episode_id}")
            return None, None

        print(f"   [>] جاري جلب معلومات الصفحة: {post_url}")
        resp = requests.get(post_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        title = soup.find("title").text.strip() if soup.find("title") else episode_id
        print(f"   [>] العنوان: {title}")

        embed_url = get_embed_url(post_url)
        if not embed_url:
            print("   [-] لا يوجد مشغل")
            return None, None

        print(f"   [>] جاري استخراج M3U8 من: {embed_url}")
        m3u8 = get_m3u8_url_with_playwright(embed_url)

        if not m3u8:
            print("   [x] ما لقيناش m3u8")
            return None, None

        print(f"   [+] تم العثور على رابط M3U8: {m3u8}")

        archive_identifier = generate_archive_identifier(title, episode_id)
        print(f"   [>] معرف الأرشيف: {archive_identifier}")

        print("   [>] جاري تنزيل الفيديو...")
        downloaded_files = download_m3u8_video(m3u8, f"temp_{episode_id}")
        
        if not downloaded_files:
            print("   [x] فشل تنزيل الفيديو")
            return None, None

        print("   [>] جاري رفع الفيديوهات إلى الأرشيف...")
        archive_url = upload_to_archive(archive_identifier, downloaded_files, access_key, secret_key)
        
        if not archive_url:
            print("   [x] فشل رفع الفيديوهات إلى الأرشيف")
            # حذف الملفات المؤقتة حتى لو فشل الرفع
            for file in downloaded_files:
                try:
                    os.remove(file)
                except:
                    pass
            return None, None

        json_entry = {
            "title": title,
            "sources": []
        }

        available_qualities = []
        for file in downloaded_files:
            if '1080p' in file:
                available_qualities.append({
                    "file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_1080p.mp4", 
                    "label": "1080p HD"
                })
            elif '720p' in file:
                available_qualities.append({
                    "file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_720p.mp4", 
                    "label": "720p HD"
                })
            elif '480p' in file:
                available_qualities.append({
                    "file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_480p.mp4", 
                    "label": "480p SD"
                })
            elif '360p' in file:
                available_qualities.append({
                    "file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_360p.mp4", 
                    "label": "360p SD"
                })

        json_entry["sources"] = available_qualities

        csv_entry = {
            "ID": episode_id,
            "Title": title,
            "Archive_URL": archive_url,
            "Post": post_url
        }

        # حذف الملفات المؤقتة بعد الرفع
        for file in downloaded_files:
            try:
                os.remove(file)
            except:
                pass

        print(f"   [+] تم الانتهاء من معالجة: {episode_id}")
        return episode_id, {"json": json_entry, "csv": csv_entry}

    except Exception as e:
        print(f"   [!] خطأ في معالجة {post_url}: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def main(links_file):
    post_links = load_links(links_file)
    if not post_links:
        print("لا توجد روابط للمعالجة!")
        return

    existing_json_data, existing_csv_data = load_existing_data()
    print(f"البيانات الموجودة: {len(existing_json_data)} فيديو")

    access_key = os.environ.get('IA_ACCESS_KEY')
    secret_key = os.environ.get('IA_SECRET_KEY')
    
    if not access_key or not secret_key:
        print("خطأ: مفاتيح الوصول إلى الأرشيف غير متوفرة!")
        return

    print("--- بدء عملية المعالجة ---")

    new_entries = {}
    new_csv_entries = []

    for post_url in post_links:
        episode_id, entry_data = process_single_video(post_url, access_key, secret_key, existing_json_data)
        
        if episode_id and entry_data:
            if episode_id not in existing_json_data:
                new_entries[episode_id] = entry_data["json"]
                new_csv_entries.append(entry_data["csv"])
                print(f"[+] تمت إضافة: {episode_id}")
            else:
                print(f"[~] تم تجاهل (موجود مسبقاً): {episode_id}")

    final_json_data = existing_json_data.copy()
    final_json_data.update(new_entries)

    final_csv_data = existing_csv_data + new_csv_entries

    if final_json_data:
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(final_json_data, f, ensure_ascii=False, indent=2)
        print(f"\n[تم] تحديث data.json - إجمالي الفيديوهات: {len(final_json_data)}")

    if final_csv_data:
        pd.DataFrame(final_csv_data).to_csv("results.csv", index=False, encoding="utf-8-sig")
        print(f"[تم] تحديث results.csv - إجمالي السجلات: {len(final_csv_data)}")

    print("\n--- انتهى التنفيذ ---")

if __name__ == "__main__":
    import sys
    links_file = sys.argv[1] if len(sys.argv) > 1 else "links.txt"
    try:
        main(links_file)
    except Exception as e:
        print(f"خطأ عام في التشغيل: {e}")
        import traceback
        traceback.print_exc()
        raise
