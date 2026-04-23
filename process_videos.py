# File: process_videos.py
import asyncio
import requests
import re
import pandas as pd
import json
import subprocess
import os
import glob
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import nest_asyncio
import urllib.parse

nest_asyncio.apply()

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
    
    # تحميل ملف JSON الحالي إن وجد
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            json_data = json.load(f)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"خطأ في تحميل data.json: {e}")
    
    # تحميل ملف CSV الحالي إن وجد
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
    # استخراج الكلمات العربية والإنجليزية
    words = re.findall(r'[a-zA-Z\u0600-\u06FF]+', title)
    if not words:
        # إذا لم توجد كلمات، نستخدم ID الحلقة
        return f"video_{episode_id}"
    
    # أخذ أول حرف من كل كلمة
    acronym = ''.join([word[0] for word in words if word]).upper()
    # إضافة رقم الحلقة أو ID
    return f"{acronym}_{episode_id}"

async def get_direct_video_url(browser, embed_url):
    """استخراج رابط M3U8 المباشر"""
    page = await browser.new_page()
    video_url = None

    async def handle_response(response):
        nonlocal video_url
        if ".m3u8" in response.url and "master" in response.url:
            video_url = response.url

    page.on("response", handle_response)

    try:
        await page.goto(embed_url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(8)
    except Exception as e:
        print(f"خطأ أثناء الذهاب للصفحة: {e}")
    finally:
        await page.close()

    return video_url

def download_m3u8_video(m3u8_url, output_prefix):
    """تحميل الفيديو من رابط M3U8 بصيغ متعددة"""
    downloaded_files = []
    
    # تنزيل بصيغ متعددة
    formats = [
        {'height': 1080, 'name': f'{output_prefix}_1080p.mp4'},
        {'height': 720, 'name': f'{output_prefix}_720p.mp4'},
        {'height': 480, 'name': f'{output_prefix}_480p.mp4'},
        {'height': 360, 'name': f'{output_prefix}_360p.mp4'}
    ]
    
    for fmt in formats:
        try:
            print(f"جاري تنزيل: {fmt['name']}")
            cmd = [
                'ffmpeg', '-y', '-i', m3u8_url,
                '-c:v', 'libx264', '-preset', 'fast',
                '-crf', '23', '-c:a', 'aac',
                '-vf', f'scale=-1:{fmt["height"]}',
                '-movflags', '+faststart',
                fmt['name']
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0 and os.path.exists(fmt['name']):
                downloaded_files.append(fmt['name'])
                print(f"[+] تم تنزيل: {fmt['name']}")
            else:
                print(f"[-] فشل تنزيل: {fmt['name']}")
                
        except subprocess.TimeoutExpired:
            print(f"[!] انتهت مهلة تنزيل: {fmt['name']}")
        except Exception as e:
            print(f"[!] خطأ في تنزيل {fmt['name']}: {e}")
    
    return downloaded_files

def trim_videos(input_files, seconds=10):
    """اقتطاع الثواني الأولى من الفيديوهات"""
    trimmed_files = []
    
    for file in input_files:
        output = file.replace('.mp4', '_trimmed.mp4')
        try:
            print(f"جاري اقتصاص: {file}")
            cmd = ['ffmpeg', '-y', '-ss', str(seconds), '-i', file, '-c', 'copy', output]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode == 0 and os.path.exists(output):
                trimmed_files.append(output)
                print(f"[+] تم اقتصاص: {output}")
                # حذف الملف الأصلي بعد الاقتصاص
                try:
                    os.remove(file)
                except:
                    pass
            else:
                print(f"[-] فشل اقتصاص: {output}")
        except subprocess.TimeoutExpired:
            print(f"[!] انتهت مهلة اقتصاص: {file}")
        except Exception as e:
            print(f"[!] خطأ في اقتصاص {file}: {e}")
    
    return trimmed_files

def upload_to_archive(identifier, files, access_key, secret_key):
    """رفع الملفات إلى Internet Archive"""
    try:
        from internetarchive import upload
        
        # التأكد من عدم وجود معرف مكرر
        identifier = identifier.replace(' ', '_').replace('-', '_')
        
        print(f"جاري رفع الملفات إلى الأرشيف: {identifier}")
        r = upload(
            identifier,
            files=files,
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

async def process_single_video(browser, post_url, access_key, secret_key, existing_json_data):
    """معالجة فيديو واحد"""
    try:
        episode_id = post_url.split('/')[-1]
        print(f"\n[*] معالجة: {episode_id}")

        # التحقق مما إذا كان الفيديو موجوداً مسبقاً
        if episode_id in existing_json_data:
            print(f"   [~] الفيديو موجود مسبقاً: {episode_id}")
            return None, None

        resp = requests.get(post_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        title = soup.find("title").text.strip() if soup.find("title") else episode_id
        print(f"   [>] العنوان: {title}")

        cdn_links = re.findall(r"https://cdnplus\.cyou/embed-[\w\d]+\.html", resp.text)
        if not cdn_links:
            print("   [-] لا يوجد مشغل")
            return None, None

        embed = cdn_links[0]
        print(f"   [>] جاري استخراج من: {embed}")

        m3u8 = await get_direct_video_url(browser, embed)

        if not m3u8:
            print("   [x] ما لقيناش m3u8")
            return None, None

        print(f"   [+] تم العثور على رابط M3U8: {m3u8}")

        # توليد اسم معرف للأرشيف
        archive_identifier = generate_archive_identifier(title, episode_id)
        print(f"   [>] معرف الأرشيف: {archive_identifier}")

        # تنزيل الفيديو
        print("   [>] جاري تنزيل الفيديو...")
        downloaded_files = download_m3u8_video(m3u8, f"temp_{episode_id}")
        
        if not downloaded_files:
            print("   [x] فشل تنزيل الفيديو")
            return None, None

        # اقتصاص الفيديوهات
        print("   [>] جاري اقتصاص الفيديوهات...")
        trimmed_files = trim_videos(downloaded_files, 10)
        
        if not trimmed_files:
            print("   [x] فشل اقتصاص الفيديوهات")
            return None, None

        # رفع إلى الأرشيف
        print("   [>] جاري رفع الفيديوهات إلى الأرشيف...")
        archive_url = upload_to_archive(archive_identifier, trimmed_files, access_key, secret_key)
        
        if not archive_url:
            print("   [x] فشل رفع الفيديوهات إلى الأرشيف")
            return None, None

        # إنشاء بيانات JSON للحلقة
        json_entry = {
            "title": title,
            "sources": []
        }

        # إضافة الروابط حسب الجودة المتاحة
        available_qualities = []
        for file in trimmed_files:
            if '1080p' in file:
                available_qualities.append({"file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_1080p_trimmed.mp4", "label": "1080p HD"})
            elif '720p' in file:
                available_qualities.append({"file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_720p_trimmed.mp4", "label": "720p HD"})
            elif '480p' in file:
                available_qualities.append({"file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_480p_trimmed.mp4", "label": "480p SD"})
            elif '360p' in file:
                available_qualities.append({"file": f"https://ia600509.us.archive.org/18/items/{archive_identifier}/temp_{episode_id}_360p_trimmed.mp4", "label": "360p SD"})

        json_entry["sources"] = available_qualities

        # إنشاء بيانات CSV
        csv_entry = {
            "ID": episode_id,
            "Title": title,
            "Archive_URL": archive_url,
            "Post": post_url
        }

        # حذف الملفات المؤقتة
        for file in trimmed_files + downloaded_files:
            try:
                os.remove(file)
            except:
                pass

        print(f"   [+] تم الانتهاء من معالجة: {episode_id}")
        return episode_id, {"json": json_entry, "csv": csv_entry}

    except Exception as e:
        print(f"   [!] خطأ في معالجة {post_url}: {e}")
        return None, None

async def main(links_file):
    # تحميل الروابط
    post_links = load_links(links_file)
    if not post_links:
        print("لا توجد روابط للمعالجة!")
        return

    # تحميل البيانات الموجودة
    existing_json_data, existing_csv_data = load_existing_data()
    print(f"البيانات الموجودة: {len(existing_json_data)} فيديو")

    # الحصول على مفاتيح الوصول
    access_key = os.environ.get('IA_ACCESS_KEY')
    secret_key = os.environ.get('IA_SECRET_KEY')
    
    if not access_key or not secret_key:
        print("خطأ: مفاتيح الوصول إلى الأرشيف غير متوفرة!")
        return

    print("--- بدء عملية المعالجة ---")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )

        new_entries = {}
        new_csv_entries = []

        for post_url in post_links:
            episode_id, entry_data = await process_single_video(browser, post_url, access_key, secret_key, existing_json_data)
            
            if episode_id and entry_data:
                # إضافة إلى البيانات الجديدة فقط إذا لم يكن موجوداً
                if episode_id not in existing_json_data:
                    new_entries[episode_id] = entry_data["json"]
                    new_csv_entries.append(entry_data["csv"])
                    print(f"[+] تمت إضافة: {episode_id}")
                else:
                    print(f"[~] تم تجاهل (موجود مسبقاً): {episode_id}")

        await browser.close()

        # دمج البيانات الجديدة مع الموجودة
        final_json_data = existing_json_data.copy()
        final_json_data.update(new_entries)

        # تحديث ملف CSV
        final_csv_data = existing_csv_data + new_csv_entries

        # حفظ ملف JSON
        if final_json_data:
            with open("data.json", "w", encoding="utf-8") as f:
                json.dump(final_json_data, f, ensure_ascii=False, indent=2)
            print(f"\n[تم] تحديث data.json - إجمالي الفيديوهات: {len(final_json_data)}")

        # حفظ ملف CSV
        if final_csv_data:
            pd.DataFrame(final_csv_data).to_csv("results.csv", index=False, encoding="utf-8-sig")
            print(f"[تم] تحديث results.csv - إجمالي السجلات: {len(final_csv_data)}")

        print("\n--- انتهى التنفيذ ---")

if __name__ == "__main__":
    import sys
    links_file = sys.argv[1] if len(sys.argv) > 1 else "links.txt"
    try:
        asyncio.run(main(links_file))
    except Exception as e:
        print(f"خطأ عام في التشغيل: {e}")
        raise
