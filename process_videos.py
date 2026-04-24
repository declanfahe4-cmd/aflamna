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
        if not os.path.exists(file_path):
            print(f"خطأ: الملف {file_path} غير موجود!")
            return []
        with open(file_path, "r", encoding="utf-8") as f:
            post_links = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return post_links
    except Exception as e:
        print(f"خطأ أثناء قراءة {file_path}: {e}")
        return []

def load_existing_data():
    """تحميل البيانات الموجودة مسبقاً"""
    json_data = {}
    csv_data = []
    
    if os.path.exists("data.json") and os.path.getsize("data.json") > 0:
        try:
            with open("data.json", "r", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception as e:
            print(f"خطأ في تحميل data.json: {e}")
    
    if os.path.exists("results.csv"):
        try:
            df = pd.read_csv("results.csv", encoding="utf-8")
            csv_data = df.to_dict('records')
        except Exception as e:
            print(f"خطأ في تحميل results.csv: {e}")
    
    return json_data, csv_data

def generate_archive_identifier(title, episode_id):
    """توليد اسم معرف للأرشيف متوافق مع شروط IA (أحرف إنجليزية وأرقام فقط)"""
    clean_id = re.sub(r'[^a-zA-Z0-9_.-]', '_', episode_id)
    if not clean_id or not clean_id[0].isalnum():
        clean_id = "v_" + clean_id
    while len(clean_id) < 5:
        clean_id += "_vid"
    return clean_id[:100]

def get_embed_url(post_url):
    """استخراج رابط المشغل من صفحة الحلقة"""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(post_url, headers=headers, timeout=20)
        resp.raise_for_status()
        
        cdn_links = re.findall(r"https://cdnplus\.cyou/embed-[\w\d]+\.html", resp.text)
        if cdn_links: return cdn_links[0]
            
        embed_ids = re.findall(r'embed-([a-zA-Z0-9]+)', resp.text)
        if embed_ids: return f"https://cdnplus.cyou/embed-{embed_ids[0]}.html"
        return None
    except Exception as e:
        print(f"خطأ في استخراج رابط المشغل: {e}")
        return None

def get_m3u8_url_with_playwright(embed_url):
    """استخراج رابط M3U8 باستخدام Playwright"""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            m3u8_url = None
            
            def handle_response(response):
                nonlocal m3u8_url
                if ".m3u8" in response.url and "master" in response.url:
                    m3u8_url = response.url
            
            page.on("response", handle_response)
            page.goto(embed_url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(10000)
            browser.close()
            return m3u8_url
    except Exception as e:
        print(f"خطأ Playwright: {e}")
        return None

def download_m3u8_video(m3u8_url, output_prefix):
    """تحميل الفيديو بدقات مختلفة (مع إجبار إعادة الترميز لتغيير الحجم)"""
    downloaded_files = []
    # سنقوم بتجربة الدقات من الأقل للأعلى لتوفير الموارد
    formats = [
        {'height': 360, 'name': f'{output_prefix}_360p.mp4'},
        {'height': 480, 'name': f'{output_prefix}_480p.mp4'},
        {'height': 720, 'name': f'{output_prefix}_720p.mp4'}
    ]
    
    for fmt in formats:
        try:
            print(f"   [~] جاري معالجة دقة {fmt['height']}p...")
            # نستخدم إعادة الترميز (Encoding) بدلاً من النسخ (Copy) لضمان تغيير الحجم
            cmd = [
                'ffmpeg', '-y', '-i', m3u8_url,
                '-vf', f'scale=-2:{fmt["height"]}', # تغيير الحجم مع الحفاظ على التناسب
                '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '28', # ضغط سريع وجودة معقولة
                '-c:a', 'aac', '-b:a', '128k',
                '-movflags', '+faststart',
                '-timeout', '60000000',
                fmt['name']
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            
            if result.returncode == 0 and os.path.exists(fmt['name']) and os.path.getsize(fmt['name']) > 100000:
                downloaded_files.append(fmt['name'])
                print(f"   [+] تم إنتاج: {fmt['name']} (الحجم: {os.path.getsize(fmt['name'])/(1024*1024):.2f} MB)")
            else:
                print(f"   [!] فشل إنتاج دقة {fmt['height']}p")
        except Exception as e:
            print(f"   [!] خطأ أثناء معالجة {fmt['height']}p: {e}")

    return downloaded_files

def upload_to_archive(identifier, files, access_key, secret_key):
    """رفع الملفات إلى Internet Archive"""
    try:
        from internetarchive import upload
        valid_files = [f for f in files if os.path.exists(f) and os.path.getsize(f) > 100000]
        if not valid_files: return None
            
        upload(
            identifier,
            files=valid_files,
            access_key=access_key,
            secret_key=secret_key,
            metadata={
                'mediatype': 'movies',
                'collection': 'opensource_movies',
                'title': identifier,
                'creator': 'Auto Video Bot'
            }
        )
        return f"https://archive.org/details/{identifier}"
    except Exception as e:
        print(f"[!] خطأ في الرفع: {e}")
        return None

def process_single_video(post_url, access_key, secret_key, existing_json_data):
    """معالجة فيديو واحد بالكامل"""
    try:
        episode_id = post_url.split('/')[-1]
        if episode_id in existing_json_data:
            print(f"   [~] {episode_id} موجود مسبقاً، تخطي...")
            return None, None

        resp = requests.get(post_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("title").text.strip() if soup.find("title") else episode_id
        
        embed_url = get_embed_url(post_url)
        if not embed_url: return None, None
        
        m3u8 = get_m3u8_url_with_playwright(embed_url)
        if not m3u8: return None, None
        
        archive_id = generate_archive_identifier(title, episode_id)
        downloaded = download_m3u8_video(m3u8, f"temp_{episode_id}")
        
        if not downloaded: return None, None
        
        archive_url = upload_to_archive(archive_id, downloaded, access_key, secret_key)
        if not archive_url: return None, None
        
        # بناء بيانات JSON
        sources = []
        for file in downloaded:
            label = "360p SD" if "360p" in file else "480p SD" if "480p" in file else "720p HD"
            sources.append({
                "file": f"https://archive.org/download/{archive_id}/{os.path.basename(file)}",
                "label": label
            })
            
        # حذف الملفات المحلية لتوفير المساحة
        for f in downloaded: os.remove(f)
            
        return episode_id, {
            "json": {"title": title, "sources": sources},
            "csv": {"ID": episode_id, "Title": title, "Archive_URL": archive_url, "Post": post_url}
        }
    except Exception as e:
        print(f"خطأ في {post_url}: {e}")
        return None, None

def main(links_file):
    links = load_links(links_file)
    if not links: return
    
    json_data, csv_data = load_existing_data()
    access_key = os.environ.get('IA_ACCESS_KEY')
    secret_key = os.environ.get('IA_SECRET_KEY')
    
    if not access_key or not secret_key:
        print("خطأ: مفاتيح الأرشيف مفقودة!")
        return

    new_added = False
    for url in links:
        eid, data = process_single_video(url, access_key, secret_key, json_data)
        if eid and data:
            json_data[eid] = data["json"]
            csv_data.append(data["csv"])
            new_added = True
            # حفظ فوري بعد كل فيديو لضمان عدم ضياع البيانات
            with open("data.json", "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            pd.DataFrame(csv_data).to_csv("results.csv", index=False, encoding="utf-8-sig")
            print(f"[+] تم حفظ بيانات: {eid}")

    print("\n--- انتهى التنفيذ بنجاح ---")

if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else "links.txt")
