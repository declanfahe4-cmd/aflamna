# File: process_videos.py
import asyncio
import requests
import re
import pandas as pd
import json
import subprocess
import os
from bs4 import BeautifulSoup
import urllib.parse
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
        resp = requests.get(post_url, headers=headers, timeout=15)
        resp.raise_for_status()
        
        print(f"   [~] تم استقبال {len(resp.text)} بايت من البيانات")
        
        # البحث عن روابط CDN مباشرة
        cdn_links = re.findall(r"https://cdnplus\.cyou/embed-[\w\d]+\.html", resp.text)
        if cdn_links:
            print(f"   [+] تم العثور على روابط CDN: {cdn_links}")
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

def get_m3u8_url(embed_url):
    """استخراج رابط M3U8 من صفحة المشغل بطرق متعددة"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": embed_url,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        
        print(f"   [~] جاري طلب صفحة المشغل: {embed_url}")
        resp = requests.get(embed_url, headers=headers, timeout=20)
        resp.raise_for_status()
        
        content = resp.text
        print(f"   [~] تم استقبال {len(content)} بايت من صفحة المشغل")
        
        # طباعة جزء من المحتوى للتصحيح (الأول 1000 حرف)
        preview_content = content[:1000]
        print(f"   [~] معاينة المحتوى: {preview_content}")
        
        # طرق متعددة لاستخراج M3U8
        patterns = [
            # أنماط مباشرة
            r'(https?://[^\s"\']*master[^\s"\']*\.m3u8[^\s"\']*)',
            r'(https?://[^\s"\']*\.m3u8[^\s"\']*\?[^"\']*)',
            r'(https?://[^\s"\']*\.m3u8[^\s"\']*)',
            
            # أنماط في سياق JavaScript
            r'"file"\s*:\s*"([^"]*?\.m3u8[^"]*)"',
            r"'file'\s*:\s*'([^']*?\.m3u8[^']*)'",
            r'sources\s*:\s*\[\s*{\s*"file"\s*:\s*"([^"]*?\.m3u8[^"]*)"',
            
            # أنماط عامة
            r'url\s*:\s*"([^"]*?\.m3u8[^"]*)"',
            r'"([^"]*?master\.m3u8[^"]*)"',
        ]
        
        # البحث عن جميع الأنماط
        for i, pattern in enumerate(patterns):
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                print(f"   [+] تم العثور على مطابقات بالنمط {i+1}: {matches[:3]}")  # عرض أول 3 مطابقات
                m3u8_url = matches[0]
                if not m3u8_url.startswith('http'):
                    from urllib.parse import urljoin
                    m3u8_url = urljoin(embed_url, m3u8_url)
                print(f"   [+] رابط M3U8 النهائي: {m3u8_url}")
                return m3u8_url
        
        # البحث عن أي رابط يحتوي على m3u8
        all_m3u8_urls = re.findall(r'https?://[^\s"\'>]+\.m3u8[^\s"\'>]*', content)
        if all_m3u8_urls:
            print(f"   [+] تم العثور على روابط M3U8 عامة: {all_m3u8_urls[:3]}")
            return all_m3u8_urls[0]
            
        # البحث في محتوى JavaScript المشفر
        if 'eval(' in content or 'function(' in content:
            print("   [~] تم اكتشاف كود JavaScript، البحث في المحتوى المشفر...")
            # البحث عن أي شيء يشبه رابط
            potential_urls = re.findall(r'[a-zA-Z0-9\-_]+\.m3u8', content)
            if potential_urls:
                print(f"   [~] روابط محتملة: {potential_urls[:5]}")
        
        print("   [!] لم يتم العثور على رابط M3U8 بعد تجربة جميع الأنماط")
        return None
    except Exception as e:
        print(f"خطأ في استخراج رابط M3U8: {e}")
        return None

def download_m3u8_video(m3u8_url, output_prefix):
    """تحميل الفيديو من رابط M3U8 بصيغ متعددة بدون اقتصاص"""
    downloaded_files = []
    formats = [
        {'height': 1080, 'name': f'{output_prefix}_1080p.mp4'},
        {'height': 720, 'name': f'{output_prefix}_720p.mp4'},
        {'height': 480, 'name': f'{output_prefix}_480p.mp4'},
        {'height': 360, 'name': f'{output_prefix}_360p.mp4'}
    ]
    
    # فحص جودة M3U8 أولاً
    try:
        print("   [~] فحص جودة M3U8...")
        cmd_check = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', m3u8_url]
        result_check = subprocess.run(cmd_check, capture_output=True, text=True, timeout=30)
        if result_check.returncode == 0:
            print("   [+] M3U8 URL يبدو صالحاً")
        else:
            print("   [!] M3U8 URL قد يكون غير صالح")
    except:
        pass
    
    for fmt in formats:
        try:
            print(f"جاري تنزيل: {fmt['name']}")
            cmd = [
                'ffmpeg', '-y', '-i', m3u8_url,
                '-c:v', 'libx264', '-preset', 'fast',
                '-crf', '23', '-c:a', 'aac',
                '-vf', f'scale=-1:{fmt["height"]}',
                '-movflags', '+faststart',
                '-timeout', '30',
                fmt['name']
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode == 0 and os.path.exists(fmt['name']):
                downloaded_files.append(fmt['name'])
                print(f"[+] تم تنزيل: {fmt['name']}")
            else:
                print(f"[-] فشل تنزيل: {fmt['name']} - {result.stderr[:200] if result.stderr else 'No error message'}")
                
        except subprocess.TimeoutExpired:
            print(f"[!] انتهت مهلة تنزيل: {fmt['name']}")
        except Exception as e:
            print(f"[!] خطأ في تنزيل {fmt['name']}: {e}")
    
    return downloaded_files

def upload_to_archive(identifier, files, access_key, secret_key):
    """رفع الملفات إلى Internet Archive"""
    try:
        from internetarchive import upload
        
        identifier = identifier.replace(' ', '_').replace('-', '_')
        
        print(f"جاري رفع الملفات إلى الأرشيف: {identifier}")
        print(f"الملفات المراد رفعها: {files}")
        
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

def process_single_video(post_url, access_key, secret_key, existing_json_data):
    """معالجة فيديو واحد"""
    try:
        episode_id = post_url.split('/')[-1]
        print(f"\n[*] معالجة: {episode_id}")

        if episode_id in existing_json_data:
            print(f"   [~] الفيديو موجود مسبقاً: {episode_id}")
            return None, None

        print(f"   [>] جاري جلب معلومات الصفحة: {post_url}")
        resp = requests.get(post_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        title = soup.find("title").text.strip() if soup.find("title") else episode_id
        print(f"   [>] العنوان: {title}")

        embed_url = get_embed_url(post_url)
        if not embed_url:
            print("   [-] لا يوجد مشغل")
            return None, None

        print(f"   [>] جاري استخراج من: {embed_url}")
        m3u8 = get_m3u8_url(embed_url)

        if not m3u8:
            print("   [x] ما لقيناش m3u8")
            print("   [~] محاولة إضافية لاستخراج M3U8...")
            time.sleep(5)
            m3u8 = get_m3u8_url(embed_url)
            
        if not m3u8:
            print("   [x] ما لقيناش m3u8 حتى بعد المحاولة الإضافية")
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
