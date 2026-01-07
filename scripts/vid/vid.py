#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, requests, json, base64, urllib3, threading, urllib.parse
from bs4 import BeautifulSoup
from queue import Queue
from tqdm import tqdm
from cryptography.fernet import Fernet
from conf.config import ENCRYPTION_KEY

S = requests.Session()
cipher = Fernet(ENCRYPTION_KEY)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
script_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_path)
os.environ.setdefault("TERM", "xterm-256color")
MAX_WORKERS, PAGES_TO_CRAWL, BASE_SAVE_DIR = 5, [1], 'vid'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': cipher.decrypt(b'gAAAAABpWKZrfdOvBRIOuxfq1U2dNXUdwQlg-uGul5v4fhVScqt9z4-M_v9JUgEOj-mOKimck46vxasfgG-PHXt0WBso6dKpqz8YkkArRJIW-j_MtfZQcSk=').decode('utf-8')
}

ENCRYPTED_CATEGORIES = {
    '1': b'gAAAAABpWJ7icp9q-voZLcOcahh-6L6h6BccmxGcmGPXz7jU30cZuok2-3tF4jo27ERP3IMi_T5rXIcz2GRgy3iCcK3AY6R8s570_7uS-jv2x4O5VDtmZANRPVmGSDlPhXmomKD7ns89N-MB4QVpcgq7KbdgaL2ifu72obshvKpzQSeAQjFQV_wwLLuIKKITSe1J3tAsPbcW',
    '2': b'gAAAAABpWJ7i-J3AIgKgXurxcFXbuIt-0O9yiQKA2LIVcyenBaIopJjtRtDD61jZfcsykFxdMJpbwScegR1ZLDZ2FVmvFeZi3UcC0VopyiBqGqKUGgddWeF0OBCNz8-SmEuur6OW-lMXfhZ18OgDpRvrCcrsIB8nU5UqtvefCCG9Q2TAc7Od8-FRl1Ofu0A3xl0Lzgiebz-O',
    '3': b'gAAAAABpWJ7iTvIvK5mQ6rMlTRmrSN3bHfhgI0gCEYGS-osW1IUzVoEMBvTtbDqBZ2daZL0dAfeFTw7DSx45jdYpz2CiYvzibj1oFgLQi22QPCvLEKhcr--hBPOLP7Fe9X0egCeYgrQxZcS00erpkZKoAWr0pujhz-p4JBp310DoO7VSQMlgOYBsXPuHG9C2G84LB5HDkMQj',
    '4': b'gAAAAABpWJ7im2sfjGkLDyUbc5Bv5ZgAJESySA3stAE-9Zf3jKX5S8_Y0C-BNEqth7rtiJnhKACOljlYPQxFz9RqZh5dgariUztE56SAZ-o2lPbu74nWKlVwYI0C2phDgc9KhVldF4KHJf_sBumCWjXueh1oUAxa8PVK34UwrLJ7vkACCYY8p7rsbK93P0q_12KzINmpFU8s',
    '5': b'gAAAAABpWJ7iPMBzWMz17KeAUJ7uXgWhlBGSCGxsWNZyo52W-rNwWOnrbwvpwBp6U0Z-0jkkiZ2f1SvX1USR-KqfDybL23c9Qpe4dp-XY6bSizv5Vb1tCjV-aPbEzLcrYbYiHAIgb4-otAN-LmHYmz8wim6xHWhVv4xJK1L5UNCs0MpPlGxUBXch39h-_puwl0uknqDjr4F3',
    '6': b'gAAAAABpWJ7iGY0igPek6v-RZ5MQzx02yLxF_MDQOqieKpfhhbKgr1S8myb5EDmQoRex8VCN6SINTF7xCFV4Q-Pqza3rLya9R7XpghL9PwNl5ghyKMzcUXeG-UlFC-xqOX--koh49M_v_q-SsVx8CnwPbzTtlq3W29CHHrTV1Aa8F4JOZJItpsZankW1lI2Q_uNg56z_2kZ8',
}

task_queue, downloaded_ids = Queue(), set()
active_downloads, download_lock, total_found, total_done = ["Waiting"] * MAX_WORKERS, threading.Lock(), 0, 0

def safe_filename(name):
    try:
        return re.sub(r'[\\/:*?"<>|\s]+', '_', name.strip())
    except Exception as e:
        print(f"Regex match failed: {e}")
        return f"unknown_{int(time.time())}"

def get_real_info(cid):
    try:
        d = cipher.decrypt(ENCRYPTED_CATEGORIES[cid]).decode()
        n, u_b = json.loads(d)
        return n, base64.b64decode(u_b).decode('utf-8')
    except Exception as e:
        print(f"Decrypt failed: {cid}, reason: {e}")
        return None, None

def get_real_mp4(html):
    try:
        m = re.search(r'strencode\d*\s*\(\s*["\'](.+?)["\']\s*\)', html, re.DOTALL)
        if m:
            content = urllib.parse.unquote(m.group(1))
            mp4_m = re.search(r'src=[\'"]([^\'"]+?\.mp4[^\'"]*?)[\'"]', content)
            if mp4_m:
                url = mp4_m.group(1)
                return 'https:' + url if url.startswith('//') else url
        direct = re.search(r'https?://[^\s\'"<>]+?\.mp4[^\s\'"<>]*', html)
        if direct: return direct.group(0)
    except Exception as e:
        print(f"MP4 parse failed: {e}")
    return None

def safe_url(url):
    try:
        parts = urllib.parse.urlsplit(url)
        safe_q = urllib.parse.quote(parts.query, safe="=&")
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, safe_q, parts.fragment))
    except Exception as e:
        print(f"URL encode failed: {url}, reason: {e}")
        return url

def load_local_history():
    try:
        os.makedirs(BASE_SAVE_DIR, exist_ok=True)
        for r, _, fs in os.walk(BASE_SAVE_DIR):
            for f in fs:
                if f.endswith('.mp4'):
                    fid = "".join(filter(lambda x: x.isdigit(), f))
                    if fid: downloaded_ids.add(fid)
    except Exception as e:
        print(f"Load history failed: {e}")

def producer(targets):
    global total_found
    try:
        for f_n, b_u in targets:
            f_n_safe = safe_filename(f_n)
            for p in PAGES_TO_CRAWL:
                try:
                    r = S.get(f"{b_u}&page={p}", headers=HEADERS, timeout=10, verify=False)
                    soup = BeautifulSoup(r.text, 'lxml')
                    items = soup.find_all('div', class_='well well-sm videos-text-align')
                    if not items: break
                    for item in items:
                        try:
                            a = item.find('a', href=lambda x: x and 'viewkey=' in x)
                            if not a: continue
                            vid, img = "", item.find('img')
                            if img and 'data-src' in img.attrs:
                                vm = re.search(r'/(\d+)/', img['data-src'])
                                vid = vm.group(1) if vm else ""
                            if vid:
                                with download_lock:
                                    if vid in downloaded_ids: continue
                                    downloaded_ids.add(vid)

                            task_queue.put((a['href'], f_n_safe, vid))
                            total_found += 1
                        except Exception as e:
                            print(f"Video item failed: {e}")
                except Exception as e:
                    print(f"List page failed: {b_u}&page={p}, reason: {e}")
    except Exception as e:
        print(f"Producer exception: {e}")
    finally:
        for _ in range(MAX_WORKERS): task_queue.put(None)

def consumer(idx):
    global total_done
    while True:
        task = task_queue.get()
        if task is None: break
        url, f_n, v_id = task
        try:
            dr = S.get(url, headers=HEADERS, timeout=15, verify=False)
            mp4 = get_real_mp4(dr.text)
            if mp4:
                rm = re.search(r'/(\d+)\.mp4', mp4)
                fn = f"{rm.group(1)}.mp4" if rm else f"{v_id}.mp4"
                fn = safe_filename(fn)
                spath = os.path.join(BASE_SAVE_DIR, f_n)
                os.makedirs(spath, exist_ok=True)
                fpath = os.path.join(spath, fn)
                vh = HEADERS.copy()
                vh['Referer'] = url
                mp4_safe = safe_url(mp4)
                with S.get(mp4_safe, headers=vh, stream=True, timeout=(5, 60), verify=False) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0
                    with open(fpath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total_size > 0:
                                    pct = int(downloaded / total_size * 100)
                                    status_str = f"D:{fn[-8:]}({pct}%)"
                                else:
                                    status_str = f"D:{fn[-8:]}(DL...)"
                                with download_lock:
                                    active_downloads[idx] = status_str
        except Exception as e:
            print(f"Thread-{idx} Error: {v_id} -> {e}")
            with download_lock:
                if v_id in downloaded_ids: downloaded_ids.remove(v_id)
        finally:
            total_done += 1
            with download_lock:
                active_downloads[idx] = "Idle"
            task_queue.task_done()


def monitor_ui():
    try:
        pbar = tqdm(total=100, unit="vids", dynamic_ncols=True, colour='green')
        while True:
            curr_found = max(total_found, 1)
            pbar.total = curr_found
            pbar.n = total_done
            with download_lock:
                status = " | ".join([f"T{i + 1}:{n}" for i, n in enumerate(active_downloads)])
            pbar.set_description(f"📥 [{status}]")
            pbar.refresh()
            if total_done >= total_found > 0 and task_queue.empty():
                pbar.n = total_found
                pbar.refresh()
                break
            time.sleep(0.3)
        pbar.close()
    except Exception as e:
        print(f"UI Error: {e}")

def start_scrape():
    try:
        os.environ.setdefault("TERM", "xterm-256color")
        os.system('cls' if os.name == 'nt' else 'clear')
        load_local_history()
        available_cats = {}
        for k in sorted(ENCRYPTED_CATEGORIES.keys(), key=int):
            n, u = get_real_info(k)
            if n:
                available_cats[k] = (n, u)
                print(f"  [{k}] {n}")
        print("  [A] All categories")
        choice = input("Select categories: ").strip().upper()
        if choice == 'A':
            ts = list(available_cats.values())
        else:
            ts = []
            for c in choice.split(','):
                c = c.strip()
                if c in available_cats:
                    ts.append(available_cats[c])
            if not ts:
                print("Invalid selection, exiting program")
                return
        threading.Thread(target=producer, args=(ts,), daemon=True).start()
        threading.Thread(target=monitor_ui, daemon=True).start()
        threads = [threading.Thread(target=consumer, args=(i,), daemon=True) for i in range(MAX_WORKERS)]
        for t in threads: t.start()
        for t in threads: t.join()
    except Exception as e:
        print(f"Failed to start: {e}")

if __name__ == "__main__":
    start_scrape()