"""
Sync an Immich library to Amazon Photos (one-way, re-runnable).

- Uploads every Immich asset (timeline + archive) once, into an "Immich" folder.
- Skips files whose MD5 already exists in Amazon Photos and reuses that node.
- Recreates every Immich album as an Amazon Photos album with the same name.
- Keeps state in $STATE_DIR/state.json, so later runs only send what is new.

Env: AP_SESSION_ID, AP_UBID, AP_AT (amazon cookies), AP_TLD (default "es"),
     IMMICH_URL, IMMICH_API_KEY, STATE_DIR (default /state), WORKERS (default 4),
     DRY_RUN=1 to only report what would happen, TYPES=IMAGE to skip videos.
Immich's upload location must be mounted at /data (same paths Immich reports).
"""
import hashlib
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
from amazon_photos import AmazonPhotos

STATE_DIR = Path(os.environ.get('STATE_DIR', '/state'))
STATE_FILE = STATE_DIR / 'state.json'
WORKERS = int(os.environ.get('WORKERS', 4))
DRY_RUN = os.environ.get('DRY_RUN') == '1'
TYPES = os.environ.get('TYPES', 'IMAGE,VIDEO').split(',')  # e.g. TYPES=IMAGE to skip videos
FOLDER_NAME = 'Immich'
BATCH = 50
V2 = {'resourceVersion': 'V2', 'ContentType': 'JSON'}

lock = threading.Lock()
state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {'assets': {}, 'albums': {}}
state.setdefault('assets', {})
state.setdefault('albums', {})


def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)


def save():
    with lock:
        tmp = STATE_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(state))
        tmp.replace(STATE_FILE)


class AuthError(Exception):
    pass


def request(client, method, url, retries=8, **kw):
    for i in range(retries + 1):
        try:
            r = client.request(method, url, **kw)
            if r.status_code == 401:
                raise AuthError('Amazon cookies expired or invalid')
            if r.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError(f'{r.status_code}', request=r.request, response=r)
            return r
        except AuthError:
            raise
        except (httpx.HTTPError, OSError) as e:
            if i == retries:
                raise
            t = min(2 ** i + random.random(), 120)
            log(f'  retry {i + 1} in {t:.0f}s: {e}')
            time.sleep(t)
            if 'content' in kw and hasattr(kw['content'], 'seek'):
                kw['content'].seek(0)


# ---------- Immich ----------
im = httpx.Client(base_url=os.environ['IMMICH_URL'], headers={'x-api-key': os.environ['IMMICH_API_KEY']}, timeout=120)


def immich_assets(**filters):
    out = []
    for vis in ('timeline', 'archive'):
        page = 1
        while page:
            r = im.post('/api/search/metadata', json={'page': page, 'size': 1000, 'visibility': vis, 'withDeleted': False} | filters)
            r.raise_for_status()
            d = r.json()['assets']
            out += d['items']
            page = int(d['nextPage']) if d.get('nextPage') else None
    return out


# ---------- Amazon ----------
ap = AmazonPhotos(
    cookies={'session-id': os.environ['AP_SESSION_ID'], f'ubid-acb{os.environ.get("AP_TLD", "es")}': os.environ['AP_UBID'],
             f'at-acb{os.environ.get("AP_TLD", "es")}': os.environ['AP_AT']},
    tld=os.environ.get('AP_TLD', 'es'), init_db=False, init_folders=False,
    db_path='/tmp/ap.parquet', tmp='/tmp/ap',
)
amz = ap.client
DRIVE = ap.drive_url


def amazon_nodes(filters):
    out, start = [], None
    while True:
        params = V2 | {'filters': filters, 'limit': 200}
        if start:
            params['startToken'] = start
        d = request(amz, 'GET', f'{DRIVE}/nodes', params=params).json()
        out += d.get('data', [])
        start = d.get('nextToken')
        if not start:
            return out


def get_folder():
    root = ap.root['id']
    for n in amazon_nodes(f'kind:FOLDER AND parents:{root}'):
        if n.get('name') == FOLDER_NAME:
            return n['id']
    if DRY_RUN:
        return None
    r = request(amz, 'POST', f'{DRIVE}/nodes', params=V2, json={'kind': 'FOLDER', 'name': FOLDER_NAME, 'parents': [root]})
    r.raise_for_status()
    return r.json()['id']


def file_md5(p):
    h = hashlib.md5()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def upload_one(a, folder_id, amz_md5):
    p = Path(a['originalPath'])
    if not p.exists():  # storage template may have moved it since listing
        a = im.get(f"/api/assets/{a['id']}").json()
        p = Path(a['originalPath'])
    m = file_md5(p)
    if m in amz_md5:
        return amz_md5[m], 'existing'
    if DRY_RUN:
        return None, 'would-upload'
    name = a['originalFileName']
    for attempt in range(2):
        with open(p, 'rb') as f:
            r = request(amz, 'POST', ap.cdproxy_url, content=f, timeout=httpx.Timeout(900),
                        params={'name': name, 'kind': 'FILE', 'parentNodeId': folder_id})
        if r.status_code in (200, 201):
            return r.json()['id'], 'uploaded'
        if r.status_code == 409:
            info = r.json().get('info') or {}
            if r.json().get('code') == 'MD5_DUPLICATE' and info.get('nodeIds'):
                return info['nodeIds'][0], 'existing'
            nid = info.get('nodeId')
            if nid:
                node = request(amz, 'GET', f'{DRIVE}/nodes/{nid}', params=V2).json()
                if (node.get('contentProperties') or {}).get('md5') == m:
                    return nid, 'existing'
            name = f"{p.stem if attempt else Path(name).stem}_{a['id'][:8]}{Path(name).suffix}"
            continue
        raise RuntimeError(f'{r.status_code} {r.text[:200]}')
    raise RuntimeError(f'409 conflict persists for {name}')


def main():
    log('Listing Immich assets…')
    assets = [a for a in immich_assets() if a['type'] in TYPES]
    log(f'Immich: {len(assets)} assets, {len(state["assets"])} already synced')

    log('Listing Amazon Photos media (for MD5 dedup)…')
    df = ap.query('type:(PHOTOS OR VIDEOS)')
    amz_md5 = {}
    if df is not None and not df.empty and 'md5' in df:
        amz_md5 = dict(zip(df['md5'], df['id']))
    log(f'Amazon: {len(amz_md5)} media already there')

    folder_id = get_folder()
    todo = [a for a in assets if a['id'] not in state['assets']]
    log(f'To process: {len(todo)}' + (' (DRY RUN)' if DRY_RUN else ''))

    stats, errors, done = {}, [], 0
    with ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(upload_one, a, folder_id, amz_md5): a for a in todo}
        for fut in as_completed(futs):
            a = futs[fut]
            try:
                nid, how = fut.result()
                stats[how] = stats.get(how, 0) + 1
                if nid:
                    with lock:
                        state['assets'][a['id']] = nid
            except AuthError as e:
                log(f'FATAL: {e}')
                ex.shutdown(cancel_futures=True)
                save()
                sys.exit(2)
            except Exception as e:
                errors.append(a['id'])
                log(f'  ERROR {a.get("originalFileName")}: {e}')
            done += 1
            if done % 100 == 0 or done == len(todo):
                save()
                log(f'{done}/{len(todo)} {stats} errors={len(errors)}')
    save()

    log('Albums…')
    existing = {n['name']: n['id'] for n in amazon_nodes('kind:VISUAL_COLLECTION')}
    for al in im.get('/api/albums').json():
        album_assets = immich_assets(albumIds=[al['id']])
        name = al['albumName']
        node_ids = [state['assets'][x['id']] for x in album_assets if x['id'] in state['assets'] and x['type'] in TYPES]
        st = state['albums'].setdefault(al['id'], {'amazon_id': None, 'added': []})
        st['amazon_id'] = st['amazon_id'] or existing.get(name)
        missing = [n for n in dict.fromkeys(node_ids) if n not in set(st['added'])]
        if DRY_RUN:
            log(f'  {name}: {len(album_assets)} assets, would add {len(missing)}')
            continue
        if not st['amazon_id']:
            r = request(amz, 'POST', f'{DRIVE}/nodes', params=V2, json={'kind': 'VISUAL_COLLECTION', 'name': name} | V2)
            r.raise_for_status()
            st['amazon_id'] = r.json()['id']
        for i in range(0, len(missing), BATCH):
            chunk = missing[i:i + BATCH]
            r = request(amz, 'PATCH', f"{DRIVE}/nodes/{st['amazon_id']}/children",
                        json={'op': 'add', 'value': chunk} | V2)
            r.raise_for_status()
            st['added'] += chunk
        save()
        log(f'  {name}: {len(st["added"])}/{len(album_assets)} in Amazon album')

    log(f'DONE. assets={stats} errors={len(errors)}')
    if errors:
        (STATE_DIR / 'errors.json').write_text(json.dumps(errors))


if __name__ == '__main__':
    main()
