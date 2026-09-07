#!/usr/bin/env python3
import json, os, time, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(BASE, 'chat_historique.json')
PORT = int(os.environ.get('PORT', 8080))
HOST = '0.0.0.0'
INDEX = ['Le prochain match de CF Montréal.html', 'index.html']
lock = threading.Lock()

def load():
    try:
        with open(HIST, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def save(m):
    try:
        with open(HIST, 'w', encoding='utf-8') as f:
            json.dump(m, f, ensure_ascii=False)
    except Exception:
        pass

messages = load()

class H(BaseHTTPRequestHandler):
    def _repondre(self, code, data, ctype='application/json; charset=utf-8'):
        body = data.encode('utf-8') if isinstance(data, str) else data
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == '/api/messages':
            q = parse_qs(u.query)
            try:
                since = int(q.get('since', ['0'])[0])
            except Exception:
                since = 0
            with lock:
                nouveaux = messages[since:]
                total = len(messages)
            self._repondre(200, json.dumps({'ok': True, 'total': total, 'msgs': nouveaux}, ensure_ascii=False))
        else:
            self._statique(u.path.lstrip('/'))

    def _statique(self, chemin):
        if '..' in chemin:
            self._repondre(403, 'Interdit', 'text/plain; charset=utf-8')
            return
        candidats = [chemin] if chemin else INDEX
        for c in candidats:
            fp = os.path.join(BASE, c)
            if os.path.isfile(fp):
                ctype = 'text/html; charset=utf-8' if fp.endswith('.html') else 'application/octet-stream'
                with open(fp, 'rb') as f:
                    self._repondre(200, f.read(), ctype)
                return
        self._repondre(404, 'Introuvable', 'text/plain; charset=utf-8')

    def do_POST(self):
        if urlparse(self.path).path == '/api/send':
            try:
                ln = int(self.headers.get('Content-Length', '0'))
                m = json.loads(self.rfile.read(ln).decode('utf-8'))
                entree = {
                    'n': str(m.get('n', 'Supporter'))[:20],
                    't': str(m.get('t', ''))[:300],
                    'i': str(m.get('i', ''))[:24],
                    'sv': time.time()
                }
                if not entree['t'].strip():
                    self._repondre(400, json.dumps({'ok': False}))
                    return
                with lock:
                    messages.append(entree)
                    if len(messages) > 500:
                        del messages[:-500]
                    save(messages)
                print('MSG ' + entree['n'] + ' : ' + entree['t'])
                self._repondre(200, json.dumps({'ok': True}))
            except Exception as e:
                self._repondre(400, json.dumps({'ok': False, 'err': str(e)}))
        else:
            self._repondre(404, '{}')

    def log_message(self, *a):
        pass

print('Serveur CFM demarre sur le port', PORT)
print('Test local : http://localhost:' + str(PORT))
print('Astuce : gardez Termux eveille avec termux-wake-lock')
ThreadingHTTPServer((HOST, PORT), H).serve_forever()
