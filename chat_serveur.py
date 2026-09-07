#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
⚽ CF MONTRÉAL — Serveur de clavardage v4.0 (relais temps réel)
================================================================
- Sert index.html (la page des matchs + clavardage intégré)
- GET  /stream  -> flux SSE : les messages arrivent en direct
- POST /send    -> publie un message JSON {"n","t","i","tm"}
- Historique des 50 derniers messages (en mémoire) pour les nouveaux arrivants
- CORS ouvert (*) : les copies du fichier partagées par WhatsApp
  (ouvertes en file://) peuvent rejoindre le même salon

Aucune dépendance externe (bibliothèque standard uniquement).
Render : Start Command = python3 chat_serveur.py   (Build = vide)
Le port est lu depuis la variable d'environnement PORT (Render la fournit).
"""

import json
import os
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT     = int(os.environ.get("PORT", "8000"))
ICI      = os.path.dirname(os.path.abspath(__file__))
PAGE     = os.path.join(ICI, "index.html")
HIST_MAX = 50            # messages conservés en mémoire
MAX_BODY = 4096          # taille max d'un envoi
MAX_N    = 30            # pseudo
MAX_T    = 500           # texte

_lock       = threading.Lock()
_clients    = []         # files d'attente des abonnés SSE
_historique = []         # derniers messages

PAGE_BYTES = b""
if os.path.exists(PAGE):
    with open(PAGE, "rb") as f:
        PAGE_BYTES = f.read()


def journal(t):
    print(f"[{time.strftime('%H:%M:%S')}] {t}", flush=True)


def diffuser(msg):
    """Envoie le message à tous les abonnés SSE."""
    with _lock:
        cibles = list(_clients)
    ligne = "data: " + json.dumps(msg, ensure_ascii=False) + "\n\n"
    morts = []
    for q in cibles:
        try:
            q.put_nowait(ligne)
        except Exception:
            morts.append(q)
    if morts:
        with _lock:
            for q in morts:
                if q in _clients:
                    _clients.remove(q)


class Handler(BaseHTTPRequestHandler):
    server_version = "CFMChat/4.0"
    protocol_version = "HTTP/1.1"

    # ---------- utilitaires -------------------------------------------------
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):  # allège les logs Render
        pass

    # ---------- CORS preflight ----------------------------------------------
    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ---------- GET ----------------------------------------------------------
    def do_GET(self):
        if self.path.startswith("/stream"):
            return self._stream()
        if self.path.startswith("/ping"):
            return self._json(200, {"ok": True, "ts": int(time.time())})
        # page principale (/, /index.html, et n'importe quoi d'autre en GET)
        if not PAGE_BYTES:
            return self._json(503, {"erreur": "index.html manquant sur le serveur"})
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(PAGE_BYTES)))
        self.end_headers()
        self.wfile.write(PAGE_BYTES)

    def _stream(self):
        journal(f"📡 abonné SSE depuis {self.client_address[0]}")
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")   # désactive le buffering proxy
        self.end_headers()

        # 1) rejouer l'historique
        with _lock:
            histo = list(_historique)
        try:
            for m in histo:
                m2 = dict(m); m2["h"] = 1
                self.wfile.write(("data: " + json.dumps(m2, ensure_ascii=False)
                                  + "\n\n").encode("utf-8"))
            self.wfile.write(b'data: {"sys":"reprise"}\n\n')
            self.wfile.flush()
        except Exception:
            return

        # 2) s'abonner au direct
        q = queue.Queue(maxsize=100)
        with _lock:
            _clients.append(q)
        try:
            while True:
                try:
                    ligne = q.get(timeout=20)
                    self.wfile.write(ligne.encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")   # garde la connexion vivante
                self.wfile.flush()
        except Exception:
            pass
        finally:
            with _lock:
                if q in _clients:
                    _clients.remove(q)
            journal(f"📴 abonné parti ({len(_clients)} restants)")

    # ---------- POST /send ----------------------------------------------------
    def do_POST(self):
        if not self.path.startswith("/send"):
            return self._json(404, {"erreur": "route inconnue"})
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            n = 0
        if n <= 0 or n > MAX_BODY:
            return self._json(400, {"erreur": "corps invalide"})
        try:
            data = json.loads(self.rfile.read(n).decode("utf-8", "replace"))
            pseudo = str(data.get("n", "Partisan"))[:MAX_N].strip() or "Partisan"
            texte  = str(data.get("t", ""))[:MAX_T].strip()
            cid    = str(data.get("i", ""))[:64]
            tm     = int(data.get("tm", int(time.time() * 1000)))
            if not texte:
                return self._json(400, {"erreur": "message vide"})
        except Exception:
            return self._json(400, {"erreur": "JSON invalide"})

        msg = {"n": pseudo, "t": texte, "i": cid, "tm": tm}
        with _lock:
            _historique.append(msg)
            while len(_historique) > HIST_MAX:
                _historique.pop(0)
        diffuser(msg)
        journal(f"💬 {pseudo}: {texte[:60]}  ({len(_clients)} abonnés)")
        self._json(200, {"ok": True})


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    journal(f"⚽ CFM chat v4.0 — écoute sur le port {PORT} "
            f"({'page OK' if PAGE_BYTES else '⚠️ index.html ABSENT'})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
