#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
⚽ CF MONTRÉAL — Serveur de clavardage v4.1 (relais temps réel + PONT MQTT)
================================================================
- Sert index.html (la page des matchs + clavardage intégré)
- GET  /stream  -> flux SSE : les messages arrivent en direct
- POST /send    -> publie un message JSON {"n","t","i","tm"}
- GET  /etat    -> état (abonnés, pont MQTT, nb de messages)
- Historique des 50 derniers messages (en mémoire) pour les nouveaux arrivants
- CORS ouvert (*) : les copies du fichier partagées par WhatsApp
  (ouvertes en file://) peuvent rejoindre le même salon
- 🌉 PONT MQTT : fusionne le salon public MQTT (vieux fichiers v3.x,
  app impact_gui.py, repli automatique) avec ce serveur -> UN SEUL salon

Dépendance : paho-mqtt (pip install paho-mqtt) — sans lui, le serveur
fonctionne quand même, mais le pont MQTT est désactivé.
Render : Start Command = python3 chat_serveur.py   (Build = pip install -r requirements.txt)
Le port est lu depuis la variable d'environnement PORT (Render la fournit).
"""

import json
import os
import queue
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT       = int(os.environ.get("PORT", "8000"))
ICI        = os.path.dirname(os.path.abspath(__file__))
PAGE       = os.path.join(ICI, "index.html")
HIST_MAX   = 50            # messages conservés en mémoire
MAX_BODY   = 4096          # taille max d'un envoi
MAX_N      = 30            # pseudo
MAX_T      = 500           # texte
TOPIC_MQTT = "cfm-partisans/x7k2q9/salon"   # même salon que partout

_lock       = threading.Lock()
_clients    = []         # files d'attente des abonnés SSE
_historique = []         # derniers messages
_mqtt       = {"client": None, "pret": False}

PAGE_BYTES = b""
if os.path.exists(PAGE):
    with open(PAGE, "rb") as f:
        PAGE_BYTES = f.read()


def journal(t):
    print(f"[{time.strftime('%H:%M:%S')}] {t}", flush=True)


def _dans_historique(i, tm):
    with _lock:
        for m in _historique:
            if m.get("i") == i and m.get("tm") == tm:
                return True
    return False


def _parse_msg_mqtt(raw):
    try:
        d = json.loads(raw.decode("utf-8", "replace")
                       if isinstance(raw, (bytes, bytearray)) else raw)
        n  = str(d.get("n", "Partisan"))[:MAX_N].strip() or "Partisan"
        t  = str(d.get("t", ""))[:MAX_T].strip()
        i  = str(d.get("i", ""))[:64]
        tm = int(d.get("tm", int(time.time() * 1000)))
        return {"n": n, "t": t, "i": i, "tm": tm} if t else None
    except Exception:
        return None


def mqtt_publier(msg):
    """Renvoie le message vers le salon MQTT public (vieux fichiers, app .py)."""
    c = _mqtt.get("client")
    if _mqtt.get("pret") and c:
        try:
            c.publish(TOPIC_MQTT, json.dumps(msg, ensure_ascii=False), qos=0)
        except Exception:
            pass


def pont_mqtt():
    """Fil : écoute le salon MQTT public et le fusionne avec le salon local."""
    try:
        import paho.mqtt.client as mqtt_lib
    except ImportError:
        journal("⚠️  paho-mqtt absent — pont MQTT désactivé (pip install paho-mqtt)")
        return
    COURTIERS = [("broker.emqx.io",   8084, True, "/mqtt"),
                 ("broker.hivemq.com", 8884, True, "/mqtt"),
                 ("test.mosquitto.org", 8081, True, "/mqtt")]
    idx = 0
    while True:
        host, port, tls, path = COURTIERS[idx % len(COURTIERS)]
        idx += 1
        pret = threading.Event()

        def on_connect(c, u, f, rc, *_):
            if rc == 0:
                _mqtt["pret"] = True
                pret.set()
                c.subscribe(TOPIC_MQTT, qos=0)
                journal(f"🌐 pont MQTT connecté : {host}:{port}")
            else:
                journal(f"🌐 pont MQTT refusé ({rc}) : {host}")

        def on_disconnect(c, u, rc, *_):
            if _mqtt["pret"]:
                journal("🌐 pont MQTT déconnecté")
            _mqtt["pret"] = False

        def on_message(c, u, m):
            p = _parse_msg_mqtt(m.payload)
            if not p:
                return
            if p["i"] and _dans_historique(p["i"], p["tm"]):
                return                      # notre propre renvoi qui revient
            with _lock:
                _historique.append(p)
                while len(_historique) > HIST_MAX:
                    _historique.pop(0)
            diffuser(p)
            journal(f"💬 (mqtt) {p['n']} : {p['t'][:60]}")

        try:
            cid = "cfm-serveur-" + uuid.uuid4().hex[:12]
            try:
                c = mqtt_lib.Client(mqtt_lib.CallbackAPIVersion.VERSION1,
                                    client_id=cid, transport="websockets")
            except (AttributeError, TypeError):
                c = mqtt_lib.Client(client_id=cid, transport="websockets")
            c.on_connect, c.on_disconnect, c.on_message = \
                on_connect, on_disconnect, on_message
            c.ws_set_options(path=path)
            if tls:
                c.tls_set()
            c.connect_async(host, port, keepalive=45)
            c.loop_start()
            _mqtt["client"] = c
            pret.wait(10)
            if not _mqtt["pret"]:
                journal(f"🌐 pont MQTT délai dépassé : {host}")
            while _mqtt["pret"]:
                time.sleep(1)
            try:
                c.loop_stop()
                c.disconnect()
            except Exception:
                pass
            _mqtt["client"] = None
            _mqtt["pret"] = False
        except Exception as e:
            journal(f"🌐 pont MQTT erreur : {e}")
        time.sleep(5)                       # courtier suivant / nouvel essai


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
        if self.path.startswith("/etat"):
            with _lock:
                n = len(_clients)
                nb = len(_historique)
            return self._json(200, {"version": "4.1", "clients_sse": n,
                                    "pont_mqtt": bool(_mqtt.get("pret")),
                                    "messages": nb})
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
        mqtt_publier(msg)          # 🌉 renvoyer au salon MQTT public
        journal(f"💬 {pseudo}: {texte[:60]}  ({len(_clients)} abonnés)")
        self._json(200, {"ok": True})


def main():
    threading.Thread(target=pont_mqtt, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    journal(f"⚽ CFM chat v4.1 — écoute sur le port {PORT} "
            f"({'page OK' if PAGE_BYTES else '⚠️ index.html ABSENT'})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
