from flask import Flask, render_template, request, jsonify, Response
import json
import queue
import threading
import time
from datetime import datetime

app = Flask(__name__)
app.secret_key = "chat_secret_key_2024"

# Armazena as filas de cada cliente SSE conectado
clients = {}
clients_lock = threading.Lock()

# Histórico de mensagens (em memória)
message_history = []
# Lista de usuários online
online_users = {}

def add_client(client_id):
    q = queue.Queue()
    with clients_lock:
        clients[client_id] = q
    return q

def remove_client(client_id):
    with clients_lock:
        if client_id in clients:
            del clients[client_id]

def broadcast(data):
    """Envia evento para todos os clientes conectados."""
    msg = f"data: {json.dumps(data)}\n\n"
    dead = []
    with clients_lock:
        for cid, q in clients.items():
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(cid)
    for cid in dead:
        remove_client(cid)

def send_to_client(client_id, data):
    """Envia evento só para um cliente específico."""
    msg = f"data: {json.dumps(data)}\n\n"
    with clients_lock:
        if client_id in clients:
            try:
                clients[client_id].put_nowait(msg)
            except queue.Full:
                pass

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/stream")
def stream():
    """Endpoint SSE — cada aba/usuário fica conectado aqui."""
    client_id = request.args.get("client_id")
    if not client_id:
        return "Missing client_id", 400

    q = add_client(client_id)

    def event_generator():
        # Envia heartbeat inicial
        yield f"data: {json.dumps({'type': 'connected'})}\n\n"
        # Envia histórico
        for msg in message_history[-50:]:
            yield f"data: {json.dumps({'type': 'history', 'message': msg})}\n\n"
        try:
            while True:
                try:
                    msg = q.get(timeout=20)
                    yield msg
                except queue.Empty:
                    # Heartbeat a cada 20s para manter conexão viva
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
        except GeneratorExit:
            pass
        finally:
            remove_client(client_id)
            # Notifica saída se estava online
            username = online_users.pop(client_id, None)
            if username:
                broadcast({
                    "type": "user_left",
                    "username": username,
                    "online_users": list(set(online_users.values()))
                })

    return Response(event_generator(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/join", methods=["POST"])
def join():
    """Usuário entra no chat com um nome."""
    data = request.json
    client_id = data.get("client_id")
    username = data.get("username", "").strip()

    if not username or not client_id:
        return jsonify({"error": "Nome inválido"}), 400

    if len(username) > 20:
        return jsonify({"error": "Nome muito longo (máx 20 caracteres)"}), 400

    # Verifica se nome já está em uso
    if username in online_users.values():
        return jsonify({"error": "Nome já está em uso"}), 409

    online_users[client_id] = username

    system_msg = {
        "id": f"sys-{int(time.time()*1000)}",
        "type": "system",
        "text": f"{username} entrou no chat 👋",
        "timestamp": datetime.now().strftime("%H:%M")
    }
    message_history.append(system_msg)

    broadcast({
        "type": "system_message",
        "message": system_msg,
        "online_users": list(set(online_users.values()))
    })

    return jsonify({"ok": True, "username": username})

@app.route("/send", methods=["POST"])
def send():
    """Envia uma mensagem para o grupo."""
    data = request.json
    client_id = data.get("client_id")
    text = data.get("text", "").strip()

    username = online_users.get(client_id)
    if not username:
        return jsonify({"error": "Não autenticado"}), 401
    if not text:
        return jsonify({"error": "Mensagem vazia"}), 400
    if len(text) > 500:
        return jsonify({"error": "Mensagem muito longa"}), 400

    msg = {
        "id": f"msg-{int(time.time()*1000)}",
        "type": "message",
        "username": username,
        "client_id": client_id,
        "text": text,
        "timestamp": datetime.now().strftime("%H:%M")
    }
    message_history.append(msg)
    if len(message_history) > 200:
        message_history.pop(0)

    broadcast({"type": "new_message", "message": msg})
    return jsonify({"ok": True})

@app.route("/leave", methods=["POST"])
def leave():
    """Usuário sai do chat."""
    data = request.json
    client_id = data.get("client_id")
    username = online_users.pop(client_id, None)

    if username:
        system_msg = {
            "id": f"sys-{int(time.time()*1000)}",
            "type": "system",
            "text": f"{username} saiu do chat 👋",
            "timestamp": datetime.now().strftime("%H:%M")
        }
        message_history.append(system_msg)
        broadcast({
            "type": "system_message",
            "message": system_msg,
            "online_users": list(set(online_users.values()))
        })

    remove_client(client_id)
    return jsonify({"ok": True})

@app.route("/typing", methods=["POST"])
def typing():
    """Notifica que um usuário está digitando."""
    data = request.json
    client_id = data.get("client_id")
    is_typing = data.get("typing", False)
    username = online_users.get(client_id)

    if username:
        broadcast({
            "type": "typing",
            "username": username,
            "client_id": client_id,
            "typing": is_typing
        })

    return jsonify({"ok": True})

if __name__ == "__main__":
    print("\n🚀 Chat em Grupo rodando!")
    print("👉 Acesse: http://localhost:5000\n")
    app.run(debug=True, threaded=True, host="0.0.0.0", port=5000)