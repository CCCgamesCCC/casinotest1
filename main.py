import asyncio
import json
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Roulette Real Multiplayer Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Европейская рулетка (37 секторов)
WHEEL_ORDER = [
    0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23, 10,
    5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26
]
RED_NUMBERS = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]

# Глобальное состояние игры
connected_clients = set()
round_phase = "betting"  # "betting" | "spinning" | "result"
time_left = 15
current_bets = []  # Список всех ставок раунда
history = [26, 3, 0, 19, 12]  # Стартовая история

async def broadcast(message: dict):
    """Отправка сообщения всем подключенным игрокам."""
    if not connected_clients:
        return
    dead_sockets = set()
    payload = json.dumps(message)
    for ws in list(connected_clients):
        try:
            await ws.send_text(payload)
        except Exception:
            dead_sockets.add(ws)
    connected_clients.difference_update(dead_sockets)

async def game_loop():
    """Единый синхронный игровой цикл для всех игроков."""
    global round_phase, time_left, current_bets, history
    while True:
        # --- ФАЗА 1: Прием ставок (15 секунд) ---
        round_phase = "betting"
        time_left = 15
        current_bets.clear()
        await broadcast({
            "type": "phase_betting",
            "time_left": time_left,
            "history": history
        })

        while time_left > 0:
            await asyncio.sleep(1)
            time_left -= 1
            await broadcast({
                "type": "tick",
                "time_left": time_left
            })

        # --- ФАЗА 2: Вращение колеса и шарика (5.5 секунд) ---
        round_phase = "spinning"
        win_index = random.randint(0, len(WHEEL_ORDER) - 1)
        win_number = WHEEL_ORDER[win_index]

        await broadcast({
            "type": "phase_spinning",
            "win_index": win_index,
            "win_number": win_number,
            "duration": 5500
        })
        await asyncio.sleep(5.5)

        # --- ФАЗА 3: Подведение итогов (4 секунды) ---
        round_phase = "result"
        history.append(win_number)
        history = history[-10:]  # Оставляем последние 10 чисел

        await broadcast({
            "type": "phase_result",
            "win_number": win_number,
            "history": history
        })
        await asyncio.sleep(4.0)

@app.on_event("startup")
async def startup_event():
    # Запускаем игровой цикл в фоне при старте сервера
    asyncio.create_task(game_loop())

@app.get("/")
async def root():
    return {
        "status": "online",
        "phase": round_phase,
        "online_players": len(connected_clients)
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)

    # При подключении сразу передаем игроку актуальное состояние стола
    try:
        await websocket.send_json({
            "type": "init",
            "phase": round_phase,
            "time_left": time_left,
            "bets": current_bets,
            "history": history,
            "online": len(connected_clients)
        })
        # Оповещаем остальных об обновлении онлайна
        await broadcast({
            "type": "online_count",
            "online": len(connected_clients)
        })

        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)

            # Если игрок сделал ставку во время фазы приема
            if data.get("action") == "place_bet" and round_phase == "betting":
                bet = data.get("bet")
                if bet:
                    current_bets.append(bet)
                    # Рассылаем ставку всем остальным игрокам
                    await broadcast({
                        "type": "new_bet",
                        "bet": bet
                    })

    except WebSocketDisconnect:
        connected_clients.discard(websocket)
        await broadcast({
            "type": "online_count",
            "online": len(connected_clients)
        })
    except Exception:
        connected_clients.discard(websocket)