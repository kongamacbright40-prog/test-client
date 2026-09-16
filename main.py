from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict
import json

app = FastAPI()


class ConnectionManager:
    #session here means which room to join

    def __init__(self):
        
        self.sessions: Dict[str, Dict[str, WebSocket]] = {} #a dictionary of rooms, where each room is itself a dictionary of people and their phone lines

    async def connect(self, session_id: str, client_id: str, websocket: WebSocket):
        await websocket.accept()#wait untill the call is been taken
        self.sessions.setdefault(session_id, {})[client_id] = websocket
        
        await self.broadcast(# simply showing the below info of who joined the room
            session_id,
            {"type": "peer-joined", "from": client_id},
            exclude=client_id,# paasing the info only to those inthe room except the person that joined
        )

    def disconnect(self, session_id: str, client_id: str):
        if session_id in self.sessions: #the code here explains when a client wants toshut down the call so it has to check if the room exist first
            self.sessions[session_id].pop(client_id, None)
            if not self.sessions[session_id]:#when every leaves the room, end the call making sure no one is inside
                del self.sessions[session_id]

    async def broadcast(self, session_id: str, message: dict, exclude: str = None): #cid= client id, ws = websocket
        for cid, ws in self.sessions.get(session_id, {}).items():
            if cid != exclude: #exclude here means if nobody tells us to skip anyone, exclude becomes NONE
                await ws.send_json(message)

    async def send_to(self, session_id: str, target_id: str, message: dict):# here is a one to connection between two clients
        ws = self.sessions.get(session_id, {}).get(target_id)
        if ws:
            await ws.send_json(message)


manager = ConnectionManager()


@app.websocket("/ws/classroom/{session_id}/{client_id}")
async def signaling_endpoint(websocket: WebSocket, session_id: str, client_id: str):
    await manager.connect(session_id, client_id, websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            
            if msg_type in ("offer", "answer", "ice-candidate"):
                target = data.get("target")
                if target:
                    await manager.send_to(
                        session_id,
                        target,
                        {**data, "from": client_id},
                    )
            else:
                
                await manager.broadcast(
                    session_id, {**data, "from": client_id}, exclude=client_id
                )

    except WebSocketDisconnect:
        manager.disconnect(session_id, client_id)
        await manager.broadcast(
            session_id, {"type": "peer-left", "from": client_id}
        )


@app.get("/")
def health_check():
    return {"status": "signaling server running"}