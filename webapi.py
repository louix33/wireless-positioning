# Set up a HTTP server to receive 


from typing import Callable, List
from fastapi import FastAPI, Request, Response
from fastapi.routing import APIRoute
from pydantic import BaseModel
import msgpack
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime

# Rewrite this class to deserialize the request payload as MsgPack rather than JSON
class MsgPackRequest(Request):
    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            body = await super().body()
            if "application/msgpack" in self.headers.getlist("Content-Type"):
                body = msgpack.loads(body)
            self._body = body
        return self._body


class MsgPackRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            request = MsgPackRequest(request.scope, request.receive)
            return await original_route_handler(request)

        return custom_route_handler


class GatewayMsg(BaseModel):
    v: str
    mid: int
    time: int
    ip: str
    mac: str
    devices: List[bytes]


# Wrapper for psycopg2 connection pool
class PgConnectionPool:
    def __init__(self):
        self.pool = ThreadedConnectionPool(
            1, 10, dbname="bledata", user="user", password="password", host="localhost")

    def get_connection(self):
        return self.pool.getconn()

    def put_connection(self, conn):
        self.pool.putconn(conn)

    def __del__(self):
        self.pool.closeall()


app = FastAPI()
app.router.route_class = MsgPackRoute
connection_pool = PgConnectionPool()


@app.post("/api/bledata")
def bledata(message: GatewayMsg):
    gw_mac = message.mac # gateway mac address
    time = datetime.fromtimestamp(message.time).isoformat() 
    print("Msg from gateway {}, time = {}".format(gw_mac, time))

    # can receive multiple packets from one device in one cycle,
    # record all of them and calculate average rssi
    rssi_dict = dict() # {dev_mac: [rssi1, rssi2, ...]}
    uuid_dict = dict()

    for device in message.devices:
        dev_mac = device[1:7].hex() # device mac address
        rssi = device[7] - 256
        if dev_mac not in rssi_dict:
            rssi_dict[dev_mac] = []
        rssi_dict[dev_mac].append(rssi) # device uuid
        
        if dev_mac not in uuid_dict:
            uuid_dict[dev_mac] = device[17:33].hex()

    conn = connection_pool.get_connection()
    with conn.cursor() as cur:
        for dev_mac in rssi_dict:
            rssi = sum(rssi_dict[dev_mac]) / len(rssi_dict[dev_mac]) # average rssi
            uuid = uuid_dict[dev_mac]
            cur.execute("INSERT INTO ibeacon (gw_mac, dev_mac, uuid, rssi, time) VALUES (%s, %s, %s, %s, %s)",
                        (gw_mac, dev_mac, uuid, rssi, time))

    conn.commit()
    connection_pool.put_connection(conn)
    return {"status": "ok"}
