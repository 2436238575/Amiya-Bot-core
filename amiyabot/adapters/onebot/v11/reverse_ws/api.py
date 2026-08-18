"""Generic OneBot 11 reverse WebSocket adapter.

The OneBot implementation is the WebSocket client in this mode. Amiya owns
the listener, receives events, and sends actions over the same connection.
"""

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional
from urllib.parse import parse_qs, urlsplit

import websockets
from amiyalog import LoggerManager

from amiyabot.adapters import BotAdapterProtocol, HANDLER_TYPE
from amiyabot.builtin.message import Message
from amiyabot.builtin.messageChain import Chain

from amiyabot.adapters.onebot.v11.api import OneBot11API
from amiyabot.adapters.onebot.v11.builder import OneBot11MessageCallback, build_message_send
from amiyabot.adapters.onebot.v11.package import package_onebot11_message


log = LoggerManager('ReverseWebSocket')


class ReverseWebSocketResponse:
    """Small response object compatible with the HTTP API response usage."""

    def __init__(self, payload: dict):
        self.json = payload

    @property
    def status(self):
        return self.json.get('status')

    @property
    def retcode(self):
        return self.json.get('retcode')

    @property
    def data(self):
        return self.json.get('data')

    @property
    def echo(self):
        return self.json.get('echo')

    @property
    def wording(self):
        return self.json.get('wording')

    @property
    def message(self):
        return self.json.get('message')

    @property
    def stream(self):
        return self.json.get('stream')


@dataclass
class _PendingCall:
    future: asyncio.Future
    stream_queue: asyncio.Queue = field(default_factory=asyncio.Queue)


class ReverseWebSocketAPI(OneBot11API):
    """OneBot API proxy transported by a reverse WebSocket connection."""

    def __init__(self, instance: 'ReverseWebSocketInstance'):
        super().__init__(instance.host, instance.http_port, instance.token)
        self.instance = instance

    async def call(self, action: str, params: Optional[dict] = None, timeout: float = 30):
        return await self.instance.call_action(action, params or {}, timeout=timeout)

    async def call_stream(self, action: str, params: Optional[dict] = None, timeout: float = 30) -> AsyncIterator[ReverseWebSocketResponse]:
        echo = self.instance._new_echo()
        pending = self.instance._register_pending(echo)
        try:
            await self.instance._send_action(action, params or {}, echo)
            while True:
                response = await asyncio.wait_for(pending.stream_queue.get(), timeout)
                yield response
                if response.stream != 'stream-action':
                    break
        finally:
            self.instance._drop_pending(echo)

    async def get(self, url: str, params: Optional[dict] = None, *args, **kwargs):
        return await self.call(url.strip('/'), params or kwargs.get('params') or {})

    async def post(self, url: str, data: Optional[dict] = None, *args, **kwargs):
        return await self.call(url.strip('/'), data or kwargs.get('data') or {})

    async def request(self, url: str, method: str, *args, **kwargs):
        data = kwargs.get('data', kwargs.get('params'))
        return await self.call(url.strip('/'), data or {})

    def __getattr__(self, name: str):
        # The action registry is intentionally open-ended. Existing
        # explicit standard methods remain normal methods; unknown action names
        # are exposed as async convenience methods without a copied action list.
        if name.startswith('_'):
            raise AttributeError(name)

        async def action_method(*args, **kwargs):
            params = dict(kwargs)
            if args:
                if len(args) == 1 and isinstance(args[0], dict):
                    params = {**args[0], **params}
                else:
                    raise TypeError(f'{name} accepts keyword parameters or one dict')
            return await self.call(name, params)

        return action_method


class ReverseWebSocketInstance(BotAdapterProtocol):
    def __init__(self, appid: str, token: str, host: str, ws_port: int, http_port: int = 0):
        super().__init__(appid, token)
        self.host = host or '0.0.0.0'
        self.ws_port = ws_port
        self.http_port = http_port
        self.connection = None
        self._connections: dict[Any, str] = {}
        self._server = None
        self._handler: Optional[HANDLER_TYPE] = None
        self._stop_event = asyncio.Event()
        self._pending: dict[str, _PendingCall] = {}
        self._send_lock = asyncio.Lock()

    def __str__(self):
        return 'ReverseWS'

    @property
    def api(self):
        return ReverseWebSocketAPI(self)

    async def start(self, handler: HANDLER_TYPE):
        self._handler = handler
        self._stop_event.clear()
        self._server = await websockets.serve(
            self._handle_connection,
            self.host,
            self.ws_port,
            ping_interval=20,
            ping_timeout=20,
        )
        log.info(f'reverse websocket listening on {self.host}:{self.ws_port}')
        await self._stop_event.wait()

    async def close(self):
        self.keep_run = False
        self._stop_event.set()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for connection in list(self._connections):
            await connection.close()
        self._connections.clear()
        self.connection = None
        for pending in list(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(ConnectionError('Reverse WebSocket closed'))
        self._pending.clear()

    async def _handle_connection(self, websocket):
        request = getattr(websocket, 'request', None)
        headers = getattr(request, 'headers', {}) if request else {}
        path = getattr(request, 'path', '/') if request else '/'

        if not self._authorized(headers, path):
            await self._reject(websocket, 1403, 'token验证失败')
            return
        self_id = self._header(headers, 'X-Self-ID')
        if not self_id or str(self_id) != str(self.appid):
            await self._reject(websocket, 1403, 'X-Self-ID不匹配')
            return

        role = self._connection_role(headers, path)
        if role not in {'api', 'event', 'universal'}:
            await websocket.close(code=1008, reason='invalid client role')
            return

        self._connections[websocket] = role
        if role in {'api', 'universal'} and self.connection is None:
            self.connection = websocket
        self.set_alive(True)
        try:
            async for raw in websocket:
                await self._handle_frame(websocket, raw)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._connections.pop(websocket, None)
            if self.connection is websocket:
                self.connection = self._find_api_connection()
                if self.connection is None:
                    self._fail_pending(ConnectionError('Reverse WebSocket disconnected'))
            if not self._connections:
                self.set_alive(False)

    async def _handle_frame(self, websocket, raw):
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8')
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return
        echo = payload.get('echo')
        if echo is not None and str(echo) in self._pending:
            self._resolve_pending(str(echo), payload)
            return
        if 'post_type' in payload and self._handler:
            asyncio.create_task(self._handler(await package_onebot11_message(self, self.appid, payload)))

    async def _reject(self, websocket, retcode: int, wording: str):
        try:
            await websocket.send(json.dumps({
                'status': 'failed', 'retcode': retcode, 'data': None,
                'wording': wording, 'message': wording,
            }, ensure_ascii=False))
        finally:
            await websocket.close(code=1008, reason=wording)

    def _authorized(self, headers, path: str) -> bool:
        if not self.token:
            return True
        authorization = self._header(headers, 'Authorization') or ''
        if authorization == f'Bearer {self.token}' or authorization == self.token:
            return True
        query = parse_qs(urlsplit(path).query)
        return query.get('access_token', [None])[0] == self.token

    @staticmethod
    def _header(headers, name: str):
        try:
            return headers.get(name) or headers.get(name.lower())
        except AttributeError:
            return None

    @classmethod
    def _connection_role(cls, headers, path: str) -> str:
        role = (cls._header(headers, 'X-Client-Role') or '').lower()
        if role in {'api', 'event', 'universal'}:
            return role
        clean_path = urlsplit(path).path.rstrip('/') or '/'
        if clean_path.endswith('/api'):
            return 'api'
        if clean_path.endswith('/event'):
            return 'event'
        return 'universal'

    def _find_api_connection(self):
        for websocket, role in self._connections.items():
            if role in {'api', 'universal'}:
                return websocket
        return None

    def _new_echo(self):
        return uuid.uuid4().hex

    def _register_pending(self, echo: str):
        pending = _PendingCall(asyncio.get_running_loop().create_future())
        self._pending[echo] = pending
        return pending

    def _drop_pending(self, echo: str):
        self._pending.pop(echo, None)

    def _fail_pending(self, error: Exception):
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.set_exception(error)

    def _resolve_pending(self, echo: str, payload: dict):
        pending = self._pending.get(echo)
        if not pending:
            return
        response = ReverseWebSocketResponse(payload)
        if response.stream == 'stream-action':
            pending.stream_queue.put_nowait(response)
            return
        pending.stream_queue.put_nowait(response)
        if not pending.future.done():
            pending.future.set_result(response)

    async def _send_action(self, action: str, params: dict, echo: str):
        websocket = self._find_api_connection()
        if websocket is None:
            self._drop_pending(echo)
            raise ConnectionError('Reverse WebSocket is not connected')
        async with self._send_lock:
            await websocket.send(json.dumps({'action': action, 'params': params, 'echo': echo}, ensure_ascii=False))

    async def call_action(self, action: str, params: dict, timeout: float = 30):
        echo = self._new_echo()
        pending = self._register_pending(echo)
        try:
            await self._send_action(action.strip('/'), params, echo)
            return await asyncio.wait_for(pending.future, timeout)
        finally:
            self._drop_pending(echo)

    async def send_chain_message(self, chain: Chain, is_sync: bool = False):
        reply, voice_list, cq_codes = await build_message_send(chain)
        responses = []
        for item in [reply, *cq_codes, *voice_list]:
            if is_sync:
                responses.append(await self.api.post('/send_msg', item))
            else:
                echo = self._new_echo()
                await self._send_action('send_msg', item, echo)
        return [OneBot11MessageCallback(chain.data, self, item) for item in responses]

    async def build_active_message_chain(self, chain: Chain, user_id: str, channel_id: str, direct_src_guild_id: str):
        data = Message(self)
        data.user_id = user_id
        data.channel_id = channel_id
        data.message_type = 'group'
        if not channel_id and not user_id:
            raise TypeError('send_message() missing argument: "channel_id" or "user_id"')
        if not channel_id and user_id:
            data.message_type = 'private'
            data.is_direct = True
        message = Chain(data)
        message.chain = chain.chain
        message.builder = chain.builder
        return message

    async def recall_message(self, message_id: str, data: Optional[Message] = None):
        await self.api.delete_msg(message_id)


def reverse_ws(host: str, ws_port: int, http_port: int = 0):
    def adapter(appid: str, token: str):
        return ReverseWebSocketInstance(appid, token, host, ws_port, http_port)

    return adapter
