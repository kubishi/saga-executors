"""Wayline transport: the Wayline SDK (wl.WlTask), unchanged behaviour.

recv  -> WlTask.recv_raw(peer="producer.object")   local file read
send  -> WlTask.send_raw(object, data)             PUT to the local data agent + push request
close -> WlTask.close()                             SDK reports its phase timings to the agent;
                                                    the controller copies them into status
"""
from __future__ import annotations

from wl import WlTask


class WaylineTransport:
    def __init__(self) -> None:
        self._task = WlTask()
        self.name = self._task.name
        self.node = self._task.node           # NODE_NAME (downward API)

    needs_bytes = True                      # WlTask.send_raw takes bytes

    def recv(self, peer: str) -> bytes:
        return self._task.recv_raw(peer=peer)

    def send(self, output: str | None, data: bytes) -> None:
        if output is None:
            self._task.send_raw(data)
        else:
            self._task.send_raw(output, data)

    def close(self) -> None:
        self._task.close()                    # timings go to the data agent, not stdout
        return None
