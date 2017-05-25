from collections import OrderedDict

from pyactor.context import set_context, serve_forever, create_host
from pyactor.exceptions import TimeoutError

from GMS import GMS
from output import printr


class Peer(object):
    _tell = ["join", "multicast", "process_msg", "receive", "coordinator", "election"]
    _ask = ["request", "is_alive"]
    _ref = ["receive", "request", "election", "coordinator", "is_alive", "multicast"]
    _parallel = ["coordinator"]

    def __init__(self):
        self.queue = OrderedDict()
        self.seq = 0
        self.own_seq = 0
        self.pending = []

    def request(self):
        self.seq += 1
        return self.seq

    def multicast(self, msg):
        try:
            seq = self.leader.request(timeout=2)
            for peer in self.gms.get_members():
                peer.receive(seq, msg)

        except (TimeoutError, AttributeError) as e:
            self.pending.append(msg)
            self.election()

    def receive(self, seq, msg):
        self.seq = max(self.own_seq, seq)
        if self.own_seq == seq - 1:
            self.process_msg(msg)
            fifo = list(self.queue.items())
            while fifo and self.own_seq == fifo[0][0]:
                self.process_msg(fifo.pop())
                self.own_seq += 1
        else:
            self.queue[seq] = msg

    def process_msg(self, msg):
        printr(self, msg + " - " + str(self.seq))

    def join(self):
        self.gms = self.host.lookup_url('http://192.168.43.123:6969/tracker1', GMS)
        self.gms.join(self.proxy)

    def election(self):
        found = False
        members = self.gms.get_members()
        for peer in members:
            try:
                if self.id < peer.get_id() and peer.is_alive():
                    peer.election()
                    found = True
            except TimeoutError:
                continue
        if not found:
            self.seq = self.own_seq
            for peer in members:
                peer.coordinator(self.proxy)

    def coordinator(self, peer):
        self.leader = peer
        self.pending.reverse()
        [self.multicast(self.pending.pop()) for msg in self.pending]

    def is_alive(self):
        return True

if __name__ == "__main__":
    set_context('green_thread')

    h = create_host("http://192.168.43.123:7969")
    peers = []

    p1 = h.spawn("1", "Peer/Peer")
    p1.join()

    p2 = h.spawn("2", "Peer/Peer")
    p2.join()

    p3 = h.spawn("3", "Peer/Peer")
    p3.join()

    p1.multicast("hola")
    p2.multicast("k tal")
    p3.multicast("bien")

    serve_forever()
