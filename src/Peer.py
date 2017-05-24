from collections import OrderedDict
from threading import Lock

from pyactor.context import set_context, serve_forever, create_host
from pyactor.exceptions import TimeoutError

from GMS import GMS
from output import printr


class Peer(object):
    _tell = ["join", "multicast", "process_msg", "receive", "coordinator", "election"]
    _ask = ["request", "is_alive"]
    _ref = ["receive", "request", "election", "coordinator", "is_alive"]
    _parallel = ["coordinator"]

    def __init__(self):
        self.queue = OrderedDict()
        self.seq = 0
        self.own_seq = 0
        self.wait_leader = Lock()

    def request(self):
        self.seq += 1
        return self.seq

    def multicast(self, msg):
        sent = False
        while not sent:
            try:
                seq = self.leader.request(timeout=2)
                for peer in self.gms.get_members():
                    peer.receive(seq, msg)
                sent = True

            except (TimeoutError, AttributeError) as e:
                self.election()
                self.wait_leader.acquire()

    def receive(self, seq, msg):
        if self.own_seq == seq - 1:
            self.process_msg(msg)
            self.own_seq += 1
            fifo = list(self.queue.items())
            while len(fifo) is not 0 and self.own_seq == fifo[0][0]:
                self.process_msg(fifo.pop())
                self.own_seq += 1
        else:
            self.queue[seq] = msg

    def process_msg(self, msg):
        printr(self, msg)

    def join(self):
        self.gms = self.host.lookup_url('http://10.21.6.8:6969/tracker1', GMS)
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
            for peer in members:
                peer.coordinator(self.proxy)
            self.wait_leader.release()

    def coordinator(self, peer):
        self.leader = peer
        self.wait_leader.release()

    def is_alive(self):
        return True



if __name__ == "__main__":
    set_context()

    h = create_host("http://10.21.6.8:7969")
    peers = []

    for x in xrange(0, 5):
        p = h.spawn("peer" + str(x), "Peer/Peer")
        p.join()
        peers.append(p)

    peers[0].multicast(peers[0].actor.id)
    ''' for p in sample(peers, 2):
        p.multicast(p.actor.id) '''

    serve_forever()
