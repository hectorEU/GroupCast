from collections import OrderedDict
from random import choice

from pyactor.context import set_context, serve_forever, create_host
from pyactor.exceptions import TimeoutError

from GMS import GMS
from output import printr


class Peer(object):
    _tell = ["join", "multicast", "run", "process_msg", "receive", "set_leader"]
    _ask = ["request", "who_is_leader", "is_alive"]
    _ref = ["receive", "request", "who_is_leader", "set_leader", "is_alive"]

    def __init__(self):
        self.queue = OrderedDict()
        self.seq = 0
        self.own_seq = 0
        self.slaves = set()

    def request(self):
        self.seq += 1
        return self.seq

    def multicast(self, msg):
        try:
            seq = self.leader.request(timeout=2)
            for peer in self.gms.get_members():
                peer.receive(seq, msg)

        except TimeoutError:
            members = self.gms.get_members()
            for peer in members:
                peer.set_leader(members.sort()[0])

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
        self.gms.join(self.proxy)

    def run(self):
        self.gms = self.host.lookup_url('http://192.168.1.114:6969/tracker1', GMS)
        members = self.gms.get_members()
        if not members:
            self.leader = self.proxy
        else:
            self.leader = choice(members).who_is_leader()

    def who_is_leader(self):
        return self.leader

    def set_leader(self, peer):
        self.leader = peer

    def is_alive(self):
        return True



if __name__ == "__main__":
    set_context()

    h = create_host("http://192.168.1.114:10969")
    peers = []

    for x in xrange(0, 5):
        p = h.spawn("peer" + str(x), "Peer/Peer")
        p.run()
        p.join()
        peers.append(p)

    peers[0].multicast(peers[0].actor.id)
    ''' for p in sample(peers, 2):
        p.multicast(p.actor.id) '''

    serve_forever()
