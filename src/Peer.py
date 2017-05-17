from collections import OrderedDict

from pyactor.context import set_context, serve_forever, create_host

from GMS import GMS


class Peer(object):
    _tell = ["join", "multicast", "run", "process_msg", "receive", "set_leader"]
    _ask = ["is_alive", "request"]
    _ref = ["receive", "is_alive", "request"]

    def __init__(self):
        self.queue = OrderedDict()
        self.seq = 0

    def request(self):
        self.seq += 1
        return self.seq

    def multicast(self, msg):
        seq = self.leader.request()
        for peer in self.gms.get_members():
            peer.receive(seq, msg)

    def receive(self, seq, msg):
        if self.seq == seq:
            self.process_msg(msg)
            self.seq += 1
            fifo = list(self.queue)
            while len(fifo) is not 0 and self.seq == fifo[0][0]:
                self.process_msg(fifo.pop())
                self.seq += 1
        else:
            self.queue[seq] = msg

    def process_msg(self, msg):
        print msg

    def is_alive(self):
        return True

    def join(self):
        self.gms.join(self.proxy)

    def run(self):
        self.gms = self.host.lookup_url('http://10.21.6.8:6969/tracker1', GMS)

    def set_leader(self, peer):
        self.leader = peer


if __name__ == "__main__":
    set_context()

    h = create_host("http://10.21.6.8:26969")

    p = h.spawn("peer1", "Peer/Peer")
    p.run()
    p.join()
    p2 = h.spawn("peer2", "Peer/Peer")
    p2.run()
    p2.join()

    p.set_leader(p)
    p2.set_leader(p)

    p2.multicast("hola")

    serve_forever()
