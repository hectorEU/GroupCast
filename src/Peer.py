from pyactor.context import set_context, serve_forever, create_host, interval
from pyactor.exceptions import TimeoutError

from GMS import GMS
from output import _print


class Peer(object):
    _tell = ["multicast", "receive", "process_msg", "election", "election_callback", "unload"]
    _ask = ["request", "getleader_seq", "join", "coordinator", "is_alive"]
    _ref = ["multicast", "receive", "process_msg", "election", "election_callback", "request", "getleader_seq", "join",
            "coordinator", "unload", "is_alive"]

    def __init__(self):
        self.queue = {}
        self.seq = 0
        self.leader_seq = 0
        self.election_members = 0
        self.waiting_msg = []
        self.retry_timeout = 1

    def request(self):
        self.leader_seq += 1
        return self.leader_seq

    def getleader_seq(self):
        return self.leader_seq

    def multicast(self, msg):
        members = self.gms.get_members(self.proxy)
        try:
            if members == False:
                raise AttributeError

            if self.leader is not self.proxy:
                seq = self.leader.request(timeout=2)
            else:
                seq = self.request()

            for peer in members:
                peer.receive(seq, msg)
            self.receive(seq, msg)

        except (TimeoutError, AttributeError) as e:
            self.waiting_msg.append(msg)
            if self.gms.election_lock():
                self.election(members)

    def receive(self, seq, msg):
        # print self.leader
        # print self.id + " " + msg + " - " + str(seq) + str(self.seq) + "\n"
        self.leader_seq = max(self.leader_seq, seq)
        self.queue[seq] = msg
        for item in self.queue.items():
            if self.seq + 1 == item[0]:
                self.queue.pop(item[0])
                self.process_msg(item)
                self.seq += 1

    def process_msg(self, msg):
        _print(self, msg[1] + " - " + str(msg[0]))

    def join(self):
        self.gms = self.host.lookup_url('http://192.168.1.112:6969/tracker1', GMS)
        self.gms.join(self.proxy)
        self.interval = interval(self.host, self.retry_timeout, self.proxy, "unload")

    def election(self, members):
        found = False
        if not members:
            self.leader = self.proxy
            self.gms.election_unlock()
            return

        for peer in members:
            try:
                if self.id < peer.get_id():
                    peer.election(self.gms.get_members(peer))
                    found = True
            except TimeoutError:
                continue
        if not found:
            self.leader = self.proxy
            for peer in members:
                self.leader_seq = max(peer.coordinator(self.proxy), self.leader_seq)
            self.gms.election_unlock()

    def coordinator(self, peer):
        self.leader = peer
        return self.leader_seq

    def unload(self):
        for msg in self.waiting_msg:
            self.multicast(self.waiting_msg.pop())

    def is_alive(self):
        return True


if __name__ == "__main__":
    set_context()

    h = create_host("http://192.168.1.112:7969")
    peers = []

    p1 = h.spawn("1", "Peer/Peer")
    p2 = h.spawn("2", "Peer/Peer")
    p3 = h.spawn("3", "Peer/Peer")

    p1.join()
    p2.join()
    p3.join()

    p1.multicast("hola")
    p2.multicast("k tal")
    p3.multicast("bien")
    p1.multicast("k ase")
    p1.multicast("hola")
    p2.multicast("k tal")
    p3.multicast("bien")
    p1.multicast("k ase")
    p1.multicast("hola")
    p2.multicast("k tal")
    p3.multicast("bien")
    p1.multicast("k ase")

    serve_forever()
