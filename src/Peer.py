from Sequencer import Sequencer
from collections import OrderedDict
from GMS import GMS
from pyactor.context import set_context, serve_forever, interval, create_host


class Peer(object):
    _tell = ["join", "multicast", "run"]

    def __init__(self):
        self.seq = Sequencer()
        self.queue = OrderedDict()
        self.peers = []

    def __hash__(self):
        return self.get_url()

    def __eq__(self, other):
        return self.get_url() == other.get_url()

    def __ne__(self, other):
        return not self.__eq__(self, other)

    def multicast(self, msg):
        id = self.seq.request()
        for peer in self.peers:
            peer.receive(msg, id)

    def receive(self, msg, id):
        self.queue[id] = msg

    def process_msg(self):
        keys = self.queue.keys()
        if keys[1] > keys[0]+1:
            raise LookupError
        return self.queue.pop(0)

    def join(self):
        self.gms.join(self.proxy)

    def run(self):
        self.gms = self.host.lookup_url('http://10.21.6.8:6969/tracker1', GMS)

if __name__ == "__main__":
    set_context()

    h = create_host("http://10.21.6.8:17969")

    p = h.spawn("peer1", "Peer/Peer")
    p.run()
    p.join()
    # p.multicast("hola")

    serve_forever()