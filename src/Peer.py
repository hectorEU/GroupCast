from Sequencer import Sequencer
from collections import OrderedDict


class Peer(object):

    def __init__(self):
        self.seq = Sequencer()
        self.queue = OrderedDict()
        self.peers = []
        self.gms = GMS()

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

if __name__ == "__main__":

    p = Peer()
    p.multicast("hola")