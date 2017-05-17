import random
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta

from pyactor.context import set_context, serve_forever, interval, create_host

from output import printr


class GMS(object):
    _tell = ["join", "failfix", "run"]
    _ask = ["get_members"]
    _ref = ["join", "get_members"]

    def __init__(self, peers_offer=3, fail_timeout=12):
        self.peers = {}  # Key: Peer proxy, Value: Connection attempts
        self.peers_offer = peers_offer  # Max. random sample
        self.fail_timeout = fail_timeout  # Disconnected peer clean-up
        self.max_attempt = 5

    # Public actor methods *************************
    def join(self, peer):
        self.peers[peer] = datetime.now()
        printr(self, "Connected: " + peer.get_url())

    def get_members(self):
        members = self.peers.values()
        return random.sample(members, self.peers_offer if self.peers_offer <= len(members) else len(members))

    # ***********************************************

    # Removes inactive peers from the group
    def failfix(self):
        old_peers = set(self.peers.keys())

        for peers in self.peers.items():

        for peer in old_peers.difference(self.peers.keys()):
                printr(self, "Removed: " + peer.get_url())

    def leave(self, peer):
        self.peers.pop(peer)
        printr(self, "Disconnected: " + peer.get_url())


    # Activates tracker
    def run(self):
        self.loop = interval(self.host, self.fail_timeout, self.proxy, "failfix")


if __name__ == "__main__":
    set_context()

    h = create_host("http://10.21.6.8:6969")
    tracker = h.spawn("tracker1", GMS)
    tracker.run()
    print "Tracker ready"

    serve_forever()
