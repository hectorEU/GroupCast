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
        self.index = defaultdict(list)  # Key: File name; Value: List of peer proxies
        self.last_announces = {}  # Key: Peer url; Value: Timestamp
        self.peers_offer = peers_offer  # Max. random sample
        self.fail_timeout = fail_timeout  # Disconnected peer clean-up

    # Public actor methods *************************
    def join(self, file_name, peer_ref):
        if peer_ref not in self.index[file_name]:
            self.index[file_name].append(peer_ref)  # First time announce, add member to swarm

        self.last_announces[peer_ref.actor.url] = datetime.now()  # Keep announce timestamp
        printr(self, "Subscribed: " + peer_ref.actor.url + " in: " + file_name)

    def get_members(self, file_name):
        if file_name not in self.index:  # Control invalid requests from members out of the swarm
            return file_name, None
        peers = self.index[file_name]
        return [file_name, random.sample(peers,  # Select a random sample of connected peers
                                         self.peers_offer if self.peers_offer <= len(peers) else len(peers))]

    # ***********************************************

    # Removes inactive peers from the swarms
    def failfix(self):
        # Remove inactive peers from self.last_announces
        self.last_announces = {peer: timestamp for peer, timestamp in self.last_announces.items() if
                               datetime.now() - timestamp <= timedelta(seconds=self.fail_timeout)}

        # Remove inactive peers from self.index
        for file_name, peers in self.index.items():
            for peer in peers:
                if peer.actor.url not in self.last_announces:
                    self.leave(file_name, peer)

    def leave(self, file_name, peer):
        self.index[file_name].remove(peer)
        printr(self, "Unsubscribed: " + peer.actor.url + " of: " + file_name)

    # Activates tracker
    def run(self):
        self.loop1 = interval(self.host, self.fail_timeout, self.proxy, "failfix")


if __name__ == "__main__":
    set_context()

    h = create_host("http://10.21.6.8:6969")
    tracker = h.spawn("tracker1", Tracker)
    tracker.run()
    print "Tracker ready"

    serve_forever()
