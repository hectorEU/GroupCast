from pyactor.context import set_context, serve_forever, interval, create_host
from pyactor.exceptions import TimeoutError

from output import printr


class GMS(object):
    _tell = ["join", "failfix", "run"]
    _ask = ["get_members"]
    _ref = ["join", "get_members"]

    def __init__(self):
        self.peers = {}  # Key: Peer proxy, Value: Connection attempts
        self.fail_detect = 10
        self.fail_timeout = 2
        self.max_attempts = 1

    # Public actor methods *************************
    def join(self, peer):
        self.peers[peer] = 0
        printr(self, "Connected: " + peer.get_url())

    def get_members(self):
        return self.peers.keys()

    # ***********************************************

    # Removes inactive peers from the group
    def failfix(self):
        for peer in self.peers:
            try:
                if peer.is_alive(timeout=self.fail_timeout):
                    self.peers[peer] = 0
            except TimeoutError:
                self.peers[peer] += 1
                if self.peers[peer] >= self.max_attempts:
                    self.peers.pop(peer)
                    printr(self, "Removed: " + peer.get_url())

    def leave(self, peer):
        self.peers.pop(peer)
        printr(self, "Disconnected: " + peer.get_url())

    # Activates tracker
    def run(self):
        self.loop = interval(self.host, self.fail_detect, self.proxy, "failfix")


if __name__ == "__main__":
    set_context()

    h = create_host("http://192.168.43.123:6969")
    tracker = h.spawn("tracker1", "GMS/GMS")
    tracker.run()
    print "Tracker ready"

    serve_forever()
