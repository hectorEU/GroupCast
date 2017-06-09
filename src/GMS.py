from pyactor.context import set_context, serve_forever, interval, create_host
from pyactor.exceptions import TimeoutError

from output import _print


class GMS(object):
    _tell = ["election_unlock", "run", "failfix"]
    _ask = ["join", "get_members", "election_lock"]
    _ref = ["election_unlock", "run", "join", "get_members", "election_lock", "failfix"]


    def __init__(self):
        self.peers = []
        self.fail_detect = 5
        self.fail_timeout = 2
        self.election = False
        self.waiting_join = []

    # Public actor methods *************************
    def join(self, peer):
        if not self.election:
            self.peers.append(peer)
            _print(self, "Connected: " + peer.get_url())
        else:
            self.waiting_join.append(peer)

    def get_members(self, peer):
        peers = list(self.peers)
        try:
            peers.remove(peer)
            return peers
        except ValueError:
            return False

    def election_unlock(self):
        self.election = False
        for peer in self.waiting_join:
            self.waiting_join.pop().join()

    def election_lock(self):
        if not self.election:
            self.election = True
            return True
        else:
            return False

    def get_lock(self):
        return self.election

    # ***********************************************

    # Removes inactive peers from the group
    def failfix(self):
        for peer in self.peers:
            try:
                if peer.is_alive(timeout=self.fail_timeout):
                    continue
            except TimeoutError:
                self.peers.remove(peer)
                _print(self, "Removed: " + peer.get_url())

    def leave(self, peer):
        self.peers.pop(peer)
        _print(self, "Disconnected: " + peer.get_url())

    # Activates tracker
    def run(self):
        self.loop = interval(self.host, self.fail_detect, self.proxy, "failfix")
        self.loop2 = interval(self.host, self.deadlock_detect, self.proxy, "deadlock")


if __name__ == "__main__":
    set_context()

    h = create_host("http://192.168.1.112:6969")
    tracker = h.spawn("tracker1", "GMS/GMS")
    tracker.run()
    print "Tracker ready"

    serve_forever()
