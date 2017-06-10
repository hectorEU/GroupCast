from os.path import basename
from threading import Timer

from pyactor.context import set_context, serve_forever, create_host, later
from pyactor.exceptions import TimeoutError

from output import _print


class GMS(object):
    _tell = ["election_unlock", "run", "failfix", "deadlock_fix"]
    _ask = ["join", "get_members", "election_lock", "get_lock"]
    _ref = ["election_unlock", "run", "join", "get_members", "election_lock", "failfix", "get_lock", "deadlock_fix"]

    def __init__(self):
        self.members = {}  # id : proxy
        self.waiting_join = set()
        self.fail_detect = 120
        self.fail_timeout = 60
        self.election = False
        self.checking = False
        self.deadlock_timeout = 30

    # Public actor methods *******************************

    def join(self, member):
        if not self.election:
            self.members[int(basename(member.get_url()))] = member
            _print(self, "Connected: " + member.get_url())
        else:
            self.waiting_join.add(member)

    def leave(self, member):
        self.members.pop(int(member.get_id()))
        _print(self, "Disconnected: " + member.get_url())

    def get_members(self, member):
        members = list(self.members.values())
        try:
            members.remove(member)  # If member not in members, return False
            return members
        except ValueError:
            return False

    def election_lock(self):
        if not self.election:
            self.election = True
            self.timer = Timer(self.deadlock_detect, self.election_unlock)
            self.timer.start()
            _print(self, "Election started")
            return True
        else:
            return False

    def election_unlock(self):
        self.timer.cancel()
        self.election = False
        for new_member in self.waiting_join:
            self.waiting_join.pop().join(self.proxy)
        _print(self, "Election finished")

    def get_lock(self):
        return self.election

    # Internal methods ***************************************************************

    # Removes inactive members from the group
    def failfix(self):
        for member in self.members.items():
            try:
                if member[1].is_alive(timeout=self.fail_timeout):
                    continue
            except TimeoutError:
                self.members.pop(member[0])
                _print(self, "Removed: " + member[1].get_url())
        self.loop = later(self.host, self.fail_detect, self.proxy, "failfix")


    # Activates tracker
    def run(self):
        self.loop = later(self.fail_detect, self.proxy, "failfix")

        # ********************************************************************************

if __name__ == "__main__":
    set_context()

    h = create_host("http://192.168.1.112:6969")
    gms = h.spawn("gms1", "GMS/GMS")
    gms.run()
    print "GMS ready"

    serve_forever()
