from os.path import basename
from threading import Timer

from output import _print
from pyactor.context import set_context, serve_forever, create_host, later
from pyactor.exceptions import TimeoutError


class GMS(object):
    _tell = ["election_unlock", "run", "failfix", "deadlock_fix"]
    _ask = ["join", "get_members", "election_lock", "get_lock"]
    _ref = ["election_unlock", "run", "join", "get_members", "election_lock", "get_lock", "failfix", "deadlock_fix"]

    def __init__(self):
        self.members = {}  # Key -> Member ID : Value -> Member proxy object. Dictionary=Unique ID per member
        self.waiting_join = set()  # Members waiting to join because an ongoing election. Set=Unique ID per member
        self.election = False  # Election lock
        self.fail_detect = 120  # Max. seconds to check all members connectivity
        self.fail_timeout = 60  # Max. seconds to check if a single member is up
        self.deadlock_detect = 30  # Lock agent inspector

    # Public actor methods *********************************************************************************************

    # Join the group
    def join(self, member):
        if not self.election:
            self.members[int(basename(member.get_url()))] = member
            # Basename is used to extract ID from http://host_IP/ID_number -> ID_number

            _print(self, "Connected: " + member.get_url())
        else:
            self.waiting_join.add(member)  # Queue member to waiting list for joining the group

    # Leave the group
    def leave(self, member):
        id = int(member.get_id())
        if id in self.members:
            self.members.pop(id)
            _print(self, "Disconnected: " + member.get_url())

    # Get members from the group
    def get_members(self, member):
        members = list(self.members.values())  # members = all proxys from the group in GMS
        try:
            members.remove(member)
            return members  # Returning list of all members in GMS but the calling member
        except ValueError:
            return False  # If member not in GMS (it didn't join), return False

    def election_lock(self):
        if not self.election:
            self.election = True
            self.timer = Timer(self.deadlock_detect, self.election_unlock)
            self.timer.start()
            # If no member calls election_unlock within self.deadlock_detect seconds, self.election lock will be
            # by the GMS

            _print(self, "Election started")
            return True  # Lock acquired, return True
        else:
            return False  # There's already an election running, return False

    def election_unlock(self):
        self.timer.cancel()  # Election success, cancel timer to call this method forcefully
        self.election = False
        for new_member in self.waiting_join:
            self.waiting_join.pop().join(self.proxy)
            # Election finished, call new members to join the GMS. Let them know which is the GMS by passing the
            # this GMS proxy (self.proxy)

        _print(self, "Election finished")

    # Query lock status
    def get_lock(self):
        return self.election

    # Internal methods *************************************************************************************************

    # Removes inactive members from the group
    def failfix(self):
        for member in self.members.items():
            try:
                if member[1].is_alive(timeout=self.fail_timeout):
                    continue
            except TimeoutError:
                self.members.pop(member[0])
                _print(self, "Removed: " + member[1].get_url())
        self.loop = later(self.host, self.fail_detect, self.proxy, "failfix")  # Recursive timer, working as "interval"

    # Activates timer mode "later" not "interval", in order to avoid overlapping calls and collapse the GMS
    def run(self):
        self.loop = later(self.fail_detect, self.proxy, "failfix")

        # ******************************************************************************************************************

if __name__ == "__main__":
    set_context()

    h = create_host("http://192.168.1.112:6969")
    gms = h.spawn("gms1", "GMS/GMS")
    gms.run()
    print "GMS ready"

    serve_forever()
