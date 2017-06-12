from random import sample, choice
from string import lowercase

from GMS import GMS
from output import _print, _error
from pyactor.context import set_context, serve_forever, create_host, sleep
from pyactor.exceptions import TimeoutError


class Member(object):
    _tell = ["multicast", "receive", "process_msg", "election", "release", "leave", "coordinator", "sync",
             "show_history"]
    _ask = ["request", "getleader_seq", "join", "is_alive"]
    _ref = ["multicast", "receive", "process_msg", "election", "request", "getleader_seq", "join",
            "coordinator", "release", "is_alive", "leave", "sync", "show_history"]

    def __init__(self):
        self.history = {}  # Key -> Sequence : Value -> Message. Message record for testing purposes
        self.queue = {}  # Key -> Sequence : Value -> Message. Queue to maintain totally ordered multicast processing
        self.seq = 0  # Last processed message sequence
        self.leader_seq = 0  # Sequencer sequence number, synchronized with every member in GMS
        self.waiting_msg = []  # Pending messages to be multicasted
        self.sem = 0  # Election semaphore

    # Leader (Sequencer) methods ***************************************************************************************

    def request(self):
        self.leader_seq += 1
        _print(self, "Leader sequence " + str(self.leader_seq))
        return self.leader_seq

    # Query leader sequence
    def getleader_seq(self):
        return self.leader_seq

    # Public actor methods *********************************************************************************************
    # "join" is in _ask because it has to finish before any other call such as "multicast, leave..."
    def join(self, gms):
        if isinstance(gms, basestring):
            self.gms = self.host.lookup_url(gms, GMS)  # If gms is GMS url string, look it up
        else:
            self.gms = gms  # Else assign it as a proxy
        self.gms.join(self.proxy)

    def leave(self):
        try:
            self.gms.leave(self.proxy)
        except AttributeError:
            _error(self, "does not belong to any GMS")  # self.gms NULL means it has not joined any GMS

    def multicast(self, msg):
        members = False
        try:
            members = self.gms.get_members(self.proxy)
        except AttributeError:
            _error(self, "does not belong to any GMS")  # self.gms NULL means it has not joined any GMS
            return
        try:
            if not members or self.gms.get_lock():
                raise ValueError
                # members = False if current member is not in the group yet or there is an election taking place

            if self.leader != self.proxy:  # A member cannot establish remote communication with itself
                _print(self, "Requesting sequence to leader " + str(self.leader.get_id()))
                seq = self.leader.request()
            else:
                seq = self.request()  # Note: self.leader.request() vs self.request()
                _print(self, "Requested own sequence")

            _print(self, "Sequence obtained " + str(seq))

            for peer in members:
                peer.receive(seq, msg)  # Multicast sequence and message
            self.receive(seq, msg)  # Also to itself

        except ValueError:
            # Manually raised exception in line 59

            self.waiting_msg.append(msg)
            _error(self, "Election in progress, queuing msg " + msg)

        except (TimeoutError, AttributeError) as e:
            # Raised exception by calling self.leader.request() if the leader does not answer or exist line 64

            self.waiting_msg.append(msg)
            _error(self, "Leader not working properly, queuing msg " + msg)
            if self.gms.election_lock():  # If there is no election, start election
                _error(self, "Starting election")
                self.election(members)

    def receive(self, seq, msg):
        self.queue[seq] = msg
        for item in self.queue.items():
            if self.seq + 1 == item[0]:
                self.queue.pop(item[0])
                self.process_msg(item)  # Process message when all previous messages arrived, ordered by sequence
                self.seq += 1
        self.leader_seq = max(self.leader_seq, seq)  # Update leader sequence

    def election(self, members):
        self.leader = members[0]
        for member in members:
            if int(member.get_id()) > int(self.leader.get_id()):
                self.leader = member

        if int(self.id) > int(self.leader.get_id()):
            self.leader = self.proxy

        # At this point, self.leader has the proxy object of the highest ID member

        mem = list(members)
        mem.append(self.proxy)

        # mem = temporal list of all members in the group

        _print(self, "Choosing leader " + str(self.leader))
        for member in members:
            try:
                member.coordinator(self.leader, mem)
            except TimeoutError:
                continue
        self.coordinator(self.leader, mem)

    def coordinator(self, leader, members):
        self.leader = leader
        if self.leader != self.proxy:
            self.leader.sync(self.leader_seq, members)  # The new leader must get the highest leader_seq from all

    def sync(self, seq, members):
        self.leader_seq = max(self.leader_seq, seq)
        self.sem += 1
        _print(self, "Leader acknowledge, sem " + str(self.sem))
        if len(members) - 1 == self.sem:
            self.sem = 0
            self.gms.election_unlock()  # All members have agreed this is the leader, finish election
            _print(self, "Election finished, releasing messages")
            self.release()  # Release own pending messages for multicast
            for member in members:
                member.release()  # Release their pending messages for multicast

    def release(self):
        for msg in self.waiting_msg:
            _print(self, "Releasing " + msg)
            self.waiting_msg.remove(msg)
            self.multicast(msg)

    def is_alive(self):
        return True  # Method for GMS to decide status of member

    # Internal methods *************************************************************************************************

    def process_msg(self, msg):
        self.history[int(msg[0])] = msg[1]
        _print(self, msg[1] + " - " + str(msg[0]))

    def show_history(self):
        print(sorted(self.history.items()))

        # ******************************************************************************************************************


if __name__ == "__main__":
    set_context()
    h = create_host("http://192.168.1.112:7969")
    members = []
    gms = "http://192.168.1.112:6969/gms1"

    # Create members and join GMS
    for i in xrange(20):
        member = h.spawn(str(i), "Member/Member")
        member.join(gms)
        members.append(member)

    for ite in xrange(100):
        print "New Iteration"
        for member in sample(members, 5):  # Each 5 random members send a multicast message of 10 random chars
            member.multicast(''.join(choice(lowercase) for i in range(10)))

    sleep(30)
    print "Printing history"

    for member in members:
        sleep(1)
        print "Id: " + str(member.get_id())
        member.show_history()  # Show TOM

    serve_forever()
