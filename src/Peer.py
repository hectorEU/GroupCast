from random import sample, choice
from string import lowercase

from pyactor.context import set_context, serve_forever, create_host, sleep
from pyactor.exceptions import TimeoutError

from GMS import GMS
from output import _print, _error


class Peer(object):
    _tell = ["multicast", "receive", "process_msg", "election", "release", "leave", "coordinator", "sync",
             "show_history"]
    _ask = ["request", "getleader_seq", "join", "is_alive"]
    _ref = ["multicast", "receive", "process_msg", "election", "request", "getleader_seq", "join",
            "coordinator", "release", "is_alive", "leave", "sync", "show_history"]

    def __init__(self):
        self.history = {}
        self.queue = {}
        self.seq = 0
        self.leader_seq = 0
        self.election_members = 0
        self.waiting_msg = []
        self.sem = 0
        self.retry_timeout = 1

    # Leader functions *****************************************************************

    def request(self):
        self.leader_seq += 1
        _print(self, "Current sequence " + str(self.leader_seq))
        return self.leader_seq

    def getleader_seq(self):
        return self.leader_seq

    # Public actor methods *************************************************************

    def multicast(self, msg):
        members = False
        try:
            members = self.gms.get_members(self.proxy)
        except AttributeError:
            _error(self, "does not belong to any GMS")
            return
        try:
            if not members or self.gms.get_lock():
                raise ValueError

            if self.leader != self.proxy:
                _print(self, "Requesting sequence to member " + str(self.leader.get_id()))
                seq = self.leader.request()
            else:
                _print(self, "Requesting own sequence")
                seq = self.request()

            _print(self, "Sequence obtained")

            for peer in members:
                peer.receive(seq, msg)
            self.receive(seq, msg)

        except ValueError:
            self.waiting_msg.append(msg)
            _error(self, "Election in progress, queuing msg " + msg)

        except (TimeoutError, AttributeError) as e:
            self.waiting_msg.append(msg)
            _error(self, "Leader not working, queuing msg " + msg)
            if self.gms.election_lock():
                _error(self, "Starting election")
                self.election(members)

    def receive(self, seq, msg):
        # print self.leader
        # print self.id + " " + msg + " - " + str(seq) + str(self.seq) + "\n"
        self.queue[seq] = msg
        for item in self.queue.items():
            if self.seq + 1 == item[0]:
                self.queue.pop(item[0])
                self.process_msg(item)
                self.seq += 1
        self.leader_seq = max(self.leader_seq, seq)

    def election(self, members):
        self.leader = members[0]
        for member in members:
            if int(member.get_id()) > int(self.leader.get_id()):
                self.leader = member

        if int(self.id) > int(self.leader.get_id()):
            self.leader = self.proxy

        mem = list(members)
        mem.append(self.proxy)

        _print(self, "Choosing leader " + str(self.leader.get_id()))
        for member in members:
            try:
                member.coordinator(self.leader, mem)
            except TimeoutError:
                continue
        self.coordinator(self.leader, mem)

    def sync(self, seq, members):
        self.leader_seq = max(self.leader_seq, seq)
        self.sem += 1
        _print(self, "Leader acknowledge, sem " + str(self.sem))
        if len(members) - 1 == self.sem:
            self.sem = 0
            self.gms.election_unlock()
            _print(self, "Election finished, releasing messages")
            self.release()
            for member in members:
                member.release()

    def coordinator(self, leader, members):
        self.leader = leader
        if self.leader != self.proxy:
            self.leader.sync(self.leader_seq, members)

    def release(self):
        for msg in self.waiting_msg:
            _print(self, "Releasing " + msg)
            self.multicast(self.waiting_msg.pop())

    def is_alive(self):
        return True

    def join(self, gms):
        if isinstance(gms, basestring):
            self.gms = self.host.lookup_url(gms, GMS)
        else:
            self.gms = gms
        self.gms.join(self.proxy)

    def leave(self):
        try:
            self.gms.leave(self.proxy)
        except AttributeError:
            _error(self, "does not belong to any GMS")

    # Internal methods *****************************************************************

    def process_msg(self, msg):
        self.history[int(msg[0])] = msg[1]
        _print(self, msg[1] + " - " + str(msg[0]))

    def show_history(self):
        print(self.history)

        # **********************************************************************************


if __name__ == "__main__":
    set_context()
    h = create_host("http://192.168.1.112:9969")
    members = []
    gms = "http://192.168.1.112:6969/gms1"

    for i in xrange(20):
        member = h.spawn(str(i), "Peer/Peer")
        member.join(gms)
        members.append(member)

    for ite in xrange(100):
        print "New Iteration"
        for member in sample(members, 5):
            member.multicast(''.join(choice(lowercase) for i in range(10)))

    sleep(30)
    print "Printing history"

    for member in members:
        sleep(1)
        print "Id: " + str(member.get_id())
        member.show_history()

    serve_forever()
