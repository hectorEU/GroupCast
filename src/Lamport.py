import Member
from output import _print, _error
from pyactor.exceptions import TimeoutError


class Lamport(Member):
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
        self.leader_seq = max(self.leader_seq, seq)
        self.leader_seq += 1
        self.queue[self.leader_seq] = msg
        self.process_msg([self.leader_seq, msg])
