from random import sample, choice
from string import lowercase

from Member import Member
from output import _print, _error
from pyactor.context import set_context, serve_forever, create_host, sleep
from pyactor.exceptions import TimeoutError


class Lamport(Member):  # Extends from Member, inherits all functions, variables...

    # Internal methods *************************************************************************************************

    def request(self):
        self.leader_seq += int(self.id)  # Different clock speeds
        _print(self, "Current clock " + str(self.leader_seq))
        return self.leader_seq

    # Public actor methods *********************************************************************************************

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

            seq = self.request()  # There is no sequencer in Lamport

            for peer in members:
                peer.receive(seq, msg)
            self.process_msg([self.leader_seq, msg])

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
        current_seq = self.request()
        if seq >= current_seq:
            self.leader_seq = seq + 1
            _error(self, "Clock forwarded to >> " + str(self.leader_seq))
        self.process_msg([self.leader_seq, msg])

        # ******************************************************************************************************************


if __name__ == "__main__":
    set_context()
    h = create_host("http://192.168.1.112:7969")
    members = []
    gms = "http://192.168.1.112:6969/gms1"

    # Create members and join GMS
    for i in xrange(20):
        member = h.spawn(str(i), "Lamport/Lamport")
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
        member.show_history()  # Show POM

    serve_forever()
