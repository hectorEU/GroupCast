import sys
import unittest

from pyactor.context import set_context, create_host, sleep


class PeerTest(unittest.TestCase):
    def test_receive_request(self):
        set_context()
        h = create_host("http://192.168.1.112:7969")
        peer = h.spawn("1", "Peer/Peer")
        terminal = sys.stdout
        sys.stdout = open("test.txt", "w")
        peer.receive(5, "cinco")
        peer.receive(2, "dos")
        peer.receive(1, "uno")
        peer.receive(3, "tres")
        peer.receive(4, "cuatro")
        sleep(1)
        sys.stdout.close()
        sys.stdout = terminal
        with open("test.txt", "r") as file:
            line = file.read().replace("\n", "")
            self.assertEqual(line, "1 uno - 11 dos - 21 tres - 31 cuatro - 41 cinco - 5")

        peer.request()
        peer.request()
        peer.request()
        self.assertEqual(peer.getleader_seq(), 8)


if __name__ == '__main__':
    unittest.main()
