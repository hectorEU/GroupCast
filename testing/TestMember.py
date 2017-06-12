import sys
import unittest

from pyactor.context import set_context, create_host, sleep


class MyTestCase(unittest.TestCase):
    def test_something(self):
        set_context()
        h = create_host("http://localhost:7969")
        peer = h.spawn("1", "src.Member/Member")
        terminal = sys.stdout
        sys.stdout = open("test.txt", "w")
        peer.receive(5, "cinco")
        peer.receive(2, "dos")
        peer.receive(1, "uno")  # Stdout -> uno, dos
        peer.receive(3, "tres")  # Stdout -> tres
        peer.receive(4, "cuatro")  # Stdout -> cuatro, cinco
        sleep(1)
        sys.stdout.close()
        sys.stdout = terminal
        with open("test.txt", "r") as file:
            line = file.read().replace("\n", "")
            self.assertEqual(line, "1 uno - 11 dos - 21 tres - 31 cuatro - 41 cinco - 5")  # Test TOM

        peer.request()
        peer.request()
        peer.request()
        self.assertEqual(peer.getleader_seq(), 8)


if __name__ == '__main__':
    unittest.main()
