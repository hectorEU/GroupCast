

class Sequencer(object):

    def __init__(self):
        self.id = 0

    def request(self):
        self.id += 1
        return self.id

