import sys


def _print(object, string):
    print object.id + " " + string


def _error(object, string):
    print >> sys.stderr, object.id + " " + string
