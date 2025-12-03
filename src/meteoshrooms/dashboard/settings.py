import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--debug', action='store_true')


def get_args():
    return parser.parse_args()
