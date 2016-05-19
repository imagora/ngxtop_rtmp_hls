import sys
import logging


def choose_one(choices, prompt):
    for idx, choice in enumerate(choices):
        print('%d. %s' % (idx + 1, choice))
    selected = None
    if sys.version[0] == '3':
        raw_input = input
    while not selected or selected <= 0 or selected > len(choices):
        selected = raw_input(prompt)
        try:
            selected = int(selected)
        except ValueError:
            selected = None
    return choices[selected - 1]


def trace(sequence, phase=''):
    for item in sequence:
        logging.debug('%s:\n%s', phase, item)
        yield item


def error_exit(msg, status=1):
    sys.stderr.write('Error: %s\n' % msg)
    sys.exit(status)


def to_int(value):
    return int(value) if value and value != '-' else 0


def to_float(value):
    return float(value) if value and value != '-' else 0.0
