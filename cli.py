#!/usr/bin/env python
# coding: utf8

import sys, curses
import re, time, datetime

from core import Tracer


def extract_ipv4(lines):
    for l in lines:
        m = re.search(r'(\d+\.\d+\.\d+\.\d+)', l)
        if m:
            yield m.group(1)


def main(stdscr, inputs):
    stdscr.refresh()  # to avoid getch() clear whole screen
    ips = list(extract_ipv4(inputs.split('\n')))

    t = Tracer()

    def on_tick(tracer):
        stdscr.addstr(0, 0, datetime.datetime.now().strftime('%F %T'))
        stdscr.refresh()

    t.on_tick(on_tick)
    t.run(ips)

    return

    pad = curses.newpad(100, 100)
    #  These loops fill the pad with letters; this is
    # explained in the next section
    for y in range(0, 100):
        for x in range(0, 100):
            try:
                pad.addch(y, x, ord('a') + (x*x+y*y) % 26)
            except curses.error:
                pass

    #  Displays a section of the pad in the middle of the screen
    y, x = stdscr.getmaxyx()

    curses.napms(1000)
    while 1:
        c = stdscr.getch()
        curses.napms(50)

        if c == ord('q'):
            exit(0)


if '__main__' == __name__:
    if len(sys.argv) > 1:
        curses.wrapper(main, sys.argv[1])
        # main(None, sys.argv[1])
    else:
        print 'Usage: %s ip' % sys.argv[0]
