import os
from subprocess import Popen, PIPE, STDOUT
import time
import sys
import signal


TWEET_COUNT = 55
DELAY = 2

p = Popen('/bin/nc -lk 9999', shell=True, bufsize=1024*64, stdin=PIPE, stdout=sys.stdout)


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    if p is not None:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    sys.exit(0)

# registering CTRL-C handler
signal.signal(signal.SIGINT, signal_handler)


def nextNtweets(n, file):
    buf = []
    try:
        for _ in range(n):
            tweet = next(file)
            buf.append(tweet)
    except StopIteration:
        if len(buf) == 0:
            return None
    return buf

with open("/home/vagrant/wspace/data/isis_tiny.data", "r") as f:
    tweets = nextNtweets(TWEET_COUNT, f)
    while tweets is not None:
        lines = "\n".join(tweets)
        p.stdin.write(b"str")
        print("OK. line count: {0}".format(len(tweets)))
        time.sleep(DELAY)
        tweets = nextNtweets(TWEET_COUNT, f)

# as we use shell, we need to kill all the group (e.g. shell and the process itself)
os.killpg(os.getpgid(p.pid), signal.SIGTERM)
p.wait()

