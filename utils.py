import os
import random
import string


def GenRndFile(fname, size):
    content = "".join([random.choice(string.ascii_letters) for i in range(size)])
    fpath = os.path.join('', fname)
    with open(fpath, 'wt') as fh:
        fh.write(content)

