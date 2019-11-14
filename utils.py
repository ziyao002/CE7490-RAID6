import os
import random
import string
import numpy as np


def GenRndFile(fname, size):
    content = "".join([random.choice(string.ascii_letters) for i in range(size)])
    # content = str(np.random.randint(255, size=size))
    fpath = os.path.join('', fname)
    with open(fpath, 'wt') as fh:
        fh.write(content)
