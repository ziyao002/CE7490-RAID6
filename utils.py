import os
import random
import string
import codecs


def GenRndFile(fname, size):
    content = "".join([random.choice(string.ascii_letters) for i in range(size)])
    fpath = os.path.join('', fname)
    with codecs.open(fpath, 'w', 'utf-8') as fh:
        fh.write(content)
