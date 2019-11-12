import time
import raid4
from utils import *
from configure import *


if __name__ == '__main__':
    # generate random file
    GenRndFile(TestFileName, DataSize)

    # RAID4 system initiation
    Raid4 = raid4.RAID4(TestFileName, DiskNumber, DataSize, BlockSize)

    # test for RAID4 writing
    WriteStartTime = time.time()
    Raid4.SequentialWrite()
    WriteStopTime = time.time()
    print("Sequential writing time for RAID4 against", TestFileName, "=", WriteStopTime - WriteStartTime, "second")

    # test for RAID4 reading
    ReadStartTime = time.time()
    Raid4.read()
    ReadStopTime = time.time()
    print("Reading time for RAID4 against", TestFileName, "=", ReadStopTime - ReadStartTime, "second")

    # test for RAID4 rebuilding
    RebuildStartTime = time.time()
    ErrorDiskIndex = 2
    Raid4.rebuild(ErrorDiskIndex)
    RebuildStopTime = time.time()
    print("Rebuilding time for RAID4 against", TestFileName, "=", RebuildStopTime - RebuildStartTime, "second")

