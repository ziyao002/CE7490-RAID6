import time
import raid4
from utils import *
from configure import *


if __name__ == '__main__':

    # generate random file
    GenRndFile(TestFileName, DataSize)

    # RAID4 system initiation
    Raid4 = raid4.RAID4(TestFileName, DiskNumber, DataSize, BlockSize)

    # test for RAID4 sequential writing
    SeqWriteStartTime = time.time()
    Raid4.SequentialWrite()
    SeqWriteStopTime = time.time()
    print("Sequential writing time for RAID4 against", TestFileName, "=", SeqWriteStopTime - SeqWriteStartTime, "second")

    # test for RAID4 random reading
    RndIndexData = Raid4.GenRndIndexData(DataSize)
    RndReadStartTime = time.time()
    Raid4.RandomWrite(RndIndexData[0], RndIndexData[1], RndIndexData[2])
    RndReadStopTime = time.time()
    print("Random writing time for RAID4 against", TestFileName, "=", RndReadStopTime - RndReadStartTime, "second")

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


