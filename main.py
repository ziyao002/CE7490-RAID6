import time
import raid4
import raid5
from utils import *
from configure import *


if __name__ == '__main__':

    # generate random file
    GenRndFile(TestFileName, DataSize)
    ### test Raid6 system
    Raid6 = raid6.RAID6(TestFileName, DiskNumber, DataSize, BlockSize)
    RndIndexDataForRaid6 = Raid6.GenRndIndexData()

    # test for RAID6 sequential writing
    SeqWriteStartTime = time.time()
    Raid6.SequentialWrite()
    SeqWriteStopTime = time.time()
    print("RAID6 sequential writing duration = ", SeqWriteStopTime - SeqWriteStartTime, "second")
    # test for RAID6 random writing
    # RndReadStartTime = time.time()
    # Raid6.RandomWrite(RndIndexDataForRaid6[0], RndIndexDataForRaid6[1], RndIndexDataForRaid6[2])
    # RndReadStopTime = time.time()
    # print("RAID6 random writing duration = ", RndReadStopTime - RndReadStartTime, "second")
    # test for RAID6 reading
    ReadStartTime = time.time()
    Raid6.read()
    ReadStopTime = time.time()
    print("RAID46reading duration = ", ReadStopTime - ReadStartTime, "second")

    # Recover for 2 Data Disks
    ErrorDiskList = [5,6]
    Raid6.RAID6rebuild(ErrorDiskList)


    Raid4 = raid4.RAID4(TestFileName, DiskNumber, DataSize, BlockSize)
    Raid5 = raid5.RAID5(TestFileName, DiskNumber, DataSize, BlockSize)
    RndIndexDataForRaid4 = Raid4.GenRndIndexData()
    RndIndexDataForRaid5 = Raid5.GenRndIndexData()

    # test for RAID4 sequential writing
    SeqWriteStartTime = time.time()
    Raid4.SequentialWrite()
    SeqWriteStopTime = time.time()
    print("RAID4 sequential writing duration = ", SeqWriteStopTime - SeqWriteStartTime, "second")
    # test for RAID4 random writing
    RndReadStartTime = time.time()
    Raid4.RandomWrite(RndIndexDataForRaid4[0], RndIndexDataForRaid4[1], RndIndexDataForRaid4[2])
    RndReadStopTime = time.time()
    print("RAID4 random writing duration = ", RndReadStopTime - RndReadStartTime, "second")
    # test for RAID4 reading
    ReadStartTime = time.time()
    Raid4.read()
    ReadStopTime = time.time()
    print("RAID4 reading duration = ", ReadStopTime - ReadStartTime, "second")
    # test for RAID4 rebuilding
    RebuildStartTime = time.time()
    ErrorDiskIndex = 2
    Raid4.rebuild(ErrorDiskIndex)
    RebuildStopTime = time.time()
    print("RAID4 rebuilding duration = ", RebuildStopTime - RebuildStartTime, "second", '\n')

    # test for RAID5 sequential writing
    SeqWriteStartTime = time.time()
    Raid5.SequentialWrite()
    SeqWriteStopTime = time.time()
    print("RAID5 sequential writing duration = ", SeqWriteStopTime - SeqWriteStartTime, "second")
    # test for RAID5 random writing
    RndReadStartTime = time.time()
    Raid5.RandomWrite(RndIndexDataForRaid5[0], RndIndexDataForRaid5[1], RndIndexDataForRaid5[2])
    RndReadStopTime = time.time()
    print("RAID5 random writing duration = ", RndReadStopTime - RndReadStartTime, "second")
    # test for RAID5 reading
    ReadStartTime = time.time()
    Raid5.read()
    ReadStopTime = time.time()
    print("RAID5 reading duration = ", ReadStopTime - ReadStartTime, "second")
    # test for RAID5 rebuilding
    RebuildStartTime = time.time()
    ErrorDiskIndex = 2
    Raid5.rebuild(ErrorDiskIndex)
    RebuildStopTime = time.time()
    print("RAID5 rebuilding duration = ", RebuildStopTime - RebuildStartTime, "second", '\n')
