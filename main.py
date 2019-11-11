import time
import utils
import raid4


if __name__ == '__main__':
    # generate random file and read as "content"
    FileName = 'data0'
    DiskNumber = 10
    DataSize = 1000000
    utils.GenRndFile(FileName, DataSize)

    # RAID4 system
    Raid4 = raid4.RAID4(DiskNumber, FileName, DataSize)

    # test for RAID4 writing
    WriteStartTime = time.time()
    Raid4.write()
    WriteStopTime = time.time()
    print("Writing time for RAID4 against", FileName, "=", WriteStopTime - WriteStartTime, "Second")

    # test for RAID4 reading
    ReadStartTime = time.time()
    Raid4.read()
    ReadStopTime = time.time()
    print("Reading time for RAID4 against", FileName, "=", ReadStopTime - ReadStartTime, "Second")

    # test for RAID4 rebuilding
    RebuildStartTime = time.time()
    ErrorDiskIndex = 2
    Raid4.rebuild(ErrorDiskIndex)
    RebuildStopTime = time.time()
    print("Rebuilding time for RAID4 against", FileName, "=", RebuildStopTime - RebuildStartTime, "Second")

