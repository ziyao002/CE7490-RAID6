from threading import Thread
import time
import numpy as np

# RAID parameters
DiskNumber = 10
DataSize = 100

# Here BlockSize = 1
DiskList = list(np.random.randint(DiskNumber - 1, size=DataSize))
BlockList = [DiskNumber - x % DiskNumber for x in DiskList]

# Disk: allows only one IO operation (write/read)
# Stripe: allows only one random write
DiskLockList = [0] * DiskNumber
StripeLockList = [0] * len(BlockList)

# IO simulation parameters
WriteIOTime = 0.005
ReadIOTime = 0.003
XorTime = 0.0001


def Write2Disk(DiskIndex, BlockIndex):
    # Lock disk
    global DiskLockList, WriteIOTime
    while DiskLockList[DiskIndex]:
        pass
    DiskLockList[DiskIndex] = 1
    # Write data to a file
    time.sleep(WriteIOTime)
    # Release disk
    DiskLockList[DiskIndex] = 0


def ReadFromDisk(DiskIndex, BlockIndex):
    # Lock disk
    global DiskLockList, ReadIOTime
    while DiskLockList[DiskIndex]:
        pass
    DiskLockList[DiskIndex] = 1
    # Read data from a file
    time.sleep(ReadIOTime)
    # Release disk
    DiskLockList[DiskIndex] = 0


def RndWrite(DiskIndex, BlockIndex, RaidType):
    # Lock stripe
    global DiskNumber, StripeLockList, XorTime
    while StripeLockList[BlockIndex]:
        pass
        StripeLockList[BlockIndex] = 1
    # Get index of parity disk
    if RaidType == 'RAID4':
        ParityDiskIndex = DiskNumber - 1
    elif RaidType == 'RAID5':
        ParityDiskIndex = (DiskNumber - 1 - BlockIndex) % DiskNumber
    else:
        raise TypeError("RAID Type Error")
    # Two read
    ReadFromDisk(DiskIndex, BlockIndex)
    ReadFromDisk(ParityDiskIndex, BlockIndex)
    # Get new parity data
    time.sleep(XorTime)
    # Two write
    Write2Disk(DiskIndex, BlockIndex)
    Write2Disk(ParityDiskIndex, BlockIndex)
    # Release stripe
    StripeLockList[BlockIndex] = 0


# Test for RAID4
StartTime = time.time()
# Generate thread pool
for i in range(len(BlockList)):
    t = Thread(target=RndWrite, args=(DiskList[i], BlockList[i], 'RAID4'))
    t.start()
# Check if all the thread are finished
while any(StripeLockList):
    pass
StopTime = time.time()
print("Raid 4 random write duration = ", StopTime - StartTime)

# Test for RAID5
StartTime = time.time()
# Generate thread pool
for i in range(len(BlockList)):
    t = Thread(target=RndWrite, args=(DiskList[i], BlockList[i], 'RAID5'))
    t.start()
# Check if all the thread are finished
while any(StripeLockList):
    pass
StopTime = time.time()
print("Raid 5 random write duration = ", StopTime - StartTime)
