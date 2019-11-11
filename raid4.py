import os
import numpy as np
import concurrent
import concurrent.futures
from itertools import repeat


class RAID4:
    def __init__(self, DiskNumber, FileName, DataSize):
        self.N = DiskNumber
        self.fname = FileName
        self.dsize = DataSize

    def Content2Array(self, content):
        ContentList = [[] for i in repeat(None, (self.N - 1))]
        ByteList = []
        # allocate the data to N-1 disks
        for i in range(len(content)):
            mod_i = i % (self.N-1)
            ContentList[mod_i].append(content[i])
        # map the data to unicode and fill 0
        MaxLen = len(sorted(ContentList, key=len, reverse=True)[0])
        for content in ContentList:
            CurrentStrList = [ord(s) for s in content] + [0] * (MaxLen - len(content))
            ByteList.append(CurrentStrList)
        # list to np.array
        ByteAarray = np.array(ByteList, dtype=np.int8)
        return ByteAarray

    def Array2Content(self, OneDArray):
        # delete 0
        OneDArrayNo0 = filter(lambda x: x >= 0, OneDArray)
        StrList = [chr(x) for x in OneDArrayNo0]
        return ''.join(StrList)

    def GenPartity(self, ContentArray):
        return np.bitwise_xor.reduce(ContentArray).reshape(1, -1)

    def GenWriteArray(self, ContentArray):
        parity = self.GenPartity(ContentArray)
        WriteArray = np.concatenate([ContentArray, parity])
        return WriteArray

    def GetPath(self, DiskIndex):
        DiskPath = os.path.join('RAID4', 'Disk' + str(DiskIndex))
        if not os.path.isdir(DiskPath):
            os.makedirs(DiskPath)
        return os.path.join(DiskPath, self.fname)

    def Write2Disk(self, FilePath, OneDArray):
        OneDStr = self.Array2Content(OneDArray.reshape(-1, 1))
        with open(FilePath, 'wt') as fh:
            fh.write(OneDStr)

    def ReadFromDisk(self, FilePath):
        with open(FilePath, 'rt') as fh:
            return fh.read()

    def ParallelWrite(self, WriteArray):
        FilePathList = [self.GetPath(i) for i in range(self.N)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.N) as executor:
            executor.map(self.Write2Disk, FilePathList, WriteArray)

    def ParallelRead(self):
        FilePathList = [self.GetPath(i) for i in range(self.N)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.N) as executor:
            ContentList = list(executor.map(self.ReadFromDisk, FilePathList))
        ByteList = []
        for content in ContentList:
            CurrentStrList = [ord(s) for s in content]
            ByteList.append(CurrentStrList)
        ByteAarray = np.array(ByteList, dtype=np.int8)
        return ByteAarray

    def write(self):
        with open(self.fname, 'rt') as fh:
            content = fh.read()
        ByteNDArray = self.Content2Array(content)
        WriteArray = self.GenWriteArray(ByteNDArray)
        self.ParallelWrite(WriteArray)

    def read(self):
        ByteArray = self.ParallelRead()
        self.Datacheck(ByteArray)
        DataArray = ByteArray[:-1]
        FlatList = DataArray.ravel(1)[:self.dsize]
        FlatStrList = [chr(x) for x in FlatList]
        return ''.join(FlatStrList)

    def Datacheck(self, ByteNDArray):
        CheckArray = np.bitwise_xor.reduce(ByteNDArray).reshape(1, -1)
        if np.count_nonzero(CheckArray) != 0:
            raise Exception("RAID4 Check Fails!")

    def rebuild(self, ErrorDiskIndex):
        ByteArray = self.ParallelRead()
        ByteArrayForRebuild = np.delete(ByteArray, ErrorDiskIndex, axis=0)
        RebuildArray = np.bitwise_xor.reduce(ByteArrayForRebuild).reshape(1, -1)
        self.Write2Disk(self.GetPath(ErrorDiskIndex), RebuildArray)
