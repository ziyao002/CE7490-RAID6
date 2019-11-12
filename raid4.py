import os
import math
import numpy as np
import concurrent
from concurrent.futures import ThreadPoolExecutor, wait
from configure import *


class RAID4:
    def __init__(self, InputFileName, DiskNumber, DataSize, BlockSize):
        self.N = DiskNumber
        self.infname = InputFileName
        self.dsize = DataSize
        self.bsize = BlockSize
        self.MaxBlockIndex = 0

    def Content2ArrayBlock(self, content):
        # return ContentArray(DiskIndex, BlockIndex, DataIndex)
        self.MaxBlockIndex = int(math.ceil(math.ceil(len(content)/(self.N-1))/self.bsize))
        ContentArray = np.zeros((self.N - 1, self.MaxBlockIndex, self.bsize), dtype=np.int8)
        # allocate the data to the block in N-1 disks
        for i in range(len(content)):
            mod_i = i // self.bsize % (self.N - 1)      # Disk Index
            mod_j = i // self.bsize // (self.N - 1)     # Block Index
            mod_k = i % self.bsize                      # Data Index
            ContentArray[mod_i][mod_j][mod_k] = ord(content[i])
        return ContentArray

    def Array2Content(self, OneDArray):
        # delete 0
        OneDArrayNo0 = filter(lambda x: x >= 0, OneDArray)
        StrList = [chr(x) for x in OneDArrayNo0]
        return ''.join(StrList)

    def GenXor(self, ContentArray):
        return np.bitwise_xor.reduce(ContentArray).reshape(1, ContentArray.shape[1], ContentArray.shape[2])

    def GenWriteArray(self, ContentArray):
        parity = self.GenXor(ContentArray)
        WriteArray = np.concatenate([ContentArray, parity])
        return WriteArray

    def GetPath(self, DiskIndex, BlockIndex):
        DiskPath = os.path.join('RAID4', 'Disk' + str(DiskIndex))
        if not os.path.isdir(DiskPath):
            os.makedirs(DiskPath)
        return os.path.join(DiskPath, 'Block' + str(BlockIndex))

    def Write2Disk(self, FilePath, OneDArray, ParityIndex):

        global ParityDiskBlockFlag
        while ParityDiskBlockFlag:
            pass
        if FilePath.split('\\')[1] == ('Disk'+str(ParityIndex)):
            ParityDiskBlockFlag = 1
            OneDStr = self.Array2Content(OneDArray.reshape(-1, 1))
            with open(FilePath, 'wt') as fh:
                fh.write(OneDStr)
            ParityDiskBlockFlag = 0
        else:
            OneDStr = self.Array2Content(OneDArray.reshape(-1, 1))
            with open(FilePath, 'wt') as fh:
                fh.write(OneDStr)

    def ReadFromDisk(self, FilePath):
        with open(FilePath, 'rt') as fh:
            return fh.read()

    def ParallelWrite(self, WriteArray):
        FilePathList = []
        fs = []
        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxBlockIndex):
                FilePathList.append(self.GetPath(DiskIndex, BlockIndex))

        pool = ThreadPoolExecutor(max_workers=self.N)
        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxBlockIndex):
                fs.append(pool.submit(self.Write2Disk, FilePathList[DiskIndex * self.MaxBlockIndex + BlockIndex], WriteArray[DiskIndex][BlockIndex], ParityIndex))
        wait(fs)
        pool.shutdown()

    def ParallelRead(self):
        FilePathList = []
        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxBlockIndex):
                FilePathList.append(self.GetPath(DiskIndex, BlockIndex))
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.N) as executor:
            ContentList = list(executor.map(self.ReadFromDisk, FilePathList))
        DiskByteList = []
        for i in range(self.N):
            BlockByteList = []
            for j in range(self.MaxBlockIndex):
                ContentStrList = ContentList[i * self.MaxBlockIndex + j]
                BlockByteList.append([ord(s) for s in ContentStrList])
            DiskByteList.append(BlockByteList)
        ByteNDArray = np.array(DiskByteList, dtype=np.int8)
        return ByteNDArray

    def SequentialWrite(self):
        with open(self.infname, 'rt') as fh:
            content = fh.read()
        ByteNDArray = self.Content2ArrayBlock(content)
        WriteArray = self.GenWriteArray(ByteNDArray)
        self.ParallelWrite(WriteArray)

    def read(self):
        ByteArray = self.ParallelRead()
        self.Datacheck(ByteArray)
        DataArray = ByteArray[:-1]
        FlatList = []
        for i in range(self.MaxBlockIndex):
            DataStripList = (DataArray[:, i].reshape(1, -1)).tolist()
            FlatList = FlatList + DataStripList[0]
        FlatStrList = [chr(x) for x in FlatList]
        return ''.join(FlatStrList)

    def Datacheck(self, ByteNDArray):
        CheckArray = np.bitwise_xor.reduce(ByteNDArray).reshape(1, -1)
        if np.count_nonzero(CheckArray) != 0:
            raise Exception("RAID4 Check Fails!")

    def rebuild(self, ErrorDiskIndex):
        ByteArray = self.ParallelRead()
        ByteArrayForRebuild = np.delete(ByteArray, ErrorDiskIndex, axis=0)
        RebuildArray = np.bitwise_xor.reduce(ByteArrayForRebuild)
        for BlockIndex in range(self.MaxBlockIndex):
            self.Write2Disk(self.GetPath(ErrorDiskIndex, BlockIndex), RebuildArray[BlockIndex], ParityIndex)

