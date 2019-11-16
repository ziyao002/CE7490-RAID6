import os
import math
import random
import string
import codecs
import time
import numpy as np
from threading import Thread
import threading
import concurrent
from concurrent.futures import ThreadPoolExecutor, wait
from configure import *


class RAID5:
    def __init__(self, InputFileName, DiskNumber, DataSize, BlockSize):
        self.N = DiskNumber
        self.infname = InputFileName
        self.dsize = DataSize
        self.bsize = BlockSize
        self.MaxBlockIndex = int(math.ceil(math.ceil(DataSize / (DiskNumber - 1)) / BlockSize))
        self.DiskLockList = [0] * DiskNumber
        self.ParityLockFlag = 0
        self.StripeLockList = []
        self.CountNum = 0

    def Content2ArrayBlock(self, content):
        # return ContentArray(DiskIndex, BlockIndex, DataIndex)
        self.StripeLockList = [0] * self.MaxBlockIndex
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

    def SwapParity(self, WriteArray):
        for i in range(self.MaxBlockIndex):
            WriteArray[[(self.N - 1 - i) % self.N, ParityDiskIndex_RAID4], i, :] = WriteArray[[ParityDiskIndex_RAID4, (self.N - 1 - i) % self.N], i, :]
        return WriteArray

    def GenWriteArray(self, ContentArray):
        parity = self.GenXor(ContentArray)
        ConcArray = np.concatenate([ContentArray, parity])
        WriteArray = self.SwapParity(ConcArray)
        return WriteArray

    def GetPath(self, DiskIndex, BlockIndex):
        DiskPath = os.path.join('RAID5', 'Disk' + str(DiskIndex))
        if not os.path.isdir(DiskPath):
            os.makedirs(DiskPath)
        return os.path.join(DiskPath, 'Block' + str(BlockIndex))

    def GetParityDiskIndex(self, DiskIndex, BlockIndex):
        return (self.N - 1 - BlockIndex) % self.N

    def Write2Disk(self, FilePath, OneDStr, DiskIndex):
        # Lock Disk
        while self.DiskLockList[DiskIndex]:
            pass
        self.DiskLockList[DiskIndex] = 1
        with codecs.open(FilePath, 'w', 'utf-8') as fh:
            fh.write(OneDStr)
        # Release Disk
        self.DiskLockList[DiskIndex] = 0

    def SeqWrite2Disk(self, FilePath, OneDArray, DiskIndex):
        # Lock Disk
        while self.DiskLockList[DiskIndex]:
            pass
        self.DiskLockList[DiskIndex] = 1

        # Write Data
        OneDStr = self.Array2Content(OneDArray.reshape(-1, 1))
        with codecs.open(FilePath, 'w', 'utf-8') as fh:
            fh.write(OneDStr)

        # Release Disk
        self.DiskLockList[DiskIndex] = 0

    def ReadFromDisk(self, FilePath, DiskIndex):
        # Lock Disk
        while self.DiskLockList[DiskIndex]:
            pass
        self.DiskLockList[DiskIndex] = 1
        with codecs.open(FilePath, 'r', 'utf-8') as fh:
            # Release Disk
            content = fh.read()
            self.DiskLockList[DiskIndex] = 0
            return content

    def SequentialWrite(self):
        with codecs.open(self.infname, 'r', 'utf-8') as fh:
            content = fh.read()
        ByteNDArray = self.Content2ArrayBlock(content)
        WriteArray = self.GenWriteArray(ByteNDArray)

        FilePathList = []
        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxBlockIndex):
                FilePathList.append(self.GetPath(DiskIndex, BlockIndex))

        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxBlockIndex):
                t = Thread(target=self.SeqWrite2Disk, args=(
                FilePathList[DiskIndex * self.MaxBlockIndex + BlockIndex], WriteArray[DiskIndex][BlockIndex], DiskIndex))
                t.start()
                t.join()

    def RndWrite2Disk(self, DiskIndex, BlockIndex, NewData):

        # Lock stripe
        while self.StripeLockList[BlockIndex]:
            pass
        self.StripeLockList[BlockIndex] = 1

        ParityDiskIndex = self.GetParityDiskIndex(DiskIndex, BlockIndex)
        DataPath = self.GetPath(DiskIndex, BlockIndex)
        ParityPath = self.GetPath(ParityDiskIndex, BlockIndex)

        # Two read
        OldDataByte = [ord(x) for x in self.ReadFromDisk(DataPath, DiskIndex)]
        OldParityByte = [ord(x) for x in self.ReadFromDisk(ParityPath, ParityDiskIndex)]

        # Xor
        NewDataByte = [ord(x) for x in NewData]
        NewParityByte = np.bitwise_xor(np.bitwise_xor(NewDataByte, OldDataByte), OldParityByte)
        NewParity = ''.join([chr(x) for x in NewParityByte])

        # Two write
        self.Write2Disk(DataPath, NewData, DiskIndex)
        self.Write2Disk(ParityPath, NewParity, ParityDiskIndex)

        # Release ParityBlock
        # self.CountNum += 1
        # print(self.CountNum)
        self.StripeLockList[BlockIndex] = 0

    def RandomWrite(self, DiskIndexList, BlockIndexList, NewDataArray):
        for i in range(self.MaxBlockIndex * self.N):
            t = Thread(target=self.RndWrite2Disk, args=(DiskIndexList[i], BlockIndexList[i], NewDataArray[i]))
            t.start()
        while any(self.StripeLockList):
            pass
        self.CountNum = 0

    def ParallelRead(self):
        FilePathList = []
        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxBlockIndex):
                FilePathList.append(self.GetPath(DiskIndex, BlockIndex))
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.N) as executor:
            ContentList = list(executor.map(self.ReadFromDisk, FilePathList, list(range(self.N)) * self.MaxBlockIndex))

        DiskByteList = []
        for i in range(self.N):
            BlockByteList = []
            for j in range(self.MaxBlockIndex):
                ContentStrList = ContentList[i * self.MaxBlockIndex + j]
                BlockByteList.append([ord(s) for s in ContentStrList])
            DiskByteList.append(BlockByteList)
        ByteNDArray = np.array(DiskByteList, dtype=np.int8)
        return ByteNDArray

    def read(self):
        ByteArray = self.ParallelRead()
        self.DataCheck(ByteArray)
        DataArray = ByteArray[:-1]
        FlatList = []
        for i in range(self.MaxBlockIndex):
            DataStripList = (DataArray[:, i].reshape(1, -1)).tolist()
            FlatList = FlatList + DataStripList[0]
        FlatStrList = [chr(x) for x in FlatList]
        return ''.join(FlatStrList)

    def DataCheck(self, ByteNDArray):
        CheckArray = np.bitwise_xor.reduce(ByteNDArray)
        if np.count_nonzero(CheckArray) != 0:
            raise Exception("RAID5 Check Fails!")

    def rebuild(self, ErrorDiskIndex):
        ByteArray = self.ParallelRead()
        ByteArrayForRebuild = np.delete(ByteArray, ErrorDiskIndex, axis=0)
        RebuildArray = np.bitwise_xor.reduce(ByteArrayForRebuild)
        for BlockIndex in range(self.MaxBlockIndex):
            self.SeqWrite2Disk(self.GetPath(ErrorDiskIndex, BlockIndex), RebuildArray[BlockIndex], ErrorDiskIndex)

    def GenRndIndexData(self):
        DiskIndexList = list(np.random.randint(self.N, size=self.MaxBlockIndex * self.N))
        BlockIndexList = [(random.randint(0, self.MaxBlockIndex // self.N) * self.N + self.N - 2 - x) % self.MaxBlockIndex for x in DiskIndexList]
        NewDataArray = [''.join([random.choice(string.ascii_letters) for i in range(self.bsize)]) for j in range(self.MaxBlockIndex * self.N)]
        return [DiskIndexList, BlockIndexList, NewDataArray]
