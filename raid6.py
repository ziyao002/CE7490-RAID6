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


class RAID6:
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

    def GetQByte(self, OldDataByte, OldPByte, OldQByte):
        return OldQByte

    def GetQArray(self, ContentArray):
        return ContentArray[0]

    def SwapParity(self, WriteArray):
        for i in range(self.MaxBlockIndex):
            POldIndex = ParityDiskIndex_RAID4 - 1
            PnewIndex = ((self.N - 1 - i) % self.N - 1) % self.N
            QOldIndex = ParityDiskIndex_RAID4
            QnewIndex = (self.N - 1 - i) % self.N
            WriteArray[[POldIndex, QOldIndex, PnewIndex, QnewIndex], i, :] = WriteArray[[PnewIndex, QnewIndex, POldIndex, QOldIndex], i, :]
        return WriteArray

    def GenWriteArray(self, ContentArray):
        PArray = self.GenXor(ContentArray)
        QArray = self.GetQArray(ContentArray)
        ConcArray = np.concatenate([ContentArray, PArray, QArray])
        WriteArray = self.SwapParity(ConcArray)
        return WriteArray

    def GetPath(self, DiskIndex, BlockIndex):
        DiskPath = os.path.join('RAID6', 'Disk' + str(DiskIndex))
        if not os.path.isdir(DiskPath):
            os.makedirs(DiskPath)
        return os.path.join(DiskPath, 'Block' + str(BlockIndex))

    def GetParityDiskIndex(self, DiskIndex, BlockIndex, ParityType):
        if ParityType == 'P':
            return ((self.N - 1 - BlockIndex) % self.N - 1) % self.N
        elif ParityType == 'Q':
            return (self.N - 1 - BlockIndex) % self.N
        else:
            raise Exception("ParityType Error!")

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

        PDiskIndex = self.GetParityDiskIndex(DiskIndex, BlockIndex, 'P')
        QDiskIndex = self.GetParityDiskIndex(DiskIndex, BlockIndex, 'Q')
        DataPath = self.GetPath(DiskIndex, BlockIndex)
        PPath = self.GetPath(PDiskIndex, BlockIndex)
        QPath = self.GetPath(QDiskIndex, BlockIndex)

        # Three read
        OldDataByte = [ord(x) for x in self.ReadFromDisk(DataPath, DiskIndex)]
        OldPByte = [ord(x) for x in self.ReadFromDisk(PPath, PDiskIndex)]
        OldQByte = [ord(x) for x in self.ReadFromDisk(QPath, QDiskIndex)]

        # Xor
        NewDataByte = [ord(x) for x in NewData]
        NewPByte = np.bitwise_xor(np.bitwise_xor(np.bitwise_xor(NewDataByte, OldDataByte), OldPByte), OldQByte)
        NewPStr = ''.join([chr(x) for x in NewPByte])
        NewQByte = self.GetQByte(OldDataByte, OldPByte, OldQByte)
        NewQStr = ''.join([chr(x) for x in NewQByte])

        # Three write
        self.Write2Disk(DataPath, NewData, DiskIndex)
        self.Write2Disk(PPath, NewPStr, PDiskIndex)
        self.Write2Disk(QPath, NewQStr, QDiskIndex)

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
            raise Exception("RAID6 Check Fails!")

    def RAID6rebuild(self, ByteArrayForRebuild):
        return ByteArrayForRebuild[0:2]

    def rebuild(self, ErrorDiskIndex0, ErrorDiskIndex1):
        ByteArray = self.ParallelRead()
        ByteArrayForRebuild = np.delete(ByteArray, [ErrorDiskIndex0, ErrorDiskIndex1], axis=0)
        RebuildArray = self.RAID6rebuild(ByteArrayForRebuild)
        for BlockIndex in range(self.MaxBlockIndex):
            self.SeqWrite2Disk(self.GetPath(ErrorDiskIndex0, BlockIndex), RebuildArray[BlockIndex], ErrorDiskIndex0)
            self.SeqWrite2Disk(self.GetPath(ErrorDiskIndex1, BlockIndex), RebuildArray[BlockIndex], ErrorDiskIndex1)

    def GenRndIndexData(self):
        DiskIndexList = list(np.random.randint(self.N, size=self.MaxBlockIndex * self.N))
        BlockIndexList = [(random.randint(0, self.MaxBlockIndex // self.N) * self.N + self.N - 2 - x) % self.MaxBlockIndex for x in DiskIndexList]
        NewDataArray = [''.join([random.choice(string.ascii_letters) for i in range(self.bsize)]) for j in range(self.MaxBlockIndex * self.N)]
        return [DiskIndexList, BlockIndexList, NewDataArray]
