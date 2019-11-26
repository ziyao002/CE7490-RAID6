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
from pyfinite.ffield import FField


class RAID6:
    def __init__(self, InputFileName, DiskNumber, DataSize, BlockSize):
        self.N = DiskNumber
        self.infname = InputFileName
        self.dsize = DataSize
        self.bsize = BlockSize
        self.MaxStripeIndex = int(math.ceil(math.ceil(DataSize / (DiskNumber - 2)) / BlockSize))
        self.DiskLockList = [0] * DiskNumber
        self.ParityLockFlag = 0
        self.StripeLockList = []
        self.CountNum = 0
        self.F = FField(8)
        # Q coefficient, if DiskNumber = 8, coefficient is: 1,2,4,8,16,32,64,128
        self.Q_coef = np.array([pow(2,i) for i in range(DiskNumber-2)])

    def Content2ArrayBlock(self, content):
        # return ContentArray(DiskIndex, BlockIndex, DataIndex)
        self.StripeLockList = [0] * self.MaxStripeIndex
        ContentArray = np.zeros((self.N - 2, self.MaxStripeIndex, self.bsize), dtype=np.uint8)
        # allocate the data to the block in N-2 disks
        for i in range(len(content)):
            mod_i = i // self.bsize % (self.N - 2)      # Disk Index
            mod_j = i // self.bsize // (self.N - 2)     # Block Index
            mod_k = i % self.bsize                      # Data Index
            ContentArray[mod_i][mod_j][mod_k] = ord(content[i])
            #debug print("content[i] = ", content[i], "ord(content[i]) = ", ord(content[i]))
        #debug print("ContentArray = ", ContentArray)
        return ContentArray

    def Array2Content(self, OneDArray):
        # delete 0
        OneDArrayNo0 = filter(lambda x: x >= 0, OneDArray)
        StrList = [chr(x) for x in OneDArrayNo0]
        return ''.join(StrList)

    def GenXor(self, ContentArray):
        return np.bitwise_xor.reduce(ContentArray).reshape(1, ContentArray.shape[1], ContentArray.shape[2])

    def GetQByte(self, OldDataByte, NewDataByte, OldQByte, DiskIndex, BlockIndex, PDiskIndex, QDiskIndex):
        '''
            # obtain Qcoef_index
            case 1: D D D D P Q
            case 2: D D D P Q D
            case 3: P Q D D D D
            case 4: Q D D D D P
        '''
        # need to consider the case the DiskIndex is swapped with P or Q disk drive during writing
        POldIndex = ParityDiskIndex_RAID6
        QOldIndex = ParityDiskIndex_RAID4

        # case 1
        QCoef_index = DiskIndex
        # case 2/3:
        # if DiskIndex == POldIndex:
        #     QCoef_index = PDiskIndex
        # # case 4:
        # if DiskIndex == QOldIndex and BlockIndex%self.N == 1:
        #     QCoef_index = PDiskIndex
        # else:
        #     QCoef_index = QDiskIndex
        # print("QCoef_index = ",QCoef_index)
        # Q_new = Q_old XOR (Dn_new*gn) XOR (Dn_old*gn)

        # PIndex = ((self.N - 1 - BlockIndex) % self.N - 1) % self.N
        # QIndex = (self.N - 1 - BlockIndex) % self.N
        #
        # if DiskIndex < PIndex and PIndex < QIndex:
        #     QCoef_index = DiskIndex
        # elif DiskIndex < PIndex and QIndex < PIndex:
        #     QCoef_index = DiskIndex - 1
        # else:
        #     QCoef_index = DiskIndex - 2

        print("DiskIndex ", DiskIndex, "PDiskIndex ", PDiskIndex, "QDiskIndex ", QDiskIndex, "BlockIndex ",BlockIndex)
        NewQByte = [np.bitwise_xor(np.bitwise_xor(self.F.Multiply(self.Q_coef[QCoef_index],NewDataByte[i]),self.F.Multiply(self.Q_coef[QCoef_index],OldDataByte[i])), OldQByte[i]) for i in range(self.bsize)]
        return NewQByte

    def GetQArray(self, ContentArray):
        # step 1: D0*g0+D1*g1+...+Dn*gn
        NewContentArray = np.zeros((self.N - 2, self.MaxStripeIndex, self.bsize), dtype=np.uint8)
        for i in range(ContentArray.shape[0]):
            for j in range(ContentArray.shape[1]):
                for k in range(ContentArray.shape[2]):
                    NewContentArray[i][j][k] = self.F.Multiply(ContentArray[i][j][k],self.Q_coef[i])
        # step 2: bitwise XOR
        return np.bitwise_xor.reduce(NewContentArray).reshape(1, NewContentArray.shape[1], NewContentArray.shape[2])

    def GetQArray_withcoef(self, ContentArray, coef):
        # step 1: D0*g0+D1*g1+...+Dn*gn
        NewContentArray = np.zeros((ContentArray.shape[0], ContentArray.shape[1], ContentArray.shape[2]), dtype=np.uint8)
        for i in range(ContentArray.shape[0]):
            for j in range(ContentArray.shape[1]):
                for k in range(ContentArray.shape[2]):
                    NewContentArray[i][j][k] = self.F.Multiply(ContentArray[i][j][k],coef[i])
        # step 2: bitwise XOR
        return np.bitwise_xor.reduce(NewContentArray).reshape(1, NewContentArray.shape[1], NewContentArray.shape[2])  

    def SwapParity(self, WriteArray):
        for i in range(self.MaxStripeIndex):
            POldIndex = ParityDiskIndex_RAID6
            PnewIndex = ((self.N - 1 - i) % self.N - 1) % self.N
            QOldIndex = ParityDiskIndex_RAID4
            QnewIndex = (self.N - 1 - i) % self.N
            # swap P
            WriteArray[[PnewIndex, POldIndex], i, :] = WriteArray[[POldIndex, PnewIndex], i, :]
            # swap Q
            WriteArray[[QnewIndex, QOldIndex], i, :] = WriteArray[[QOldIndex, QnewIndex], i, :] 
        return WriteArray

    def RecoverParity(self, WriteArray):
        for i in range(self.MaxStripeIndex):
            POldIndex = ParityDiskIndex_RAID6
            PnewIndex = ((self.N - 1 - i) % self.N - 1) % self.N
            QOldIndex = ParityDiskIndex_RAID4
            QnewIndex = (self.N - 1 - i) % self.N
            # special case: DDDPQD -> DDDDPQ
            if i % self.N == 1:
                # swap P - D
                WriteArray[[PnewIndex, QOldIndex], i, :] = WriteArray[[QOldIndex, PnewIndex], i, :]
                # swap P - Q
                WriteArray[[POldIndex, QOldIndex], i, :] = WriteArray[[QOldIndex, POldIndex], i, :] 
            else:
                # swap P
                WriteArray[[PnewIndex, POldIndex], i, :] = WriteArray[[POldIndex, PnewIndex], i, :]
                # swap Q
                WriteArray[[QnewIndex, QOldIndex], i, :] = WriteArray[[QOldIndex, QnewIndex], i, :] 
        return WriteArray
    
    # recover parity for a stripe, assume StripIndex is known
    def RecoverParityStrip(self, InputArray, StripIndex):
        WriteArray = InputArray
        POldIndex = ParityDiskIndex_RAID6
        PnewIndex = ((self.N - 1 - StripIndex) % self.N - 1) % self.N
        QOldIndex = ParityDiskIndex_RAID4
        QnewIndex = (self.N - 1 - StripIndex) % self.N
        # special case: DDDPQD -> DDDDPQ
        if StripIndex % self.N == 1:
            # swap P - D
            WriteArray[[PnewIndex, QOldIndex], :, :] = WriteArray[[QOldIndex, PnewIndex], :, :]
            # swap P - Q
            WriteArray[[POldIndex, QOldIndex], :, :] = WriteArray[[QOldIndex, POldIndex], :, :] 
        else:
            # swap P
            #print("WriteArray ",WriteArray)
            #print("WriteArray[[PnewIndex, POldIndex], :, :] = ",WriteArray[PnewIndex, :, :])
            WriteArray[[PnewIndex, POldIndex], :, :] = WriteArray[[POldIndex, PnewIndex], :, :]
            # print("WriteArray[[PnewIndex, POldIndex], :, :] = ",WriteArray[PnewIndex, :, :])
            ## swap Q
            WriteArray[[QnewIndex, QOldIndex], :, :] = WriteArray[[QOldIndex, QnewIndex], :, :] 
        return WriteArray

    def GenWriteArray(self, ContentArray):
        PArray = self.GenXor(ContentArray)
        QArray = self.GetQArray(ContentArray)
        ConcArray = np.concatenate([ContentArray, PArray, QArray])
        # print("ConcArray = ",ConcArray[:,2,:])
        WriteArray = self.SwapParity(ConcArray)
        # print("WriteArray = ",WriteArray[:,2,:])
        #ReturnWriteArray = self.RecoverParity(WriteArray)
        #print("ReturnWriteArray = ",ReturnWriteArray)   
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

        #debug print("FilePath = ", FilePath, "OneDArray = ", OneDArray, "OneDStr = ", OneDStr)

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
        # print("WriteArray[:,2,:] = ",WriteArray[:,2,:])
        # print("write array = ",WriteArray)
        FilePathList = []
        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxStripeIndex):
                FilePathList.append(self.GetPath(DiskIndex, BlockIndex))

        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxStripeIndex):
                t = Thread(target=self.SeqWrite2Disk, args=(
                FilePathList[DiskIndex * self.MaxStripeIndex + BlockIndex], WriteArray[DiskIndex][BlockIndex], DiskIndex))
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
        #debug print("OldDataByte: {}\n NewDataByte: {}\n OldPByte: {} \n OldQByte: {} \n".format(len(OldDataByte),len(NewDataByte),len(OldPByte),len(OldQByte)))
        NewPByte = np.bitwise_xor(np.bitwise_xor(NewDataByte, OldDataByte), OldPByte)
        NewPStr = ''.join([chr(x) for x in NewPByte])
        NewQByte = self.GetQByte(OldDataByte, NewDataByte, OldQByte, DiskIndex, BlockIndex, PDiskIndex, QDiskIndex)
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
        for i in range(self.MaxStripeIndex * self.N):
            t = Thread(target=self.RndWrite2Disk, args=(DiskIndexList[i], BlockIndexList[i], NewDataArray[i]))
            t.start()
        while any(self.StripeLockList):
            pass
        self.CountNum = 0

    def ParallelRead(self):
        FilePathList = []
        for DiskIndex in range(self.N):
            for BlockIndex in range(self.MaxStripeIndex):
                FilePathList.append(self.GetPath(DiskIndex, BlockIndex))
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.N) as executor:
            ContentList = list(executor.map(self.ReadFromDisk, FilePathList, list(range(self.N)) * self.MaxStripeIndex))

        DiskByteList = []
        for i in range(self.N):
            BlockByteList = []
            for j in range(self.MaxStripeIndex):
                ContentStrList = ContentList[i * self.MaxStripeIndex + j]
                BlockByteList.append([ord(s) for s in ContentStrList])
            DiskByteList.append(BlockByteList)
        ByteNDArray = np.array(DiskByteList, dtype=np.uint8)
        return ByteNDArray

    def read(self):
        ByteArray = self.ParallelRead()
        # print("ByteArray = ",ByteArray)
        self.DataCheck(ByteArray)
        DataArray = ByteArray[:-1]
        FlatList = []
        for i in range(self.MaxStripeIndex):
            DataStripList = (DataArray[:, i].reshape(1, -1)).tolist()
            FlatList = FlatList + DataStripList[0]
        FlatStrList = [chr(x) for x in FlatList]
        return ''.join(FlatStrList)

    def DataCheck(self, ByteNDArray):
        # place P,Q parity disk to last 2 columns
        NewByteNDArray = self.RecoverParity(ByteNDArray)
        # check P parity
        CheckArray_P = np.bitwise_xor.reduce(NewByteNDArray[:-1])
        NewQArray = self.GetQArray(NewByteNDArray[:-2]).reshape(self.MaxStripeIndex,self.bsize)
        CheckArray_Q = np.bitwise_xor(NewQArray,NewByteNDArray[-1])
        #debug print("ByteNDArray = ", ByteNDArray)

        if np.count_nonzero(CheckArray_P) != 0 or np.count_nonzero(CheckArray_Q) != 0:
            raise Exception("RAID6 Check Fails!")

    def RAID6rebuild(self, ErrorDiskList):
        # array written to file
        ByteArray = self.ParallelRead()
        # print("error disk content: ",ByteArray[ErrorDiskList])
        # P, Q at last 2 disks
        # RecoverNDArray = self.RecoverParity(ByteArray)
        RebuildArray = np.zeros((2, self.MaxStripeIndex, self.bsize), dtype=np.uint8)
        # D for 0, P for 1, Q for 2
        RebuildStripeArray = np.zeros((self.MaxStripeIndex,2), dtype=np.uint8)
        for i in range(self.MaxStripeIndex):
            P_disk = self.GetParityDiskIndex(ErrorDiskList[0],i,'P')
            Q_disk = self.GetParityDiskIndex(ErrorDiskList[1],i,'Q')
            # if disk is P then assign 1, Q assign 2, data assign 0
            if P_disk in ErrorDiskList:
                P_index = ErrorDiskList.index(P_disk)
                RebuildStripeArray[i][P_index] = 1
            if Q_disk in ErrorDiskList:
                Q_index = ErrorDiskList.index(Q_disk)
                RebuildStripeArray[i][Q_index] = 2
        # print("RebuildStripeArray = ", RebuildStripeArray)
        # for i in range(self.MaxStripeIndex):
        for i in range(self.MaxStripeIndex):
            # case 1: rebuild P, Q disk
            if RebuildStripeArray[i][0] == 1 and RebuildStripeArray[i][1] == 2:
                # P_old = ByteArray[ErrorDiskList[0]][i]
                # Q_old = ByteArray[ErrorDiskList[1]][i]
                # get data strip and reshape to 3 dimension in order to pass to GenXor and GetQArray function
                DataStrip = ByteArray[:,i,:].reshape(self.N,1,self.bsize)
                # put P,Q disk to end of disk
                RecoveredStrip = self.RecoverParityStrip(DataStrip,i) # RecoveredStrip shape (10, 1, 4)
                DataStrip = ByteArray[:,i,:].reshape(self.N,1,self.bsize)
                PArray = self.GenXor(RecoveredStrip[:-2])
                QArray = self.GetQArray_withcoef(RecoveredStrip[:-2],self.Q_coef)
                RebuildArray[0][i] = PArray.reshape(self.bsize)
                RebuildArray[1][i] = QArray.reshape(self.bsize)
                # print("i is ",i,"RebuildArray[0][i]",RebuildArray[0][i],"RebuildArray[1][i]",RebuildArray[1][i])
            # case 2: Single data drive and Q drive
            if RebuildStripeArray[i][0] == 0 and RebuildStripeArray[i][1] == 2:
                DataStrip = ByteArray[:,i,:].reshape(self.N,1,self.bsize)
                RecoveredStrip = self.RecoverParityStrip(DataStrip,i) # RecoveredStrip shape (10, 1, 4)
                # remove data disk and Q disk
                Strip_No_DQ = np.delete(RecoveredStrip,[ErrorDiskList[0],self.N-1],axis=0)
                # gen new Data array
                New_DArray = self.GenXor(Strip_No_DQ)
                # replace error data disk with recovered data
                RecoveredStrip[ErrorDiskList[0]] = New_DArray.reshape(self.bsize)
                New_QArray = self.GetQArray_withcoef(RecoveredStrip[:-2],self.Q_coef)
                RebuildArray[0][i] = New_DArray.reshape(self.bsize)
                RebuildArray[1][i] = New_QArray.reshape(self.bsize)
                # print("i is ",i,"RebuildArray[0][i]",RebuildArray[0][i],"RebuildArray[1][i]",RebuildArray[1][i])
            # case 3: Single data drive and P drive    
            if RebuildStripeArray[i][0] == 0 and RebuildStripeArray[i][1] == 1:  
                DataStrip = ByteArray[:,i,:].reshape(self.N,1,self.bsize)
                RecoveredStrip = self.RecoverParityStrip(DataStrip,i) # RecoveredStrip shape (10, 1, 4)
                DataStrip_noD = np.delete(RecoveredStrip,ErrorDiskList[0],axis=0)
                new_coef = np.delete(self.Q_coef,ErrorDiskList[0],axis=0)
                Q_prime = self.GetQArray_withcoef(DataStrip_noD[:-2],new_coef).reshape(self.bsize)
                # Q XOR Q_prime
                New_DArray = np.bitwise_xor(Q_prime,RecoveredStrip[-1]).reshape(self.bsize)
                # inverse coefficient
                G_inv = self.F.DoInverseForSmallField(self.Q_coef[ErrorDiskList[0]])
                for j in range(self.bsize):
                    New_DArray[j] = self.F.Multiply(G_inv,New_DArray[j])
                RecoveredStrip[ErrorDiskList[0]] = New_DArray
                New_PArray = self.GenXor(RecoveredStrip[:-2]).reshape(1,self.bsize)
                RebuildArray[0][i] = New_DArray
                RebuildArray[1][i] = New_PArray 
                # print("i is ",i,"RebuildArray[0][i]",RebuildArray[0][i],"RebuildArray[1][i]",RebuildArray[1][i])
            # case 4: 2 data array failed              
            if RebuildStripeArray[i][0] == 0 and RebuildStripeArray[i][1] == 0:  
                # print("i is ",i)
                DataStrip = ByteArray[:,i,:].reshape(self.N,1,self.bsize)
                # put P,Q disk to end of disk
                RecoveredStrip = self.RecoverParityStrip(DataStrip,i) # RecoveredStrip shape (10, 1, 4)
                POldIndex = ParityDiskIndex_RAID6
                PnewIndex = ((self.N - 1 - i) % self.N - 1) % self.N
                QOldIndex = ParityDiskIndex_RAID4
                QnewIndex = (self.N - 1 - i) % self.N   
                # if data disk locate at old P/Q diskindex, means they were swapped, need to get their original index
                DataDisk1_index = ErrorDiskList[0]
                DataDisk2_index = ErrorDiskList[1]
                if ErrorDiskList[0] == POldIndex:
                    DataDisk1_index = PnewIndex
                if ErrorDiskList[0] == QOldIndex:
                    DataDisk1_index = QnewIndex
                if ErrorDiskList[1] == POldIndex:
                    DataDisk2_index = PnewIndex
                if ErrorDiskList[1] == QOldIndex:
                    DataDisk2_index = QnewIndex    
                # print("Q_coef[DataDisk1_index] = ",self.Q_coef[DataDisk1_index],"Q_coef[DataDisk2_index] = ",self.Q_coef[DataDisk2_index])
                G1_G2_inv = self.F.DoInverseForSmallField(self.F.Add(self.Q_coef[DataDisk1_index],self.Q_coef[DataDisk2_index]))
                # remove 2 data disk
                Strip_No_DD = np.delete(RecoveredStrip,[DataDisk1_index,DataDisk2_index],axis=0)
                # remove data disk coef
                new_coef = np.delete(self.Q_coef,[DataDisk1_index,DataDisk2_index],axis=0)
                # print("Strip_No_DD = ",Strip_No_DD, "RecoveredStrip = ",RecoveredStrip,"DataStrip = ",DataStrip)
                # get p_prime without error disks
                P_prime = self.GenXor(Strip_No_DD[:-2]).reshape(1,self.bsize)
                P_P_prime_XOR = np.bitwise_xor(RecoveredStrip[-2],P_prime).reshape(self.bsize)
                # print("P = ",RecoveredStrip[-2],"P_prime = ",P_prime,"P_P_prime_XOR = ",P_P_prime_XOR)
                P_P_prime_XOR_x_G2 = np.zeros((self.bsize), dtype=np.uint8)
                for j in range(self.bsize):
                    P_P_prime_XOR_x_G2[j] = self.F.Multiply(self.Q_coef[DataDisk2_index],P_P_prime_XOR[j])
                # print("P_P_prime_XOR_x_G2 =",P_P_prime_XOR_x_G2,"P_P_prime_XOR",P_P_prime_XOR)
                Q_prime = self.GetQArray_withcoef(Strip_No_DD[:-2],new_coef).reshape(self.bsize)
                # print("Q_prime = ",Q_prime,"Strip_No_DD[:-2] = ",Strip_No_DD[:-2],"new_coef = ",new_coef)
                # SecTerm = np.bitwise_xor(P_P_prime_XOR,RecoveredStrip[-1].reshape(self.bsize),Q_prime)
                SecTerm1 = np.bitwise_xor(P_P_prime_XOR_x_G2,RecoveredStrip[-1].reshape(self.bsize))
                SecTerm = np.bitwise_xor(SecTerm1,Q_prime)
                New_D1Array = np.zeros((self.bsize), dtype=np.uint8)
                for j in range(self.bsize):
                    New_D1Array[j] = self.F.Multiply(G1_G2_inv,SecTerm[j])
                # rebuild D2
                New_D2Array = np.bitwise_xor(New_D1Array,P_P_prime_XOR)
                RebuildArray[0][i] = New_D1Array
                RebuildArray[1][i] = New_D2Array 
                # print("i is ",i,"RebuildArray[0][i]",RebuildArray[0][i],"RebuildArray[1][i]",RebuildArray[1][i])
        # re build array
        # print(RebuildArray)
        for strip in range(self.MaxStripeIndex):
            self.SeqWrite2Disk(self.GetPath(ErrorDiskList[0], strip), RebuildArray[0][strip], ErrorDiskList[0])
            self.SeqWrite2Disk(self.GetPath(ErrorDiskList[1], strip), RebuildArray[1][strip], ErrorDiskList[1])

    def rebuild(self, ErrorDiskList):
        ByteArray = self.ParallelRead()
        ByteArrayForRebuild = np.delete(ByteArray, [ErrorDiskList[0], ErrorDiskList[1]], axis=0)
        RebuildArray = self.RAID6rebuild(ByteArrayForRebuild)
        for BlockIndex in range(self.MaxStripeIndex):
            self.SeqWrite2Disk(self.GetPath(ErrorDiskList[0], BlockIndex), RebuildArray[0][BlockIndex], ErrorDiskList[0])
            self.SeqWrite2Disk(self.GetPath(ErrorDiskList[1], BlockIndex), RebuildArray[1][BlockIndex], ErrorDiskList[1])

    def GenRndIndexData(self):
        StripeIndexList = list(np.random.randint(self.MaxStripeIndex, size=self.MaxStripeIndex * self.N))

        DiskIndexList = []
        for x in StripeIndexList:
            PIndex = ((self.N - 1 - x) % self.N - 1) % self.N
            QIndex = (self.N - 1 - x) % self.N
            DiskIndex = np.random.randint(0, self.N - 2)
            while DiskIndex == PIndex or DiskIndex == QIndex:
                DiskIndex = np.random.randint(0, self.N - 2)
            DiskIndexList.append(DiskIndex)
        print(StripeIndexList)
        print(DiskIndexList)

        NewDataArray = [''.join([random.choice(string.ascii_letters) for i in range(self.bsize)]) for j in range(self.MaxStripeIndex * self.N)]
        return [DiskIndexList, StripeIndexList, NewDataArray]
