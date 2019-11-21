# CE7490-RAID6
A proof of concept of thread-based RAID4/5/6 implementation, supporting:

1. sequential write
2. random write
3. read with failure check
4. data rebuild

In simple_RAID_random_write.py, a simple demo of random writing for RAID4/RAID5 is given. Function time.sleep() is used to simulate the IO operation and Xor operation. Also, threads are used to simulate the parallel writing to random data blocks.
