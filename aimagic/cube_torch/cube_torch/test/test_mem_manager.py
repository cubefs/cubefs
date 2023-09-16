import multiprocessing
import os
import unittest
from multiprocessing import Process, Lock

from cube_torch import get_manager
from cube_torch.mem_manager import MemoryManager


class MemoryManagerTests(unittest.TestCase):

    def setUp(self):
        self.manager = MemoryManager(1000)
        manager=get_manager()
        manager.mem_manager=self.manager


    def tearDown(self):
        self.manager = None

    def test_allocate_single_block(self):
        offset, size = self.manager.allocate(200)
        self.assertEqual(0,offset)
        self.assertEqual(200,size)

    def test_allocate_multiple_blocks(self):
        offset1, size1 = self.manager.allocate(200)
        offset2, size2 = self.manager.allocate(300)
        offset3, size3 = self.manager.allocate(150)

        self.assertEqual(0,offset1)
        self.assertEqual(200,offset2)
        self.assertEqual(500,offset3)

        self.assertEqual(200,size1)
        self.assertEqual(300,size2)
        self.assertEqual(150,size3)

    def test_allocate_expanded_block(self):
        offset1, size1 = self.manager.allocate(200)
        offset2, size2 = self.manager.allocate(500)

        self.assertEqual(0,offset1)
        self.assertEqual(200,offset2)

        self.assertEqual(200,size1)
        self.assertEqual(500,size2)

    def test_allocate_insufficient_memory(self):
        offset1, size1 = self.manager.allocate(1000)
        offset2, size2 = self.manager.allocate(200)

        self.assertEqual(0,offset1)
        self.assertIsNone(offset2)

        self.assertEqual(1000,size1)
        self.assertIsNone(size2)

    def test_free_single_block(self):
        offset, size = self.manager.allocate(200)
        self.manager.free(offset, size)

        self.assertEqual(1000,self.manager.total_free_memory)

    def test_free_multiple_blocks(self):
        offset1, size1 = self.manager.allocate(200)
        offset2, size2 = self.manager.allocate(300)
        offset3, size3 = self.manager.allocate(150)

        self.manager.free(offset1, size1)
        self.manager.free(offset2, size2)
        self.manager.free(offset3, size3)

        self.assertEqual(1000,self.manager.total_free_memory)

    def test_defragmentation(self):
        offset1, size1 = self.manager.allocate(200)
        offset2, size2 = self.manager.allocate(300)
        offset3, size3 = self.manager.allocate(150)

        self.manager.free(offset1, size1)
        self.manager.free(offset2, size2)

        offset4, size4 = self.manager.allocate(400)

        self.assertEqual(0,offset4)
        self.assertEqual(400,size4)
        self.assertEqual(450,self.manager.total_free_memory)

    def test_concurrent_allocation(self):
        ctx = multiprocessing.get_context("fork")
        process=[]
        for i in range(10):
            w=ctx.Process(target=self.allocate_in_parallel)
            w.daemon=True
            w.start()
            process.append(w)

        for w in process:
            w.join()

        self.assertEqual(0,self.manager.total_free_memory)

    def allocate_in_parallel(self):
        mem_manager = get_manager().mem_manager
        offset1, size1 = mem_manager.allocate(100)
        self.assertIsNotNone(offset1)
        self.assertIsNotNone(size1)


    def test_concurrent_free(self):
        offset1, size1 = self.manager.allocate(200)
        offset2, size2 = self.manager.allocate(300)
        ctx = multiprocessing.get_context("fork")
        process = []
        for i in range(10):
            w = ctx.Process(target=self.free_in_parallel,args=(offset1,size1,offset2,size2))
            w.daemon = True
            w.start()
            process.append(w)

        for w in process:
            w.join()
        self.assertEqual(1000,self.manager.total_free_memory)

    def free_in_parallel(self, offset1, size1, offset2, size2):
        self.manager.free(offset1, size1)
        self.manager.free(offset2, size2)
        print("pid:{} free_in_parallel offset1:{} size1:{} offset2:{} size2:{}".format(os.getpid(),offset1,size1,offset2,size2))

    def test_fragmentation(self):
        offset1, size1 = self.manager.allocate(200)
        offset2, size2 = self.manager.allocate(300)
        self.manager.free(offset1, size1)

        offset3, size3 = self.manager.allocate(250)
        offset4, size4 = self.manager.allocate(150)

        self.assertEqual(500,offset3)
        self.assertEqual(0,offset4)

        self.assertEqual(250,size3)
        self.assertEqual(150,size4)

        self.assertEqual(300,self.manager.total_free_memory)


if __name__ == '__main__':
    unittest.main()