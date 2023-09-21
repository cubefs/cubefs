import os
import threading


class MemoryAllocater:
    def __init__(self, total_memory,batch_download_workers, batch_download_notify_queues, free_memory_queue):
        self._total_memory = total_memory
        self.allocate_memory_threads = []
        self.mem_manager = MemoryManager(total_memory)
        self.allocate_memory_event = []
        self.batch_download_workers = batch_download_workers
        self.batch_download_notify_queues = batch_download_notify_queues
        for index in range(batch_download_workers):
            e = threading.Event()
            input_queue = self.batch_download_notify_queues[index][0]
            out_queue = self.batch_download_notify_queues[index][1]
            t = threading.Thread(target=self.background_allocate_memory, args=(index,input_queue,out_queue, e))
            t.daemon = True
            t.start()
            self.allocate_memory_threads.append(t)
            self.allocate_memory_event.append(e)
        e = threading.Event()
        t = threading.Thread(target=self.background_free_memory, args=(free_memory_queue,e))
        t.daemon = True
        t.start()
        self.allocate_memory_threads.append(t)
        self.allocate_memory_event.append(e)

    def background_allocate_memory(self, batch_download_idx,input_queue,out_queue, event):
        while not event.is_set():
            request = input_queue.get()
            if len(request)==5:
                worker_idx, file_path, free_offset, size,is_free=request
                self.mem_manager.free(free_offset,size)
                continue
            worker_idx, file_path, write_size = request
            write_offset, size = self.mem_manager.allocate(write_size)
            if write_offset is None:
                response = (worker_idx, file_path, None, None)
            else:
                response = (worker_idx, file_path, write_offset, size)
            out_queue.put(response)

    def background_free_memory(self,free_item_meta_queue,event):
        while not event.is_set():
            request = free_item_meta_queue.get()
            actual_file_path, free_offset, size=request
            self.mem_manager.free(free_offset,size)



class MemoryManager:
    def __init__(self, total_memory):
        self.lock = threading.Lock()
        self.memory = [{'offset': 0, 'size': total_memory, 'used': False}]
        self.total_memory = total_memory
        self.total_free_memory = total_memory

    def allocate(self, size):
        with self.lock:
            block = self._first_fit(size)
            if block and block['size'] >= size:
                if block['size'] > size:
                    remaining_block = {
                        'offset': block['offset'] + size,
                        'size': block['size'] - size,
                        'used': False
                    }
                    self.memory.insert(self.memory.index(block) + 1, remaining_block)
                    block['size'] = size
                block['used'] = True
                self.total_free_memory -= size
                return block['offset'], block['size']

            self._defragment()
            block = self._first_fit(size)

            if block and block['size'] >= size:
                if block['size'] > size:
                    remaining_block = {
                        'offset': block['offset'] + size,
                        'size': block['size'] - size,
                        'used': False
                    }
                    self.memory.insert(self.memory.index(block) + 1, remaining_block)
                    block['size'] = size
                block['used'] = True
                self.total_free_memory -= size
                return block['offset'], block['size']

            return None, None

    def free(self, offset, size):
        with self.lock:
            block = self._block_containing(offset)
            if block and block['offset'] == offset and block['size'] == size and block['used']:
                block['used'] = False
                self.total_free_memory += size
                self._defragment()

    def _first_fit(self, size):
        for block in self.memory:
            if not block['used'] and block['size'] >= size:
                return block
        return None

    def _block_containing(self, offset):
        for block in self.memory:
            if block['offset'] <= offset < block['offset'] + block['size']:
                return block
        return None

    def _defragment(self):
        self.memory.sort(key=lambda b: b['offset'])
        i = 0
        while i < len(self.memory) - 1:
            current = self.memory[i]
            next_block = self.memory[i + 1]
            if not current['used'] and not next_block['used']:
                current['size'] += next_block['size']
                self.memory.pop(i + 1)
            else:
                i += 1
