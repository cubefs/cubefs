import ctypes
import multiprocessing


def bytes_to_c_void_p(byte_array, shared_memory):
    shared_memory[:len(byte_array)] = byte_array


def c_void_p_to_bytes(shared_memory, size):
    byte_array = shared_memory[:size]
    return byte_array


def process_a(byte_array, shared_memory):
    bytes_to_c_void_p(byte_array, shared_memory)
    print("Process A: shared_memory =:", shared_memory[0:100])


def process_b(shared_memory, size):
    byte_array = c_void_p_to_bytes(shared_memory, size)
    print("Process B: byte_array =", bytes(byte_array))


if __name__ == '__main__':
    byte_array = b'Hello, World!'
    size = len(byte_array)

    # Create shared memory for c_void_p object
    shared_memory = multiprocessing.RawArray(ctypes.c_byte, 100 * 12)
    print(shared_memory[0:])

    # Start Process A
    p1 = multiprocessing.Process(target=process_a, args=(byte_array, shared_memory))
    p1.start()
    p1.join()

    # Start Process B
    p2 = multiprocessing.Process(target=process_b, args=(shared_memory, size))
    p2.start()
    p2.join()
