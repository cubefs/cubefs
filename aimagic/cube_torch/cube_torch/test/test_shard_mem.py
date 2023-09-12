import ctypes
import multiprocessing


def string_to_c_void_p(input_string):
    byte_array = input_string.encode()
    shared_memory = multiprocessing.Array(ctypes.c_byte, len(byte_array))
    byte_array_ct = ctypes.create_string_buffer(byte_array)
    ctypes.memmove(shared_memory.get_obj(), byte_array_ct, len(byte_array))
    return ctypes.cast(shared_memory.get_obj(), ctypes.c_void_p)


def bytes_to_c_void_p(byte_array):
    shared_memory = multiprocessing.Array(ctypes.c_byte, len(byte_array))
    byte_array_ct = ctypes.create_string_buffer(byte_array)
    ctypes.memmove(shared_memory.get_obj(), byte_array_ct, len(byte_array))
    return ctypes.cast(shared_memory.get_obj(), ctypes.c_void_p)


def c_void_p_to_bytes(c_void_p, size):
    ptr_type = ctypes.POINTER(ctypes.c_byte * size)
    ptr = ctypes.cast(c_void_p, ptr_type)
    byte_array = bytes(ptr.contents)

    return byte_array


def c_void_p_to_string(c_void_p, size):
    ptr_type = ctypes.POINTER(ctypes.c_byte * size)
    ptr = ctypes.cast(c_void_p, ptr_type)
    byte_array = bytes(ptr.contents).decode()

    return byte_array


if __name__ == "__main__":
    byte_array = b'nihaoa'
    c_voidp = bytes_to_c_void_p(byte_array)
    print("cvoid_p addr is :{}".format(c_voidp))
    print("cvoid_p addr is :{}".format(type(c_voidp)))


    def process_function(c_void_p, size):
        print("cvoid_p addr is :{}".format(c_voidp))
        byte_array = c_void_p_to_string(c_void_p, size)
        print("Received byte array:", byte_array)


    process = multiprocessing.Process(target=process_function, args=(c_voidp, len(byte_array),))
    process.start()
    process.join()
