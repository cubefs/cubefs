import ctypes
import multiprocessing

from ctypes import Structure, POINTER, py_object, c_void_p


class PyList(POINTER(py_object)):
    pass


class CubeItemList(Structure):
    _fields_ = [('list_ptr', c_void_p)]


item = CubeItemList()

# 获取列表指针
plist = PyList.from_address(0)

# 转为c_void_p类型
item.list_ptr = ctypes.addressof(plist)

print("item.list_ptr is {}".format(item.list_ptr))
