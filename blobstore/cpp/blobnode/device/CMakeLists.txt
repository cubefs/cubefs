set(src
     "kernel_device.cc"
     "spdk_device.cc"
)

add_library(OBJS OBJECT ${src})
target_link_libraries(OBJS Seastar::seastar)
