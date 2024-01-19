MODULES_DIR := /lib/modules/$(KVER)
KDIR := $(MODULES_DIR)/build

obj-m += cubefs.o

cubefs-y :=	btree.o \
	cfs_buffer.o \
	cfs_common.o \
	cfs_core.o \
	cfs_extent_cache.o \
	cfs_extent_client.o \
	cfs_extent_reader.o \
	cfs_extent_stream.o \
	cfs_extent_writer.o \
	cfs_fs.o \
	cfs_json.o \
	cfs_log.o \
	cfs_master.o \
	cfs_meta.o \
	cfs_option.o \
	cfs_packet.o \
	cfs_page.o \
	cfs_socket.o \
	rdma/rdma_api.o \
	cfs_rdma_socket.o

all:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules
clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
mount_dev:
	sudo dmesg -C
	sudo insmod cubefs.ko
	sudo mount -t cubefs -o owner=ltptest,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000 \
    //10.177.128.203:17010,10.177.128.204:17010,10.177.128.205:17010/ltptest /mnt/cubefs
# enable RDMA mode:
#	sudo mount -t cubefs -o owner=ltptest,enable_rdma=true,rdma_port=17360 \
#    //10.177.128.203:17010,10.177.128.204:17010,10.177.128.205:17010/ltptest /mnt/cubefs
mount_test:
	sudo dmesg -C
	sudo insmod cubefs.ko
	sudo mount -t cubefs -o owner=console_vol,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000 \
    //10.177.69.105:17010,10.177.69.106:17010,10.177.117.108:17010/kernel_test /mnt/cubefs
umount:
	-sudo umount /mnt/cubefs
	-sudo rmmod cubefs
