MODULES_DIR := /lib/modules/$(KVER)
KDIR := $(MODULES_DIR)/build

obj-m += cubefs.o

cubefs-y :=	btree.o \
	cfs_buffer.o \
	cfs_common.o \
	cfs_core.o \
	cfs_extent.o \
	cfs_fs.o \
	cfs_json.o \
	cfs_master.o \
	cfs_meta.o \
	cfs_option.o \
	cfs_packet.o \
	cfs_socket.o

all:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules
clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
mount:
	sudo dmesg -C
	sudo insmod cubefs.ko
	sudo mount -t cubefs -o owner=ltptest,dentry_cache_valid_ms=5000,attr_cache_valid_ms=30000 \
    //10.177.128.203:17010,10.177.128.204:17010,10.177.128.205:17010/ltptest /mnt/cubefs
umount:
	-sudo umount /mnt/cubefs
	-sudo rmmod cubefs
