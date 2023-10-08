MODULES_DIR := /lib/modules/$(KVER)
KDIR := $(MODULES_DIR)/build

obj-m += cubefs.o

cubefs-y = btree.o \
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
test:
	-sudo rmmod cubefs
	sudo dmesg -C
	sudo insmod cubefs.ko
	dmesg
