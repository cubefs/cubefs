package datanode

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/cubefs/cubefs/util/log"
)

type nvme_passthru_cmd struct {
	opcode       uint8
	flags        uint8
	rsvd1        uint16
	nsid         uint32
	cdw2         uint32
	cdw3         uint32
	metadata     uint64
	addr         uint64
	metadata_len uint32
	data_len     uint32
	cdw10        uint32
	cdw11        uint32
	cdw12        uint32
	cdw13        uint32
	cdw14        uint32
	cdw15        uint32
	timeout_ms   uint32
	result       uint32
}

type nvme_extended_health_info struct {
	soft_read_recoverable_errs      uint32
	flash_die_raid_recoverable_errs uint32
	pcie_rx_correct_errs            uint32
	pcie_rx_uncorrect_errs          uint32
	data_read_from_flash            uint32
	data_write_to_flash             uint32
	temp_throttle_info              uint32
	power_consumption               uint32
	pf_bbd_read_cnt                 uint32
	sfx_critical_warning            uint32
	raid_recovery_total_count       uint32
	rvd                             uint32
	opn                             [32]uint8
	total_physical_capability       uint64
	free_physical_capability        uint64 //sector_count             the sector size is 512
	physical_usage_ratio            uint32
	comp_ratio                      uint32
	otp_rsa_en                      uint32
	power_mw_consumption            uint32
	io_speed                        uint32
}

type sfx_status struct {
	totalPhysicalCapability uint64
	freePhysicalCapability  uint64 //sector_count             the sector size is 512
	physicalUsageRatio      uint32
	compRatio               uint32
}

const (
	NVME_IOCTL_ADMIN_CMD = 0xc0484e41 //NVME_IOCTL_ADMIN_CMD	_IOWR('N', 0x41, struct nvme_admin_cmd)
)

type nvme_id_ctrl struct {
	vId        uint16 //Only the first two bytes of vid are used, so there is no complete description of the struct. The nvme_id_ctrl struct can be found in linux/nvme.h
	ignoreWord [2047]uint16
}

func nvmeAdminPassthru(fd int, cmd nvme_passthru_cmd) error {
	_, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), NVME_IOCTL_ADMIN_CMD, uintptr(unsafe.Pointer(&cmd)))
	if err != 0 {
		return syscall.Errno(err)
	}
	return nil
}

/**
 * @brief GetDevCheckSfx get devName form file path and check if it is sfx csd
 *
 * @param path, file path
 * @param isSfx, true:is on sfx csd; false:fail or not on sfx csd
 * @param devName, the sfx csd block dev name
 *
 */
func GetDevCheckSfx(path string) (isSfx bool, devName string) {
	isSfx = false

	dInfo, err := os.Stat(path)
	if err != nil {
		log.LogDebugf("get Disk %s stat fail err:%s\n", path, err.Error())
		return
	}
	dStat := dInfo.Sys().(*syscall.Stat_t)
	maj := dStat.Dev >> 8
	min := dStat.Dev & 0xFF
	b := make([]byte, 128)
	devLink := "/sys/dev/block/" + strconv.FormatUint(maj, 10) + ":" + strconv.FormatUint(min, 10)
	n, err := syscall.Readlink(devLink, b)
	if err != nil {
		log.LogErrorf("get devName fail err:%s\n", err.Error())
		return
	}
	devDir := string(b[0:n])
	n = strings.LastIndex(devDir, "/")
	devName = "/dev/" + devDir[n+1:]

	var idctl nvme_id_ctrl = nvme_id_ctrl{}
	var ioctlCmd nvme_passthru_cmd = nvme_passthru_cmd{}
	fd, err := syscall.Open(devName, syscall.O_RDWR, 0777)
	if err != nil {
		log.LogErrorf("%s device open failed %s\n", devName, err.Error())
		return
	}
	defer syscall.Close(fd)

	ioctlCmd.opcode = 0x06 //nvme_admin_identify
	ioctlCmd.nsid = 0
	ioctlCmd.addr = uint64(uintptr(unsafe.Pointer(&idctl)))
	ioctlCmd.data_len = 4096
	ioctlCmd.cdw10 = 1
	ioctlCmd.cdw11 = 0
	err = nvmeAdminPassthru(fd, ioctlCmd)
	if err != nil {
		log.LogErrorf("get %s Vid fail %s\n", devName, err.Error())
		return
	}

	//0xcc53 is sfx vendor id
	if 0xcc53 == idctl.vId {
		isSfx = true
	}
	return
}

/**
 * @brief GetCSDStatus get sfx status by devName
 *
 * @param devName, the sfx csd block dev name
 * @param dStatus.compRatio, full disk compression ratio (100%~800%)
 * @param dStatus.physicalUsageRatio, physical space usage ratio
 * @param dStatus.freePhysicalCapability, free physical space .Byte
 * @param dStatus.totalPhysicalCapability, total physical space .Byte
 *
 * @return nil success; err fail
 */
func GetSfxStatus(devName string) (dStatus sfx_status, err error) {
	var dataLen uint32 = 128
	var numd uint32 = (dataLen >> 2) - 1
	var numdu uint16 = uint16(numd >> 16)
	var numdl uint16 = uint16(numd & 0xffff)
	var cdw10 uint32 = 0xc2 | (uint32(numdl) << 16) //logid:0xc2 NVME_LOG_EXTENDED_HEALTH_INFO
	var extendHealth nvme_extended_health_info = nvme_extended_health_info{}
	var ioctlCmd nvme_passthru_cmd = nvme_passthru_cmd{}
	fd, err := syscall.Open(devName, syscall.O_RDWR, 0777)
	if err != nil {
		log.LogErrorf("device %s open failed %s\n", devName, err.Error())
		return
	}
	defer syscall.Close(fd)

	ioctlCmd.opcode = 0x02 //nvme_admin_get_log_page
	ioctlCmd.nsid = 0
	ioctlCmd.addr = uint64(uintptr(unsafe.Pointer(&extendHealth)))
	ioctlCmd.data_len = dataLen
	ioctlCmd.cdw10 = cdw10
	ioctlCmd.cdw11 = uint32(numdu)
	err = nvmeAdminPassthru(fd, ioctlCmd)
	if err != nil {
		log.LogErrorf("device %s get status failed %s\n", devName, err.Error())
		return
	}

	dStatus.compRatio = extendHealth.comp_ratio
	if dStatus.compRatio < 100 {
		dStatus.compRatio = 100
	} else if dStatus.compRatio > 800 {
		dStatus.compRatio = 800
	}

	dStatus.physicalUsageRatio = extendHealth.physical_usage_ratio
	//The unit of PhysicalCapability is the number of sectors, and the sector size is 512 bytes, converted to bytes
	dStatus.freePhysicalCapability = extendHealth.free_physical_capability * 512
	dStatus.totalPhysicalCapability = extendHealth.total_physical_capability * 512

	return
}
