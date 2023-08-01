package datanode

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/cubefs/cubefs/util/log"
)

type nvmePassthruCmd struct {
	opcode      uint8
	flags       uint8
	rsvd1       uint16
	nsid        uint32
	cdw2        uint32
	cdw3        uint32
	metadata    uint64
	addr        uint64
	metadataLen uint32
	dataLen     uint32
	cdw10       uint32
	cdw11       uint32
	cdw12       uint32
	cdw13       uint32
	cdw14       uint32
	cdw15       uint32
	timeoutMs   uint32
	result      uint32
}

type nvmeExtendedHealthInfo struct {
	softReadRecoverableErrs     uint32
	flashDieRaidRecoverableErrs uint32
	pcieRxCorrectErrs           uint32
	pcieRxUncorrectErrs         uint32
	dataReadFromFlash           uint32
	dataWriteToFlash            uint32
	tempThrottleInfo            uint32
	powerConsumption            uint32
	pfBbdReadCnt                uint32
	sfxCriticalWarning          uint32
	raidRecoveryTotalCount      uint32
	rvd                         uint32
	opn                         [32]uint8
	totalPhysicalCapability     uint64
	freePhysicalCapability      uint64 //unit is sectorCount and the sector size is 512
	physicalUsageRatio          uint32
	compRatio                   uint32
	otpRsaEn                    uint32
	powerMwConsumption          uint32
	ioSpeed                     uint32
}

type sfxStatus struct {
	totalPhysicalCapability uint64
	freePhysicalCapability  uint64 //unit is sectorCount and the sector size is 512
	physicalUsageRatio      uint32
	compRatio               uint32
}

const (
	//nvme ioctl cmd
	NVME_IOCTL_ADMIN_CMD = 0xc0484e41 //NVME_IOCTL_ADMIN_CMD	_IOWR('N', 0x41, struct nvme_admin_cmd)
	//nvme admin opcode
	NVME_ADMIN_GET_LOG_PAGE = 0x02
	NVME_ADMIN_IDENTIFY     = 0x06
	//nvme logpage id
	NVME_LOG_EXTENDED_HEALTH_INFO = 0xc2 //SFX extended logid
	//VID
	PCI_VENDOR_ID_SFX = 0xcc53
)

type nvmeIdCtrl struct {
	vId        uint16 //Only the first two bytes of vid are used, so there is no complete description of the struct. The nvme_id_ctrl struct can be found in linux/nvme.h
	ignoreWord [2047]uint16
}

func nvmeAdminPassthru(fd int, cmd nvmePassthruCmd) error {
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

	var idctl nvmeIdCtrl = nvmeIdCtrl{}
	var ioctlCmd nvmePassthruCmd = nvmePassthruCmd{}
	fd, err := syscall.Open(devName, syscall.O_RDWR, 0777)
	if err != nil {
		log.LogErrorf("%s device open failed %s\n", devName, err.Error())
		return
	}
	defer syscall.Close(fd)

	ioctlCmd.opcode = NVME_ADMIN_IDENTIFY
	ioctlCmd.nsid = 0
	ioctlCmd.addr = uint64(uintptr(unsafe.Pointer(&idctl)))
	ioctlCmd.dataLen = 4096
	ioctlCmd.cdw10 = 1
	ioctlCmd.cdw11 = 0
	err = nvmeAdminPassthru(fd, ioctlCmd)
	if err != nil {
		log.LogErrorf("get %s Vid fail %s\n", devName, err.Error())
		return
	}

	//sfx vendor id
	if PCI_VENDOR_ID_SFX == idctl.vId {
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
func GetSfxStatus(devName string) (dStatus sfxStatus, err error) {
	var dataLen uint32 = 128
	var numd uint32 = (dataLen >> 2) - 1
	var numdu uint16 = uint16(numd >> 16)
	var numdl uint16 = uint16(numd & 0xffff)
	var cdw10 uint32 = NVME_LOG_EXTENDED_HEALTH_INFO | (uint32(numdl) << 16)
	var extendHealth nvmeExtendedHealthInfo = nvmeExtendedHealthInfo{}
	var ioctlCmd nvmePassthruCmd = nvmePassthruCmd{}
	fd, err := syscall.Open(devName, syscall.O_RDWR, 0777)
	if err != nil {
		log.LogErrorf("device %s open failed %s\n", devName, err.Error())
		return
	}
	defer syscall.Close(fd)

	ioctlCmd.opcode = NVME_ADMIN_GET_LOG_PAGE
	ioctlCmd.nsid = 0
	ioctlCmd.addr = uint64(uintptr(unsafe.Pointer(&extendHealth)))
	ioctlCmd.dataLen = dataLen
	ioctlCmd.cdw10 = cdw10
	ioctlCmd.cdw11 = uint32(numdu)
	err = nvmeAdminPassthru(fd, ioctlCmd)
	if err != nil {
		log.LogErrorf("device %s get status failed %s\n", devName, err.Error())
		return
	}

	dStatus.compRatio = extendHealth.compRatio
	if dStatus.compRatio < 100 {
		dStatus.compRatio = 100
	} else if dStatus.compRatio > 800 {
		dStatus.compRatio = 800
	}

	dStatus.physicalUsageRatio = extendHealth.physicalUsageRatio
	//The unit of PhysicalCapability is the number of sectors, and the sector size is 512 bytes, converted to bytes
	dStatus.freePhysicalCapability = extendHealth.freePhysicalCapability * 512
	dStatus.totalPhysicalCapability = extendHealth.totalPhysicalCapability * 512

	return
}
