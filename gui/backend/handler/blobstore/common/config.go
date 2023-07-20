package common

import (
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/config"
)

type CodeMode struct {
	ModeName codemode.CodeModeName `json:"mode_name"`
	CodeMode codemode.CodeMode     `json:"code_mode"`
	Enable   bool                  `json:"enable"`
	Tactic   codemode.Tactic       `json:"tactic"`
}

func GetCodeMode(c *gin.Context, consulAddr string) ([]CodeMode, error) {
	policies, err := config.Get(c, consulAddr, "code_mode")
	if err != nil {
		return nil, err
	}

	modes := make([]CodeMode, 0)
	for _, policy := range policies {
		item := CodeMode{
			ModeName: policy.ModeName,
			CodeMode: policy.ModeName.GetCodeMode(),
			Enable:   policy.Enable,
			Tactic:   policy.ModeName.Tactic(),
		}
		modes = append(modes, item)
	}
	return modes, nil
}
