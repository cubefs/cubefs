package enums

import "github.com/gin-gonic/gin"

const (
	ModeTest = "test"
	ModeDev  = "dev"
	ModeProd = "prod"
)

func GetGinMode(mode string) string {
	switch mode {
	case ModeDev:
		return gin.DebugMode
	case ModeTest:
		return gin.TestMode
	case ModeProd:
		return gin.ReleaseMode
	default:
		return gin.DebugMode
	}
}