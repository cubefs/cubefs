package proto

type AuthUser struct {
	UserID    string `json:"userID"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}
