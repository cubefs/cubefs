package proto

type UmpCollectBy int

const (
	UmpCollectByUnkown     = 0
	UmpCollectByFile       = 1
	UmpCollectByJmtpClient = 2
)

func UmpCollectByStr(by UmpCollectBy) string {
	if by == UmpCollectByFile {
		return "file"
	} else if by == UmpCollectByJmtpClient {
		return "jmtp client"
	} else {
		return "unknown"
	}
}
