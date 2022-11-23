package jmtp_client_go

type JmtpClient interface {
    Connect() error
    SetUrl(url string)
    Close() error
    Destroy() error
    SendPacket(packet JmtpPacket) (int, error)
    Reset() error
}

type JmtpConfig interface {
    GetUrl() string
    GetTimeoutSec() int
}

type Callback func(packet JmtpPacket, err error)

