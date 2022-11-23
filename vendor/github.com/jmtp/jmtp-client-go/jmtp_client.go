package jmtp_client_go

type JMTPClient interface {
    Connect() error
    Reconnect() error
    Close() error
    Destroy() error
    IsClosed() bool
}
