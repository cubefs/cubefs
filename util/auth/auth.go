package auth

type TicketMess struct {
	ClientKey   string
	TicketHost  string
	EnableHTTPS bool
	CertFile    string
}
