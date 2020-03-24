package auth

type TicketMess struct {
	ClientKey   string
	TicketHosts []string
	EnableHTTPS bool
	CertFile    string
}

//the ticket from authnode
type Ticket struct {
	ID         string `json:"client_id"`
	SessionKey string `json:"session_key"`
	ServiceID  string `json:"service_id"`
	Ticket     string `json:"ticket"`
}
