package base

//go:generate mockgen -source=./transport.go -destination=./mock_transport.go -package=base -mock_names Transport=MockTransport
