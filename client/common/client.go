package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
	stopping bool
	protocol *ClientProtocol
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:   config,
		stopping: false,
		protocol: &ClientProtocol{},
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	if c.stopping {
		return nil
	}
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	if c.stopping {
		return
	}
	// Create the connection the server in every loop iteration. Send an
	err := c.createClientSocket()
	if err != nil {
		return
	}

	if c.stopping {
		return
	}

	id := os.Getenv("CLI_ID")
	dni := os.Getenv("DOCUMENTO")
	number := os.Getenv("NUMERO")
	name := os.Getenv("NOMBRE")
	lastname := os.Getenv("APELLIDO")
	dateOfBirth := os.Getenv("NACIMIENTO")

	success, err := c.protocol.SendBet(c.conn, id, dni, name, lastname, dateOfBirth, number)
	if err != nil || !success {
		log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", dni, number)

	if c.stopping {
		return
	}
}

func (c *Client) StopClientLoop() {
	log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
	c.conn.Close()
	c.stopping = true
	os.Exit(0)
}