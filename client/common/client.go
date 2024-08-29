package common

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"os"
	"strconv"
	"encoding/binary"

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
	shutdownChan <-chan struct{}
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig, shutdownChan <-chan struct{}) *Client {
	client := &Client{
		config: config,
		shutdownChan: shutdownChan,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

func (c *Client) StartClientLoop() {
	err := c.createClientSocket()
	if err != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err) //TODO remove later
		return
	}
	defer c.conn.Close()

	dni := os.Getenv("DOCUMENTO")
	number := os.Getenv("NUMERO")
	name := os.Getenv("NOMBRE")
	lastname := os.Getenv("APELLIDO")
	dateOfBirth := os.Getenv("NACIMIENTO")

	// Make bet
	err = SendBet(c.conn, dni, name, lastname, dateOfBirth, number)
	if err != nil {
		log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	response, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		log.Errorf("action: receive_response | result: fail | client_id: %v | error: %v", c.config.ID, err) //TODO remove later
		return
	}

	if response != "SUCCESS\n" {
		log.Errorf("action: receive_response | result: fail | client_id: %v | response: %v", c.config.ID, response) //TODO remove later
		return
	}

	log.Infof("action: SendBet | result: success | dni: %v | numero: %v", dni, number)

	// Keep running until shutdown signal detected
	for {
		select {
		case <-c.shutdownChan:
			log.Infof("Shutdown signal received, closing connection for client_id: %v", c.config.ID) //TODO remove later
			return
		default:
			time.Sleep(c.config.LoopPeriod)
		}
	}
}

	// from: "https://stackoverflow.com/questions/18995477/does-golang-provide-htonl-htons"
func htonl(value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return buf[:]
}

func ntohl(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

func SendBet(
	conn net.Conn,
	dni string,
	name string,
	lastname string,
	dateOfBirth string,
	number string,
) error {
	// Helper function to send data with short write handling
	sendAll := func(data []byte) error {
		totalBytesWritten := 0
		for totalBytesWritten < len(data) {
			n, err := conn.Write(data[totalBytesWritten:])
			if err != nil {
				return err
			}
			totalBytesWritten += n
		}
		return nil
	}

	// Convert dni and number to int
	dniInt, err := strconv.Atoi(dni)
	if err != nil {
		return fmt.Errorf("error converting DNI: %v", err)
	}

	numberInt, err := strconv.Atoi(number)
	if err != nil {
		return fmt.Errorf("error converting number: %v", err)
	}

	// 4 bytes
	dniBytes := htonl(dniInt)
	if err := sendAll(dniBytes); err != nil {
		return err
	}

	// 4 bytes
	numberBytes := htonl(numberInt)
	if err := sendAll(numberBytes); err != nil {
		return err
	}

	// 10 bytes
	dateOfBirthBytes := []byte(dateOfBirth)
	if err := sendAll(dateOfBirthBytes); err != nil {
		return err
	}

	// send bytes of name + name
	nameBytes := []byte(name)
	nameLengthBytes := htonl(len(nameBytes))
	if err := sendAll(nameLengthBytes); err != nil {
		return err
	}
	if err := sendAll(nameBytes); err != nil {
		return err
	}

	// send bytes of lastname + lastname
	lastnameBytes := []byte(lastname)
	lastnameLengthBytes := htonl(len(lastnameBytes))
	if err := sendAll(lastnameLengthBytes); err != nil {
		return err
	}
	if err := sendAll(lastnameBytes); err != nil {
		return err
	}

	return nil
}
