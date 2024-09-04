package common

import (
	"os"
	"net"
	"time"
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
	protocol     *ClientProtocol
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig, shutdownChan <-chan struct{}) *Client {
	client := &Client{
		config: config,
		shutdownChan: shutdownChan,
		protocol: &ClientProtocol{},
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

	log.Errorf("action: apuesta_enviada | result: success | dni: %v | numero: %v", dni, number) //Catedra

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

// func htonl(value int) []byte {
// 	var buf [4]byte
// 	binary.BigEndian.PutUint32(buf[:], uint32(value))
// 	return buf[:]
// }

// func ntohl(b []byte) int {
// 	return int(binary.BigEndian.Uint32(b))
// }

// func sendAll(conn net.Conn, data []byte) error {
// 	totalBytesWritten := 0
// 	for totalBytesWritten < len(data) {
// 		n, err := conn.Write(data[totalBytesWritten:])
// 		if err != nil {
// 			return err
// 		}
// 		totalBytesWritten += n
// 	}
// 	return nil
// }

// func SendBet(
// 	conn net.Conn,
// 	cliID string,
// 	dni string,
// 	name string,
// 	lastname string,
// 	dateOfBirth string,
// 	number string,
// ) (bool, error) {
// 	// Convert clientId, dni and number to int
// 	cliIDInt, err := strconv.Atoi(cliID)
// 	if err != nil {
// 		return false, fmt.Errorf("error converting CLI_ID: %v", err)
// 	}

// 	dniInt, err := strconv.Atoi(dni)
// 	if err != nil {
// 		return false, fmt.Errorf("error converting DNI: %v", err)
// 	}

// 	numberInt, err := strconv.Atoi(number)
// 	if err != nil {
// 		return false, fmt.Errorf("error converting number: %v", err)
// 	}

// 	// 4 bytes
// 	cliIDBytes := htonl(cliIDInt)
// 	if err := sendAll(conn, cliIDBytes); err != nil {
// 		return false, err
// 	}

// 	// 4 bytes
// 	dniBytes := htonl(dniInt)
// 	if err := sendAll(conn, dniBytes); err != nil {
// 		return false, err
// 	}

// 	// 4 bytes
// 	numberBytes := htonl(numberInt)
// 	if err := sendAll(conn, numberBytes); err != nil {
// 		return false, err
// 	}

// 	// 10 bytes
// 	dateOfBirthBytes := []byte(dateOfBirth)
// 	if err := sendAll(conn, dateOfBirthBytes); err != nil {
// 		return false, err
// 	}

// 	// send bytes of name + name
// 	nameBytes := []byte(name)
// 	nameLengthBytes := htonl(len(nameBytes))
// 	if err := sendAll(conn, nameLengthBytes); err != nil {
// 		return false, err
// 	}
// 	if err := sendAll(conn, nameBytes); err != nil {
// 		return false, err
// 	}

// 	// send bytes of lastname + lastname
// 	lastnameBytes := []byte(lastname)
// 	lastnameLengthBytes := htonl(len(lastnameBytes))
// 	if err := sendAll(conn, lastnameLengthBytes); err != nil {
// 		return false, err
// 	}
// 	if err := sendAll(conn, lastnameBytes); err != nil {
// 		return false, err
// 	}

// 	// Wait for server response
// 	response, err := bufio.NewReader(conn).ReadString('\n')
// 	if err != nil {
// 		return false, fmt.Errorf("error receiving server response: %v", err)
// 	}

// 	// Check if the response is "SUCCESS\n"
// 	if response != "SUCCESS\n" {
// 		return false, fmt.Errorf("bet was not successful, server response: %v", response)
// 	}

// 	return true, nil
// }
