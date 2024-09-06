package common

import (
	"fmt"
	"net"
	"time"
	"os"
	"github.com/op/go-logging"
	"encoding/csv"
	"strconv"
	"io"
)

const MaxBatchSizeBytes = 8192 // 8KB

type Bet struct {
	CliID       string
	DNI         string
	Name        string
	Lastname    string
	DateOfBirth string
	Number      string
}

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	MaxBatchSize  int
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
	shutdownChan <-chan struct{}
	protocol      *ClientProtocol
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
	var conn net.Conn
	var err error
	retryCount := 0
	maxRetries := 5
	initialDelay := 2 * time.Second

	for retryCount < maxRetries {
		conn, err = net.Dial("tcp", c.config.ServerAddress)
		if err == nil {
			c.conn = conn

			clientIDInt, err := strconv.Atoi(os.Getenv("CLI_ID"))
			if err != nil {
				return fmt.Errorf("error converting client ID to int: %v", err)
			}
			clientIDBytes := c.protocol.htonl(clientIDInt)
			if err := c.protocol.sendAll(c.conn, clientIDBytes); err != nil {
				return fmt.Errorf("error sending client ID to server: %v", err)
			}

			return nil
		}

		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v | retry: %d",
			c.config.ID,
			err,
			retryCount,
		)

		retryCount++
		time.Sleep(initialDelay * time.Duration(retryCount))
	}

	return err
}


func (c *Client) StartClientLoop() {
	err := c.createClientSocket()
	if err != nil {
			log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
	}
	defer c.conn.Close()

	id := os.Getenv("CLI_ID")
	MaxBatchSize := c.config.MaxBatchSize

	err = c.SendBatches(id, MaxBatchSize)
	if err != nil {
		log.Errorf("action: send_batches | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	err = c.protocol.startLottery(c.conn)
	if err != nil {
			log.Errorf("action: start_lottery | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
	}

	winners, err := c.protocol.receiveWinners(c.conn)
	if err != nil {
			log.Errorf("action: receive_winner_number | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
	}

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", len(winners))

	for _, winner := range winners {
			log.Infof("action: consulta_ganadores | ganador_numero: %v | ganador_dni: %v", winner[0], winner[1])
	}

// Keep running until shutdown signal detected
	for {
			select {
			case <-c.shutdownChan:
					log.Infof("Shutdown signal received, closing connection for client_id: %v", c.config.ID)
					return
			default:
					time.Sleep(c.config.LoopPeriod)
			}
	}
}

func (c *Client) SendBatches(clientID string, MaxBatchSize int) error {
	filename := fmt.Sprintf("/dataset/agency-%s.csv", clientID)
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var extraBet *Bet
	const maxBatchLength = 8193

	for {
		bets := []Bet{}
		batchSize := 0

		if extraBet != nil {
			bets = append(bets, *extraBet)
			batchSize += len(extraBet.toBytes(clientID))
			extraBet = nil
		}

		for len(bets) < MaxBatchSize {
			record, err := reader.Read()

			if err == io.EOF {
				if len(bets) > 0 {
					err = c.protocol.SendBatch(c.conn, bets)
					if err != nil {
						return fmt.Errorf("error sending batch: %v", err)
					}
				}

				if _, err := c.conn.Write([]byte{0}); err != nil {
					return fmt.Errorf("error sending last batch confirmation: %v", err)
				}
				log.Infof("Sent last batch signal")

				if err := c.waitForSuccess(); err != nil {
					return fmt.Errorf("error receiving SUCCESS: %v", err)
				}

				log.Infof("SUCESS RECV")

				return nil
			}

			if err != nil {
				return fmt.Errorf("error reading CSV: %v", err)
			}

			newBet := Bet{
				CliID:       clientID,
				Name:        record[0],
				Lastname:    record[1],
				DNI:         record[2],
				DateOfBirth: record[3],
				Number:      record[4],
			}

			betSize := len(newBet.toBytes(clientID))
			if batchSize+betSize > maxBatchLength {
				extraBet = &newBet
				break
			}

			batchSize += betSize
			bets = append(bets, newBet)

			if len(bets) == MaxBatchSize || batchSize+betSize >= maxBatchLength {
				err = c.protocol.SendBatch(c.conn, bets)
				if err != nil {
					return fmt.Errorf("error sending batch: %v", err)
				}

				if _, err := c.conn.Write([]byte{1}); err != nil {
					return fmt.Errorf("error sending batch continuation signal: %v", err)
				}

				bets = []Bet{}
				batchSize = 0
			}
		}
	}
}

func (c *Client) waitForSuccess() error {
	response := make([]byte, 7) // Recibe 7 bytes sin verificar contenido
	if _, err := c.conn.Read(response); err != nil {
			return fmt.Errorf("error reading SUCCESS response: %v", err)
	}
	return nil
}

func (b *Bet) toBytes(clientID string) []byte {
	cliIDBytes := []byte(clientID)
	nameBytes := []byte(b.Name)
	lastnameBytes := []byte(b.Lastname)
	dniBytes := []byte(b.DNI)
	dateOfBirthBytes := []byte(b.DateOfBirth)
	numberBytes := []byte(b.Number)

	result := append(cliIDBytes, nameBytes...)
	result = append(result, lastnameBytes...)
	result = append(result, dniBytes...)
	result = append(result, dateOfBirthBytes...)
	result = append(result, numberBytes...)

	return result
}
