package common

import (
	"fmt"
	"net"
	"time"
	"os"
	"github.com/op/go-logging"
	"encoding/csv"
	"strconv"
)

type Bet struct {
	CliID       string
	DNI         string
	Name        string
	Lastname    string
	DateOfBirth string
	Number      string
}

func LoadBetsFromCSV(clientID string) ([]Bet, error) {
	filename := fmt.Sprintf("/dataset/agency-%s.csv", clientID)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %v", err)
	}

	var bets []Bet
	for _, record := range records {
		bet := Bet{
			CliID:       clientID,
			Name:        record[0],
			Lastname:    record[1],
			DNI:    		 record[2],
			DateOfBirth: record[3],
			Number:      record[4],
		}
		bets = append(bets, bet)
	}

	return bets, nil
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
	maxBatchSize := c.config.MaxBatchSize

	bets, err := LoadBetsFromCSV(id)
	if err != nil {
			log.Errorf("action: load_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
	}

	err = c.protocol.SendBatches(c.conn, bets, maxBatchSize)
	if err != nil {
			log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
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
