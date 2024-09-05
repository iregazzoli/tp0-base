package common

import (
	"fmt"
	"net"
	"time"
	"os"
	"strconv"
	"github.com/op/go-logging"
	"encoding/csv"
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
	protocol     *ClientProtocol
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig, shutdownChan <-chan struct{}) *Client {
	client := &Client{
		config: config,
		shutdownChan: shutdownChan,
		protocol:     &ClientProtocol{},
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

	// protocol := utils.ClientProtocol{}

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

	winnerNumber, err := c.protocol.receiveWinnerNumber(c.conn)
	if err != nil {
		log.Errorf("action: receive_winner_number | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	winners := checkIfWinner(winnerNumber, bets)
	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", winners) //CATEDRA

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

func checkIfWinner(winnerNumber int, bets []Bet) int {
	winners := 0
	for _, bet := range bets {
		betNumber, err := strconv.Atoi(bet.Number)
		if err == nil && betNumber == winnerNumber {
			winners += 1
		}
	}
	return winners
}
