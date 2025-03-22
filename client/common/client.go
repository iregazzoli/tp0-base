package common

import (
	"net"
	"os"
	"time"
	"fmt"
	"io"
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

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchMaxAmount int  
	CSVPath       string
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

	log.Infof("action: connect | result: success | client_id: %v", c.config.ID)

	c.conn = conn

	if err := c.protocol.SendID(c.conn, c.config.ID); err != nil {
		log.Errorf("action: send_id | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

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

	if err := c.sendBatches(); err != nil {
    log.Errorf("action: send_batches | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}
}

func (c *Client) sendBatches() error {
	file, err := os.Open(c.config.CSVPath)
	if err != nil {
		return fmt.Errorf("Error opening CSV: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	//Since we are sending the amount of bets the size of the batch starts at 4 bytes
	batchSize := 4
	var batch []Bet

	log.Infof("action: send_batches | result: in_progress | client_id: %v", c.config.ID)

	for {
		if c.stopping {
			return nil
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV: %w", err)
		}

		bet := Bet{
			CliID:       c.config.ID,
			Name:        record[0],
			Lastname:    record[1],
			DNI:         record[2],
			DateOfBirth: record[3],
			Number:      record[4],
		}

		betSize := computeBetBinarySize(bet)

		// If adding the new bet to the batch surpasses 8kb send the batch as it is.
		if batchSize+betSize > 8192 {
			if len(batch) > 0 {
				if _, err := c.protocol.SendBatch(c.conn, batch); err != nil {
					return fmt.Errorf("error enviando batch: %w", err)
				}
			}
			// Restart batch
			batch = []Bet{}
			batchSize = 4
		}
		batch = append(batch, bet)
		batchSize += betSize
	}

	// Send last batch if there still unsend bets
	if len(batch) > 0 {
		if _, err := c.protocol.SendBatch(c.conn, batch); err != nil {
			return fmt.Errorf("error enviando batch: %w", err)
		}
	}

	if err := c.protocol.signalEndOfBatches(c.conn); err != nil {
    return fmt.Errorf("error sending termination header: %w", err)
	}
	log.Infof("action: send_batches | result: success | client_id: %v", c.config.ID)

	winners, err := c.protocol.ConsultWinners(c.conn)
	if err != nil {
		return fmt.Errorf("error consulting winners: %w", err)
	}
	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", len(winners))

	return nil
}

func computeBetBinarySize(bet Bet) int {
	// Each Bet is:
	// cliID: 4 bytes (agency)
	// name: 4 bytes para la longitud + len(bet.Name)
	// lastname: 4 bytes para la longitud + len(bet.Lastname)
	// dni: 4 bytes
	// dateOfBirth: 10 bytes
	// number: 4 bytes
	// Total: (4 + len(name)) + (4 + len(lastname)) + 4 + 10 + 4 = 30 + len(name) + len(lastname)
	return 30 + len(bet.Name) + len(bet.Lastname)
}

func (c *Client) StopClientLoop() {
	log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
	log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
	c.conn.Close()
	c.stopping = true
	os.Exit(0)
}
