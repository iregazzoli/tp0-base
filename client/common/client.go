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
	"encoding/csv"
	"bytes"
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

	// protocol := utils.ClientProtocol{}

	id := os.Getenv("CLI_ID")
	dni := os.Getenv("DOCUMENTO")
	number := os.Getenv("NUMERO")
	maxBatchSize := c.config.MaxBatchSize

	bets, err := LoadBetsFromCSV(id)
	if err != nil {
		log.Errorf("action: load_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	err = SendBatches(c.conn, bets, maxBatchSize)
	if err != nil {
		log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", dni, number) //Catedra

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

func htonl(value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return buf[:]
}

func ntohl(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

func sendAll(conn net.Conn, data []byte) error {
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

func splitBetsIntoBatches(bets []Bet, maxBatchSize int) [][]Bet {
	var batches [][]Bet
	for i := 0; i < len(bets); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(bets) {
					end = len(bets)
			}
			batches = append(batches, bets[i:end])
	}
	return batches
}

func SendBatches(conn net.Conn, bets []Bet, maxBatchSize int) error {
	batches := splitBetsIntoBatches(bets, maxBatchSize)

	// 4 bytes
	numBatches := len(batches)
	numBatchesBytes := htonl(numBatches)
	if err := sendAll(conn, numBatchesBytes); err != nil {
		return err
	}

	for _, batch := range batches {
		if err := SendBatch(conn, batch); err != nil {
			return err
		}
	}

	return nil
}


func SendBatch(
	conn net.Conn,
	bets []Bet,
) error {
	var batchBuffer bytes.Buffer

	numBets := len(bets)
	numBetsBytes := htonl(numBets)
	batchBuffer.Write(numBetsBytes)

	for _, bet := range bets {
		// Convert Id, DNI and Number to int
		cliIDInt, err := strconv.Atoi(bet.CliID)
		if err != nil {
			return fmt.Errorf("error converting CLI_ID: %v", err)
		}
		dniInt, err := strconv.Atoi(bet.DNI)
		if err != nil {
			return fmt.Errorf("error converting DNI: %v", err)
		}
		numberInt, err := strconv.Atoi(bet.Number)
		if err != nil {
			return fmt.Errorf("error converting number: %v", err)
		}
		
		// Convert fields to bytes and send them
		// 4 bytes
		cliIDBytes := htonl(cliIDInt)
		batchBuffer.Write(cliIDBytes)

		// send bytes of name + name
		nameBytes := []byte(bet.Name)
		nameLengthBytes := htonl(len(nameBytes))
		batchBuffer.Write(nameLengthBytes)
		batchBuffer.Write(nameBytes)

		// send bytes of lastname + lastname
		lastnameBytes := []byte(bet.Lastname)
		lastnameLengthBytes := htonl(len(lastnameBytes))
		batchBuffer.Write(lastnameLengthBytes)
		batchBuffer.Write(lastnameBytes)
		
		// 4 bytes
		dniBytes := htonl(dniInt)
		batchBuffer.Write(dniBytes)

		// 10 bytes
		dateOfBirthBytes := []byte(bet.DateOfBirth)
		batchBuffer.Write(dateOfBirthBytes)

		// 4 bytes
		numberBytes := htonl(numberInt)
		batchBuffer.Write(numberBytes)
	}

	// Send whole batch in one send operation
	if err := sendAll(conn, batchBuffer.Bytes()); err != nil {
		return err
	}

	// Wait server answer
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("error receiving server response: %v", err)
	}

	if response != "SUCCESS\n" {
		return fmt.Errorf("batch was not successful, server response: %v", response)
	}

	return nil
}
