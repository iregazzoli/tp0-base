package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"bufio"
)

type ClientProtocol struct{}

// from: "https://stackoverflow.com/questions/18995477/does-golang-provide-htonl-htons"	
func (cp *ClientProtocol) htonl(value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return buf[:]
}

func (cp *ClientProtocol) ntohl(b []byte) int {
	return int(binary.LittleEndian.Uint32(b))
}

func (cp *ClientProtocol) sendAll(conn net.Conn, data []byte) error {
	// Helper function to send data with short write handling
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

func (cp *ClientProtocol) splitBetsIntoBatches(bets []Bet, maxBatchSize int) [][]Bet {
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

func (cp *ClientProtocol) SendBatches(conn net.Conn, bets []Bet, maxBatchSize int) error {
	batches := cp.splitBetsIntoBatches(bets, maxBatchSize)

	// 4 bytes
	numBatches := len(batches)
	numBatchesBytes := cp.htonl(numBatches)
	if err := cp.sendAll(conn, numBatchesBytes); err != nil {
		return err
	}

	for _, batch := range batches {
		if err := cp.SendBatch(conn, batch); err != nil {
			return err
		}
	}

	return nil
}

func (cp *ClientProtocol) SendBatch(
	conn net.Conn,
	bets []Bet,
) error {
	var batchBuffer bytes.Buffer
	numBets := len(bets)
	numBetsBytes := cp.htonl(numBets)
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
		cliIDBytes := cp.htonl(cliIDInt)
		batchBuffer.Write(cliIDBytes)

		// send bytes of name + name
		nameBytes := []byte(bet.Name)
		nameLengthBytes := cp.htonl(len(nameBytes))
		batchBuffer.Write(nameLengthBytes)
		batchBuffer.Write(nameBytes)

		// send bytes of lastname + lastname
		lastnameBytes := []byte(bet.Lastname)
		lastnameLengthBytes := cp.htonl(len(lastnameBytes))
		batchBuffer.Write(lastnameLengthBytes)
		batchBuffer.Write(lastnameBytes)
		
		// 4 bytes
		dniBytes := cp.htonl(dniInt)
		batchBuffer.Write(dniBytes)

		// 10 bytes
		dateOfBirthBytes := []byte(bet.DateOfBirth)
		batchBuffer.Write(dateOfBirthBytes)

		// 4 bytes
		numberBytes := cp.htonl(numberInt)
		batchBuffer.Write(numberBytes)
	}

	// Send whole batch in one send operation
	if err := cp.sendAll(conn, batchBuffer.Bytes()); err != nil {
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

// Function to start the lottery by sending a ready signal
func (cp *ClientProtocol) startLottery(conn net.Conn) error {
	readyByte := byte(1)
	if err := cp.sendAll(conn, []byte{readyByte}); err != nil {
		return fmt.Errorf("error sending lottery ready byte: %v", err)
	}
	return nil
}
func (cp *ClientProtocol) receiveWinners(conn net.Conn) ([][2]int, error) {

	// (4 bytes)
	var numWinnersBytes [4]byte
	if _, err := conn.Read(numWinnersBytes[:]); err != nil {
			return nil, fmt.Errorf("error receiving number of winners: %v", err)
	}
	numWinners := cp.ntohl(numWinnersBytes[:])

	winners := make([][2]int, 0, numWinners)

	for i := 0; i < numWinners; i++ {
			// (4 bytes)
			var winnerNumberBytes [4]byte
			if _, err := conn.Read(winnerNumberBytes[:]); err != nil {
					return nil, fmt.Errorf("error receiving winner number: %v", err)
			}
			winnerNumber := cp.ntohl(winnerNumberBytes[:])

			// (4 bytes) 
			var winnerDniBytes [4]byte
			if _, err := conn.Read(winnerDniBytes[:]); err != nil {
					return nil, fmt.Errorf("error receiving winner DNI: %v", err)
			}
			winnerDni := cp.ntohl(winnerDniBytes[:])

			winners = append(winners, [2]int{winnerNumber, winnerDni})
	}

	return winners, nil
}


