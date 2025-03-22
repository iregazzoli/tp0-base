package common

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
)

type ClientProtocol struct{}

// from: "https://stackoverflow.com/questions/18995477/does-golang-provide-htonl-htons"	
func (cp *ClientProtocol) htonl(value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return buf[:]
}

func (cp *ClientProtocol) ntohl(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
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

func (cp *ClientProtocol) readNBytes(conn net.Conn, n int) ([]byte, error) {
	// Helper function to read data with short read handling
	buf := make([]byte, n)
	totalRead := 0
	for totalRead < n {
		nRead, err := conn.Read(buf[totalRead:])
		if err != nil {
			return nil, err
		}
		totalRead += nRead
	}
	return buf, nil
}

func convertToInt(value string) (int, error) {
	converted, err := strconv.Atoi(value)
	if err != nil {
			return 0, fmt.Errorf("error converting %s to int: %v", value, err)
	}
	return converted, nil
}

func (cp *ClientProtocol) signalEndOfBatches(conn net.Conn) error {
	terminationHeader := cp.htonl(0)
	if err := cp.sendAll(conn, terminationHeader); err != nil {
			return fmt.Errorf("error sending termination header: %w", err)
	}
	return nil
}

func (cp *ClientProtocol) SendID(conn net.Conn, id string) error {
	idInt, err := strconv.Atoi(id)
	if err != nil {
			return fmt.Errorf("error converting client id to int: %w", err)
	}
	idBytes := cp.htonl(idInt)
	if err := cp.sendAll(conn, idBytes); err != nil {
			return fmt.Errorf("error sending client id: %w", err)
	}
	return nil
}

// Returns (true, nil) if server's response is "SUCCESS\n" and (false, error) otherwise.
func (cp *ClientProtocol) SendBatch(conn net.Conn, bets []Bet) (bool, error) {
	var batchBuffer bytes.Buffer

	// Amount of Bets in Batch (4 bytes)
	numBets := len(bets)
	numBetsBytes := cp.htonl(numBets)
	batchBuffer.Write(numBetsBytes)

	for _, bet := range bets {
		// Convert cliID, DNI y Number to int
		cliIDInt, err := strconv.Atoi(bet.CliID)
		if err != nil {
			return false, fmt.Errorf("error converting CLI_ID: %v", err)
		}
		dniInt, err := strconv.Atoi(bet.DNI)
		if err != nil {
			return false, fmt.Errorf("error converting DNI: %v", err)
		}
		numberInt, err := strconv.Atoi(bet.Number)
		if err != nil {
			return false, fmt.Errorf("error converting Number: %v", err)
		}

		// cliID (4 bytes)
		batchBuffer.Write(cp.htonl(cliIDInt))

		// DNI (4 bytes)
		batchBuffer.Write(cp.htonl(dniInt))

		// Bet Number (4 bytes)
		batchBuffer.Write(cp.htonl(numberInt))

		// Date of Birth (10 bytes)
		dateOfBirthBytes := []byte(bet.DateOfBirth)
		batchBuffer.Write(dateOfBirthBytes)

		// Name: length (4 bytes) + actual name
		nameBytes := []byte(bet.Name)
		batchBuffer.Write(cp.htonl(len(nameBytes)))
		batchBuffer.Write(nameBytes)

		// Lastname: length (4 bytes) + actual lastname
		lastnameBytes := []byte(bet.Lastname)
		batchBuffer.Write(cp.htonl(len(lastnameBytes)))
		batchBuffer.Write(lastnameBytes)
	}

	// Send batch
	if err := cp.sendAll(conn, batchBuffer.Bytes()); err != nil {
		return false, err
	}

	// Server answer to batch
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("error receiving server response: %v", err)
	}

	if response != "SUCCESS\n" {
		return false, fmt.Errorf("batch was not successful, server response: %v", response)
	}

	return true, nil
}

func (cp *ClientProtocol) ConsultWinners(conn net.Conn) ([]int, error) {
	// Amount of winners (4 bytes)
	countBytes, err := cp.readNBytes(conn, 4)
	if err != nil {
		return nil, fmt.Errorf("action: read_amount_of_winners | result: fail | %w", err)
	}

	count := cp.ntohl(countBytes)	
	
	winners := make([]int, 0, count)

	for i := 0; i < int(count); i++ {
		//DNI of winner (4 byte)
		dniBytes, err := cp.readNBytes(conn, 4)
		if err != nil {
			return nil, fmt.Errorf("action: read_winners_DNI | result: fail | DNI %d: %w", i+1, err)
		}
		dni := cp.ntohl(dniBytes)
		winners = append(winners, dni)
	}

	return winners, nil
}