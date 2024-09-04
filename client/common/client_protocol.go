package common

import (
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

func (cp *ClientProtocol) SendBet(
	conn net.Conn,
	cliID string,
	dni string,
	name string,
	lastname string,
	dateOfBirth string,
	number string,
) (bool, error) {
	// Convert clientId, dni and number to int
	cliIDInt, err := strconv.Atoi(cliID)
	if err != nil {
		return false, fmt.Errorf("error converting CLI_ID: %v", err)
	}

	dniInt, err := strconv.Atoi(dni)
	if err != nil {
		return false, fmt.Errorf("error converting DNI: %v", err)
	}

	numberInt, err := strconv.Atoi(number)
	if err != nil {
		return false, fmt.Errorf("error converting number: %v", err)
	}

	// 4 bytes
	cliIDBytes := cp.htonl(cliIDInt)
	if err := cp.sendAll(conn, cliIDBytes); err != nil {
		return false, err
	}

	// 4 bytes
	dniBytes := cp.htonl(dniInt)
	if err := cp.sendAll(conn, dniBytes); err != nil {
		return false, err
	}

	// 4 bytes
	numberBytes := cp.htonl(numberInt)
	if err := cp.sendAll(conn, numberBytes); err != nil {
		return false, err
	}

	// 10 bytes
	dateOfBirthBytes := []byte(dateOfBirth)
	if err := cp.sendAll(conn, dateOfBirthBytes); err != nil {
		return false, err
	}

	// send bytes of name + name
	nameBytes := []byte(name)
	nameLengthBytes := cp.htonl(len(nameBytes))
	if err := cp.sendAll(conn, nameLengthBytes); err != nil {
		return false, err
	}
	if err := cp.sendAll(conn, nameBytes); err != nil {
		return false, err
	}

	// send bytes of lastname + lastname
	lastnameBytes := []byte(lastname)
	lastnameLengthBytes := cp.htonl(len(lastnameBytes))
	if err := cp.sendAll(conn, lastnameLengthBytes); err != nil {
		return false, err
	}
	if err := cp.sendAll(conn, lastnameBytes); err != nil {
		return false, err
	}

	// Wait for server response
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("error receiving server response: %v", err)
	}

	// Check if the response is "SUCCESS\n"
	if response != "SUCCESS\n" {
		return false, fmt.Errorf("bet was not successful, server response: %v", response)
	}

	return true, nil
}
