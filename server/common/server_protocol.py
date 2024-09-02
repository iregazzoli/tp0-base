import logging
from .utils import Bet
import socket

class ServerProtocol:
    def recv_exact(self, sock, num_bytes):
        """
        Helper function to receive a specific number of bytes
        """
        buffer = bytearray()
        while len(buffer) < num_bytes:
            packet = sock.recv(num_bytes - len(buffer))
            if not packet:
                raise ConnectionError("Connection closed while receiving data")
            buffer.extend(packet)
        return buffer
     
    def recv_batches(self, client_sock):
        try:
            # (4 bytes)
            num_batches_bytes = self.recv_exact(client_sock, 4)
            num_batches = int.from_bytes(num_batches_bytes, byteorder='big')

            all_batches = [] # Array of array of bets

            for _ in range(num_batches):
                bets = self.recv_batch(client_sock) #array of bets
                if not bets:
                    client_sock.sendall(b"FAIL\n")
                    raise ValueError("Batch processing failed.")
                
                all_batches.append(bets)
                client_sock.sendall(b"SUCCESS\n")
            return all_batches

        except Exception as e:
            logging.error(f"action: recv_batches | result: fail | error: {e}")
            return  []


    def recv_batch(self, client_sock):
        try:
            # amount of bets (4 bytes)
            num_bets_bytes = self.recv_exact(client_sock, 4)
            num_bets = int.from_bytes(num_bets_bytes, byteorder='big')

            bets = []
            for _ in range(num_bets):
                # CLI_ID (4 bytes)
                cli_id_bytes = self.recv_exact(client_sock, 4)
                cli_id = int.from_bytes(cli_id_bytes, byteorder='big')

                # name size (4 bytes) and name (n bytes)
                name_length_bytes = self.recv_exact(client_sock, 4)
                name_length = int.from_bytes(name_length_bytes, byteorder='big')
                name_bytes = self.recv_exact(client_sock, name_length)
                name = name_bytes.decode('utf-8')

                # lastname size (4 bytes) and lastname (m bytes)
                lastname_length_bytes = self.recv_exact(client_sock, 4)
                lastname_length = int.from_bytes(lastname_length_bytes, byteorder='big')
                lastname_bytes = self.recv_exact(client_sock, lastname_length)
                lastname = lastname_bytes.decode('utf-8')
                
                # DNI (4 bytes)
                dni_bytes = self.recv_exact(client_sock, 4)
                dni = int.from_bytes(dni_bytes, byteorder='big')
                
                # birthdate (10 bytes)
                date_of_birth_bytes = self.recv_exact(client_sock, 10)
                date_of_birth = date_of_birth_bytes.decode('utf-8')

                # bet number (4 bytes)
                number_bytes = self.recv_exact(client_sock, 4)
                number = int.from_bytes(number_bytes, byteorder='big')

                bet = Bet(
                    agency=str(cli_id),
                    first_name=name,
                    last_name=lastname,
                    document=str(dni),
                    birthdate=date_of_birth,
                    number=str(number)
                )
                bets.append(bet)
            return bets

        except ConnectionError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
            return None

    def recv_lottery_confirmation(self, client_sock):
        try:
            ready_byte = client_sock.recv(1)
            if ready_byte and ready_byte[0] == 1:
                logging.info("action: recv_lottery_confirmation | result: success")
                return True
            else:
                logging.error("action: recv_lottery_confirmation | result: fail | reason: invalid byte")
                return False
        except Exception as e:
            logging.error(f"action: recv_lottery_confirmation | result: fail | error: {e}")
            return False
        
    def send_winner(self, client_sock, winner_number):
        try:
            winner_number_bytes = socket.htonl(winner_number)
            client_sock.sendall(winner_number_bytes.to_bytes(4, 'big'))
            logging.info(f"action: send_winner | result: success | number: {winner_number}")
        except Exception as e:
            logging.error(f"action: send_winner | result: fail | error: {e}")
