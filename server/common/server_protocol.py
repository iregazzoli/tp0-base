import logging
from .utils import *
import socket

class ServerProtocol:
    def recv_exact(self, sock, num_bytes):
        """
        Helper function to receive a specific number of bytes synchronously
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
            total_bets_received = 0  
            while True:

                bets = self.recv_batch(client_sock)
                if not bets:
                    raise ValueError("Batch processing failed.")

                store_bets(bets)

                total_bets_received += len(bets)

                batch_signal = client_sock.recv(1)
                if batch_signal == b'\x00':  
                    break
                elif batch_signal != b'\x01':
                    raise ValueError("Invalid batch signal received")
            client_sock.sendall(b"SUCCESS")

            return total_bets_received  

        except Exception as e:
            logging.error(f"action: recv_batches | result: fail | error: {e}")
            return 0 



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
            logging.error(f"Protocol | action: receive_message | result: fail | error: {e}")
            return None

    def recv_lottery_confirmation(self, client_sock):
        try:
            ready_byte = client_sock.recv(1)
            if ready_byte and ready_byte[0] == 1:
                return True
            else:
                logging.error("Protocol | action: recv_lottery_confirmation | result: fail | reason: invalid byte")
                return False
        except Exception as e:
            logging.error(f"Protocol | action: recv_lottery_confirmation | result: fail | error: {e}")
            return False

    def send_winners(self, client_sock, client_id, winners):
        try:
            client_sock.sendall(b'\x01')

            logging.info(f"{client_id} Sending LEN: {len(winners)}")
            num_winners = socket.htonl(len(winners))
            client_sock.sendall(num_winners.to_bytes(4, 'big'))
            
            buffer = bytearray()
            winners_info = [] # just for logging

            for bet in winners:
                logging.info(f"{client_id} Sending NUM: {bet.number}")
                winner_number_bytes = socket.htonl(bet.number)
                buffer.extend(winner_number_bytes.to_bytes(4, 'big'))

                logging.info(f"{client_id} Sending DNI: {bet.document}")
                winner_dni_bytes = socket.htonl(int(bet.document))  
                buffer.extend(winner_dni_bytes.to_bytes(4, 'big'))

                winners_info.append(f"{bet.document}-{bet.number}")

            client_sock.sendall(buffer)

            logging.info(f"Protocol | action: send_winner | result: success | client: {client_id}, winners: {', '.join(winners_info)}")
        except Exception as e:
            logging.error(f"Protocol | action: send_winner | result: fail | error: {e}")