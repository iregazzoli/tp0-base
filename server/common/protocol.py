import logging
from .utils import *

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
    def send_all(self, sock, data: bytes):
        """
        Helper to send all bytes, handling short writes
        """
        total_sent = 0
        while total_sent < len(data):
            sent = sock.send(data[total_sent:])
            if sent == 0:
                raise ConnectionError("Socket connection broken during send")
            total_sent += sent

    def recv_client_id(self, sock):
        id_bytes = self.recv_exact(sock, 4)
        client_id_int = int.from_bytes(id_bytes, byteorder='big')
        return str(client_id_int)
    
    def recv_batches(self, client_sock, client_id):
        logging.info(f'Receiving batches from client {client_id}')
        all_batches = []
        while True:
            bets = self.recv_batch(client_sock)
            if bets is None:
                logging.info(f"Termination header received: no more batches from client {client_id}.")
                break
            all_batches.append(bets)
            client_sock.sendall(b"SUCCESS\n")
        return all_batches
        
    def recv_batch(self, client_sock):
        try:
            # amount of bets (4 bytes)
            num_bets_bytes = self.recv_exact(client_sock, 4)
            num_bets = int.from_bytes(num_bets_bytes, byteorder='big')

            if num_bets == 0:
                return None

            bets = []
            for _ in range(num_bets):
                # CLI_ID (4 bytes)
                cli_id_bytes = self.recv_exact(client_sock, 4)
                cli_id = int.from_bytes(cli_id_bytes, byteorder='big')

                # DNI (4 bytes)
                dni_bytes = self.recv_exact(client_sock, 4)
                dni = int.from_bytes(dni_bytes, byteorder='big')

                # bet number (4 bytes)
                number_bytes = self.recv_exact(client_sock, 4)
                number = int.from_bytes(number_bytes, byteorder='big')

                # birthdate (10 bytes)
                date_of_birth_bytes = self.recv_exact(client_sock, 10)
                date_of_birth = date_of_birth_bytes.decode('utf-8')

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
        
    def send_winners(self, sock, winners: list[int], agency_id):
        """
        Sends to client: number of winners (4 bytes) + each DNI (4 bytes)
        """
        count_bytes = len(winners).to_bytes(4, byteorder='big')
        self.send_all(sock, count_bytes)

        for dni in winners:
            dni_bytes = int(dni).to_bytes(4, byteorder='big')
            self.send_all(sock, dni_bytes)

        logging.info(f"action: send_winners | result: success | client_Id: {agency_id} | cantidad: {len(winners)}")
