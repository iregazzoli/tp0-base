import logging
from .utils import Bet

class ServerProtocol:
    def recv_bet_batch(self, client_sock):
        """
        Receive a batch of bets from the client
        """
        # Helper function to receive a specific number of bytes
        def recv_exact(sock, num_bytes):
            buffer = bytearray()
            while len(buffer) < num_bytes:
                packet = sock.recv(num_bytes - len(buffer))
                if not packet:
                    raise ConnectionError("Connection closed while receiving data")
                buffer.extend(packet)
            return buffer

        try:
            # amount of bets (4 bytes)
            num_bets_bytes = recv_exact(client_sock, 4)
            num_bets = int.from_bytes(num_bets_bytes, byteorder='big')

            bets = []
            for _ in range(num_bets):
                # CLI_ID (4 bytes)
                cli_id_bytes = recv_exact(client_sock, 4)
                cli_id = int.from_bytes(cli_id_bytes, byteorder='big')

                # name size (4 bytes) and name (n bytes)
                name_length_bytes = recv_exact(client_sock, 4)
                name_length = int.from_bytes(name_length_bytes, byteorder='big')
                name_bytes = recv_exact(client_sock, name_length)
                name = name_bytes.decode('utf-8')

                # lastname size (4 bytes) and lastname (m bytes)
                lastname_length_bytes = recv_exact(client_sock, 4)
                lastname_length = int.from_bytes(lastname_length_bytes, byteorder='big')
                lastname_bytes = recv_exact(client_sock, lastname_length)
                lastname = lastname_bytes.decode('utf-8')
                
                # DNI (4 bytes)
                dni_bytes = recv_exact(client_sock, 4)
                dni = int.from_bytes(dni_bytes, byteorder='big')
                
                # birthdate (10 bytes)
                date_of_birth_bytes = recv_exact(client_sock, 10)
                date_of_birth = date_of_birth_bytes.decode('utf-8')

                # bet number (4 bytes)
                number_bytes = recv_exact(client_sock, 4)
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
