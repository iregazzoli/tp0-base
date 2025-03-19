import logging
import socket

class ServerProtocol:
    def recv_bet(self, client_sock):
        """
        Receive bet message from the client
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
            # CLI_ID (4 bytes)
            cli_id_bytes = recv_exact(client_sock, 4)
            cli_id = int.from_bytes(cli_id_bytes, byteorder='big')

            # DNI (4 bytes)
            dni_bytes = recv_exact(client_sock, 4)
            dni = int.from_bytes(dni_bytes, byteorder='big')

            # bet number (4 bytes)
            number_bytes = recv_exact(client_sock, 4)
            number = int.from_bytes(number_bytes, byteorder='big')

            # birthdate (10 bytes)
            date_of_birth_bytes = recv_exact(client_sock, 10)
            date_of_birth = date_of_birth_bytes.decode('utf-8')

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

            return {
                'cli_id': cli_id,
                'dni': dni,
                'number': number,
                'date_of_birth': date_of_birth,
                'name': name,
                'lastname': lastname
            }

        except ConnectionError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
            return None