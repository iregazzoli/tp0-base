import socket
import logging
import signal
from .utils import *
from .server_protocol import ServerProtocol

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_socket.settimeout(5)
        self.running = True 
        self.protocol = ServerProtocol()

        signal.signal(signal.SIGTERM, self.handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.handle_shutdown_signal)

    def run(self):
        while self.running:
            client_sock = self.__accept_new_connection()
            if client_sock: #TODO create a task or thread per connection to process the in parallel.
                self.__handle_client_connection(client_sock)
        self.shutdown()

    def handle_shutdown_signal(self, signum, frame):
        logging.info("action: shutdown | starting closure of server.") #TODO REMOVE THIS LATER
        self.running = False 

    def shutdown(self):
        # Clean up server resources
        logging.info("Closing server and liberating resorces.") #TODO REMOVE THIS LATER
        self._server_socket.close()
        logging.info("Sucesfully closed server.") #TODO REMOVE THIS LATER

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and close the socket.

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            logging.info(f'action: accept_connection | result: success | ip: {addr[0]}')

            bet_info = self.protocol.recv_bet(client_sock)
            # bet_info = self.recv_bet(client_sock)
            if bet_info:
                bet = Bet(
                    agency=bet_info['cli_id'],
                    first_name=bet_info['name'],
                    last_name=bet_info['lastname'],
                    document=str(bet_info['dni']),
                    birthdate=bet_info['date_of_birth'],
                    number=str(bet_info['number'])
                )

                store_bets([bet])
                logging.info(f"action: apuesta_almacenada | result: success | dni: {bet_info['dni']} | numero: {bet_info['number']}") #catedra

                client_sock.sendall(b"SUCCESS\n")
            else:
                client_sock.sendall(b"FAIL\n")

        except OSError as e:
           logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        # Connection arrived
        try:
            logging.info('action: accept_connections | result: in_progress')
            client_sock, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return client_sock
        except socket.timeout:
            # No connections within the timeout period
            logging.info('action: accept_connections | result: timeout') #TODO REMOVE THIS LATER
            return None
        except BlockingIOError:
            # Other non-blocking accept exception
            return None
        
    # def recv_bet(self, client_sock):
    #     """
    #     Receive bet message from the client
    #     """
    #     # Helper function to receive a specific number of bytes
    #     def recv_exact(sock, num_bytes):
    #         buffer = bytearray()
    #         while len(buffer) < num_bytes:
    #             packet = sock.recv(num_bytes - len(buffer))
    #             if not packet:
    #                 raise ConnectionError("Connection closed while receiving data")
    #             buffer.extend(packet)
    #         return buffer

    #     try:
    #         # CLI_ID (4 bytes)
    #         cli_id_bytes = recv_exact(client_sock, 4)
    #         cli_id = int.from_bytes(cli_id_bytes, byteorder='big')

    #         # DNI (4 bytes)
    #         dni_bytes = recv_exact(client_sock, 4)
    #         dni = int.from_bytes(dni_bytes, byteorder='big')

    #         # bet number (4 bytes)
    #         number_bytes = recv_exact(client_sock, 4)
    #         number = int.from_bytes(number_bytes, byteorder='big')

    #         # birthdate (10 bytes)
    #         date_of_birth_bytes = recv_exact(client_sock, 10)
    #         date_of_birth = date_of_birth_bytes.decode('utf-8')

    #         # name size (4 bytes) and name (n bytes)
    #         name_length_bytes = recv_exact(client_sock, 4)
    #         name_length = int.from_bytes(name_length_bytes, byteorder='big')
    #         name_bytes = recv_exact(client_sock, name_length)
    #         name = name_bytes.decode('utf-8')

    #         # lastname size (4 bytes) and lastname (m bytes)
    #         lastname_length_bytes = recv_exact(client_sock, 4)
    #         lastname_length = int.from_bytes(lastname_length_bytes, byteorder='big')
    #         lastname_bytes = recv_exact(client_sock, lastname_length)
    #         lastname = lastname_bytes.decode('utf-8')

    #         return {
    #             'cli_id': cli_id,
    #             'dni': dni,
    #             'number': number,
    #             'date_of_birth': date_of_birth,
    #             'name': name,
    #             'lastname': lastname
    #         }

    #     except ConnectionError as e:
    #         logging.error(f"action: receive_message | result: fail | error: {e}")
    #         return None

