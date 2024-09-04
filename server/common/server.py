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
        Read batch of messages from a specific client socket and close the socket.

        If a problem arises in the communication with the client, the
        client socket will also be closed.
        """
        try:
            addr = client_sock.getpeername()
            logging.info(f'action: accept_connection | result: success | ip: {addr[0]}')

            all_batches = self.protocol.recv_batches(client_sock)

            amount_of_bets = 0

            for bets in all_batches:
                amount_of_bets += len(bets)
                store_bets(bets)

            logging.info(f"action: apuesta_recibida | result: success | cantidad: {amount_of_bets}") 

        except ValueError as e:
            logging.error(f"action: process_batches | result: fail | error: {e}")

        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
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
            return None
        except BlockingIOError:
            # Other non-blocking accept exception
            return None