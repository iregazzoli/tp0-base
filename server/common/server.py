import socket
import logging
import os
import signal
import sys
from .protocol import ServerProtocol
from .utils import *

CLIENTS_TOTAL = int(os.environ.get("CLIENTS_TOTAL", "5"))

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self.protocol = ServerProtocol()
        self._notified_clients = 0
        self._clients = {}

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        while self._running:
            try:
                client_sock = self.__accept_new_connection()
            except OSError:
                if not self._running:
                    break
            self.__handle_client_connection(client_sock)

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]}')

            all_batches = self.protocol.recv_batches(client_sock, addr[0])
            amount_of_bets = 0

            for bets in all_batches:
                amount_of_bets += len(bets)
                store_bets(bets)

            logging.info(f"action: apuesta_recibida | result: success | cantidad: {amount_of_bets}") 

            agency_id = all_batches[0][0].agency
            self._clients[agency_id] = client_sock
            self._notified_clients += 1
            
            if self._notified_clients == CLIENTS_TOTAL:
                self._run_draw()

        except ValueError as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {amount_of_bets} | error: {e}")
        except OSError as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {amount_of_bets} | error: {e}")

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        # logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')

        client_id = self.protocol.recv_client_id(c)
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]} | client_id: {client_id}')
        
        return c
    
    def _run_draw(self):
        winners_by_agency = {}
        all_bets = load_bets()

        for bet in all_bets:
            if has_won(bet):
                agency_id = bet.agency
                winners_by_agency.setdefault(agency_id, []).append(int(bet.document))

        for agency_id, sock in self._clients.items():
            winners = winners_by_agency.get(agency_id, [])
            self.protocol.send_winners(sock, winners, agency_id)
            # sock.close()

        self._clients.clear()
        # logging.info("action: run_draw | result: success")
        logging.info("action: sorteo | result: success")
    
    def _handle_shutdown(self, signum, frame):
        self._server_socket.close()
        self._running = False
        logging.info("action: shutdown_server | result: success")
        sys.exit(0)

