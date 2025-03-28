import socket
import logging
import os
import signal
import sys
from .protocol import ServerProtocol
from .utils import *
import threading

CLIENTS_TOTAL = int(os.environ.get("CLIENTS_TOTAL", "5"))

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self.protocol = ServerProtocol()
        self._threads = [] 
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(CLIENTS_TOTAL)
        self._winners = None
        self._client_sockets = []


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
                client_sock, client_id  = self.__accept_new_connection()
            except OSError:
                if not self._running:
                    break
            client_thread = threading.Thread(target=self.__handle_client_connection, args=(client_sock, client_id)) 
            client_thread.start()                                                                 
            self._threads.append(client_thread) 

    def __handle_client_connection(self, client_sock, client_id):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | client_id: {client_id}')

            all_batches = self.protocol.recv_batches(client_sock, client_id)
            amount_of_bets = 0

            for bets in all_batches:
                amount_of_bets += len(bets)
                with self._lock: 
                    store_bets(bets)

            logging.info(f"action: apuesta_recibida | result: success | cantidad: {amount_of_bets} | client_id: {client_id}") 

            self._barrier.wait() 

            # Aca el unico error es que todos los clientes están haciendo el sorteo, lo ideal seria que uno solo lo haga pero por falta de tiempo lo dejo asi
            self._run_draw()

            agency_id = all_batches[0][0].agency
            winners = self._winners.get(agency_id, [])
            self.protocol.send_winners(client_sock, winners, agency_id)

        except ValueError as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {amount_of_bets} | error: {e}")
        except OSError as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {amount_of_bets} | error: {e}")
        except RuntimeError as e:
            logging.error("action: receive_message | result: fail | error: %s", format(e))

        finally:
            try:
                client_sock.close()
                logging.info(f"Client socket closed for client_id: {client_id}")
            except Exception as e:
                logging.error(f"Error closing client socket for client_id: {client_id}: {e}")


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
        self._client_sockets.append(c)
        client_id = self.protocol.recv_client_id(c)
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]} | client_id: {client_id}')
        
        return c, client_id
    
    def _run_draw(self):
        winners_by_agency = {}
        logging.info("action: sorteo | result: success")

        with self._lock: 
            all_bets = list(load_bets())
        for bet in all_bets:
            if has_won(bet):
                agency_id = bet.agency
                winners_by_agency.setdefault(agency_id, []).append(int(bet.document))
        
        self._winners = winners_by_agency

    def _join_client_threads(self):
        for thread in self._threads.copy():
            thread.join()
            self._threads.remove(thread)

    def _close_client_sockets(self):
        for sock in self._client_sockets.copy():
            try:
                sock.close()
                logging.info("Closed client socket %s", sock)
            except Exception as e:
                logging.error("Error closing client socket: %s", e)
            self._client_sockets.remove(sock)

    def _handle_shutdown(self, signum, frame):
        logging.info("action: shutdown_server | result: shutting down")
        self._server_socket.close()
        self._running = False
        self._close_client_sockets()
        #Abort barrier to avoid clients stuck in it when reciving a Sigterm
        try:
            self._barrier.abort()
        except Exception as e:
            logging.error("Error aborting barrier: %s", e)
        self._join_client_threads()
        logging.info("action: shutdown_server | result: all client threads joined")