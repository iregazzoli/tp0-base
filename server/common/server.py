import socket
import logging
import signal
import multiprocessing
import threading
from .utils import *
from .server_protocol import ServerProtocol

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.running = multiprocessing.Value('b', True)
        self.protocol = ServerProtocol()
        manager = multiprocessing.Manager()
        self.clients_ready_for_draw = manager.Value('i', 0)
        self.winners = manager.dict()
        self.client_sockets = manager.list()
        self.ready_event = multiprocessing.Event()
        self.lock = multiprocessing.Lock()

    def run(self):
        signal_process = multiprocessing.Process(target=self.accept_connections)
        signal_process.start()

        self.wait_for_signals()
        self.shutdown()

    def wait_for_signals(self):
        signal.signal(signal.SIGTERM, self.handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.handle_shutdown_signal)
        # Block to avoid busy wait
        signal.pause()

    def handle_shutdown_signal(self, signum, frame):
        logging.info("action: shutdown | starting closure of server.")
        self.running.value = False

    def shutdown(self):
        # Clean up server resources
        logging.info("Closing server and liberating resources.")
        self._server_socket.close()
        self.close_client_sockets()
        logging.info("Successfully closed server.")

    def accept_connections(self):
        logging.info("Starting server loop...")

        while self.running:
            try:
                logging.info("Waiting for new connection...")
                client_sock, _ = self._server_socket.accept()
                self.client_sockets.append(client_sock)

                # Create a new process to handle the client connection
                process = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock,))
                process.start()

            except Exception as e:
                logging.error(f"action: accept_connection | result: fail | error: {e}")

    def __handle_client_connection(self, client_sock):
        try:
            client_id_bytes = self.protocol.recv_exact(client_sock, 4)
            client_id = int.from_bytes(client_id_bytes, byteorder='big')

            logging.info(f'action: accept_connection | result: success | client: {client_id}')

            with self.lock: 
                total_bets = self.protocol.recv_batches(client_sock)

            logging.info(f"action: apuesta_recibida | result: success | client: {client_id} cantidad: {total_bets}")

            lottery_confirmation = self.protocol.recv_lottery_confirmation(client_sock)
            if lottery_confirmation:
                logging.info(f'action: lottery_confirmation | result: success | client: {client_id}')
                with self.lock:
                    self.clients_ready_for_draw.value += 1
                    if self.clients_ready_for_draw.value == 5:
                        winners_by_agency = self.run_draw()
                        for key, value in winners_by_agency.items():
                            self.winners[key] = value

                        self.ready_event.set()

            self.ready_event.wait()  # Wait till all clients are ready to start the lottery

            if client_id in self.winners:
                self.protocol.send_winners(client_sock, client_id, self.winners[client_id])  
            with self.lock:
                self.clients_ready_for_draw.value -= 1
                if self.clients_ready_for_draw.value == 0:
                    self.winners = {}
                    self.ready_event.clear()

        except ValueError as e:
            logging.error(f"action: process_batches | result: fail | error: {e}")
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            logging.info(f"Closing connection with client {client_id}")
            client_sock.close()

    def run_draw(self):
        winners_by_agency = {}
        
        for bet in load_bets():
            if has_won(bet):
                agency_id = int(bet.agency) 
                if agency_id not in winners_by_agency:
                    winners_by_agency[agency_id] = []
                winners_by_agency[agency_id].append(bet)  
                
        return winners_by_agency

    def close_client_sockets(self):
        for client_sock in self.client_sockets:
            try:
                if client_sock.fileno() != -1:  
                    logging.info(f"Attempting to close client socket: {client_sock.getpeername()} | FD: {client_sock.fileno()}")
                    try:
                        client_sock.shutdown(socket.SHUT_RDWR) 
                    except OSError as e:
                        logging.warning(f"Socket already closed for client: {client_sock.getpeername()} | {e}")
                    client_sock.close()
                    logging.info(f"action: close_client_socket | result: success | client: {client_sock.getpeername()}")
                else:
                    logging.warning(f"action: close_client_socket | result: skipped | reason: socket already closed or invalid | client: {client_sock.getpeername()}")
            except OSError as e:
                if e.errno == 9: 
                    logging.error(f"Socket already closed")
                else:
                    logging.error(f"Error closing client socket: {e}")
            except Exception as e:
                logging.error(f"Unexpected error closing client socket: {e}")
        
        while len(self.client_sockets) > 0:
            self.client_sockets.pop()

