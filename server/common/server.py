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
        self.running = True
        self.protocol = ServerProtocol()
        manager = multiprocessing.Manager()
        self.clients_ready_for_draw = manager.Value('i', 0)
        self.winner_number = manager.Value('i', None)  
        self.client_sockets = []
        self.ready_event = multiprocessing.Event()
        self.lock = multiprocessing.Lock()

    def run(self):
        signal_thread = threading.Thread(target=self.accept_connections)
        signal_thread.start()

        self.wait_for_signals()
        self.shutdown()

    def wait_for_signals(self):
        signal.signal(signal.SIGTERM, self.handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.handle_shutdown_signal)
        # Block to avoid busy wait
        signal.pause()

    def handle_shutdown_signal(self, signum, frame):
        logging.info("action: shutdown | starting closure of server.")
        self.running = False

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
            addr = client_sock.getpeername()
            logging.info(f'action: accept_connection | result: success | ip: {addr[0]}')

            all_batches = self.protocol.recv_batches(client_sock)

            amount_of_bets = 0

            for bets in all_batches:
                amount_of_bets += len(bets)
                store_bets(bets)

            logging.info(f"action: apuesta_recibida | result: success | cantidad: {amount_of_bets}")

            lottery_confirmation = self.protocol.recv_lottery_confirmation(client_sock)
            if lottery_confirmation:
                logging.info(f'action: lottery_confirmation | result: success | client: {addr[0]}')
                with self.lock:
                    self.clients_ready_for_draw.value += 1
                    if self.clients_ready_for_draw.value == 2:
                        self.winner_number.value = self.run_draw()
                        self.ready_event.set()

            self.ready_event.wait()  # Wait till all clients are ready to start the lottery
            self.protocol.send_winner(client_sock, self.winner_number.value)
            
            with self.lock:
                self.clients_ready_for_draw.value -= 1
                if self.clients_ready_for_draw.value == 0:
                    # Reiniciar lógica para que pueda ejecutarse nuevamente
                    self.winner_number.value = None
                    self.ready_event.clear()

        except ValueError as e:
            logging.error(f"action: process_batches | result: fail | error: {e}")
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            logging.info(f"Closing connection with {addr[0]}")
            client_sock.close()

    def run_draw(self):
        for bet in load_bets():
            if has_won(bet):
                logging.info(f"action: draw_completed | result: success | winner: {bet.number}")
                return bet.number

    def close_client_sockets(self):
        # Clear client sockets after closing
        for client_sock in self.client_sockets:
            try:
                if client_sock.fileno() != -1:  # Avoid [Errno 9] Bad file descriptor
                    client_sock.close()
                    logging.info(f"action: close_client_socket | result: success | client: {client_sock.getpeername()}")
                else:
                    logging.warning(f"action: close_client_socket | result: skipped | reason: socket already closed or invalid | client: {client_sock.getpeername()}")
            except Exception as e:
                logging.error(f"Error closing client socket: {e}")
        self.client_sockets.clear()
