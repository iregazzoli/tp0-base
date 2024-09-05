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
        self._server_socket.settimeout(15)
        self.running = True 
        self.protocol = ServerProtocol()
        self.clients_ready_for_draw = 0
        self.client_sockets = {}    
        signal.signal(signal.SIGTERM, self.handle_shutdown_signal)
        signal.signal(signal.SIGINT, self.handle_shutdown_signal)

    def run(self):
        while self.running:
            if self.clients_ready_for_draw == 2:
                winners_by_agency = self.run_draw()
                logging.info("action: sorteo | result: success") # CATEDRA
                self.announce_winners(winners_by_agency)
                self.close_client_sockets()
                self.clients_ready_for_draw = 0 # Restart logic so it can run loterry again

            client_sock = self.__accept_new_connection()
            if client_sock: 
                client_id_bytes = self.protocol.recv_exact(client_sock, 4)
                client_id = int.from_bytes(client_id_bytes, byteorder='big')
                self.client_sockets[client_id] = client_sock #Since we don't handle clients in parallel
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

            if self.protocol.recv_lottery_confirmation(client_sock):
                logging.info(f'action: lottery_confirmation | result: success | client: {addr[0]}')
                self.clients_ready_for_draw += 1  

        except ValueError as e:
            logging.error(f"action: process_batches | result: fail | error: {e}")

        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        # finally: # Don't close client connection until it has received the winning number
        #     client_sock.close()

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
        
    def run_draw(self):
        winners_by_agency = {}
        
        for bet in load_bets():
            if has_won(bet):
                agency_id = bet.agency  
                if agency_id not in winners_by_agency:
                    winners_by_agency[agency_id] = []
                winners_by_agency[agency_id].append(bet)  
                
        return winners_by_agency
            
    def announce_winners(self, winners_by_agency):
        for client_id, client_sock in self.client_sockets.items():
            if client_id in winners_by_agency:
                winners = winners_by_agency[client_id]
                self.protocol.send_winner(client_sock, winners)

    def close_client_sockets(self):
        for client_sock in self.client_sockets.values():
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
        
        self.client_sockets.clear()


