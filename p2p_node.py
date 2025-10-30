import socket
import threading
import json
from collections import defaultdict
from time import sleep
from flexibleChunkReader import FlexibleChunkReader


START_PORT = 5000
END_PORT = 5060


class P2PNode:
    def __init__(self, host='localhost'):
        self.host = host
        self.port = START_PORT
        self.peers = set()  # ip:port
        self.chunks = {}  # {chunk_hash: data}
        self.running = False
        self.socket = None
        self.nodeLog = True
        
    def start(self):
        """Start Node"""
        self.running = True

        self._create_start_listening_socket()

        # listen for new connections
        listener_thread = threading.Thread(target=self._listen_for_connections)
        listener_thread.daemon = True
        listener_thread.start()
        
        # discover peers
        discovery_thread = threading.Thread(target=self._discover_peers)
        discovery_thread.daemon = True
        discovery_thread.start()        
        
        # check peers live
        check_peers_live = threading.Thread(target=self._check_peers_live)
        check_peers_live.daemon = True
        check_peers_live.start()


    def _create_start_listening_socket(self):

        while(END_PORT+1 - self.port ):
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.socket.bind((self.host, self.port))
                self.socket.listen(5)
                self.log(f"🟢 Node starts on:{self.host}:{self.port}")
                break

            except:
                self.port +=1
            
        else:
            self.log('❌ Not available port !' , "red")
            self.stop()
        


    def _listen_for_connections(self):
        while self.running:
            try:
                client_socket, address = self.socket.accept()
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address)
                )
                thread.daemon = True
                thread.start()
            except:
                self.log("Error listening ...")
                self.stop()
                break
                
    def _handle_client(self, client_socket, address):
        try:
            data = client_socket.recv(4096).decode('utf-8')
            if not data:
                return
                
            message = json.loads(data)
            response = self._process_message(message)
            
            client_socket.send(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.log(f"❌ Problem handling client!: {e}")
        finally:
            client_socket.close()
            
    def _process_message(self, message):
        msg_type = message.get('type')
        
        if msg_type == 'PEER_DISCOVERY':
            # adding new peer
            peer_addr = message.get('address')
            self.peers.add(peer_addr)

            self.log(f"🤝 New Peer found:{peer_addr}" , 'green')
            return {'status': 'ok', 'address': f"{self.host}:{self.port}"}
            
        elif msg_type == 'STORE_CHUNK':
            # ذخیره chunk
            chunk_hash = message.get('hash')
            chunk_data = message.get('data')
            self.chunks[chunk_hash] = chunk_data
            print(f"💾 Chunk ذخیره شد: {chunk_hash[:8]}...")
            return {'status': 'stored'}
            
        elif msg_type == 'GET_CHUNK':
            # بازیابی chunk
            chunk_hash = message.get('hash')
            data = self.chunks.get(chunk_hash)
            return {'status': 'ok', 'data': data}
            
        elif msg_type == 'MAP_TASK':
            pass
            # # اجرای task پردازشی
            # chunk_hash = message.get('chunk_hash')
            # task_type = message.get('task_type')
            # chunk_data = self.chunks.get(chunk_hash, '')
            
            # result = self._execute_map_task(chunk_data, task_type)
            # return {'status': 'ok', 'result': result}
            
        elif msg_type == 'LIST_CHUNKS':
            # لیست chunkهای موجود
            return {'status': 'ok', 'chunks': list(self.chunks.keys())}        
        
        elif msg_type == 'PEER_PING':
            return {'status': 'ok'}
            
        return {'status': 'unknown_command'}
    
    
    def _discover_peers(self):

        while self.running:
            try:
                """Dicover new nodes"""
                for port in range(START_PORT, END_PORT):
                    if port != self.port:
                        peer_addr = f"localhost:{port}"
                        if(peer_addr not in self.peers):
                            try:
                                response = self._send_message(peer_addr, {
                                    'type': 'PEER_DISCOVERY',
                                    'address': f"{self.host}:{self.port}"
                                })
                                if response and response.get('status') == 'ok':
                                    self.peers.add(peer_addr)
                                    self.log(f"🤝 New Peer found:{peer_addr}", 'green')
                            except:
                                pass
            except:
                self.log("Error finding node ...")
                break
            finally:
                sleep(2)    
                
    def _check_peers_live(self):

        while self.running:
            for peer_addr in self.peers.copy():
                try:
                    response = self._send_message(peer_addr, {
                        'type': 'PEER_PING',
                        'address': f"{self.host}:{self.port}"
                    } , 1)
                    if not response or response.get('status') != 'ok':
                        self.peers.remove(peer_addr)
                        self.log(f"Node disconnected : {peer_addr}")
                except:
                    self.peers.remove(peer_addr)
                    self.log("Error finding node ...", 'red')
                    self.log(f"Node disconnected : {peer_addr}", "red")
                finally:
                    sleep(3)

        
    def _send_message(self, peer_addr, message, timeout=2):
        """send message to other peers"""
        try:
            host, port = peer_addr.split(':')
            port = int(port)
            
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(timeout)
            client.connect((host, port))
            
            client.send(json.dumps(message).encode('utf-8'))
            response = client.recv(4096).decode('utf-8')
            client.close()
            
            return json.loads(response)
        except Exception as e:
            return None
    
    
    def log(self, message, color = "white"):

        if(not self.nodeLog ): return
        colors = {
            "red": "\033[91m",
            "green": "\033[92m",
            "yellow": "\033[93m",
            "blue": "\033[94m",
            "magenta": "\033[95m",
            "cyan": "\033[96m",
            "white": "\033[97m",
        }
        reset = "\033[0m"
        color_code = colors.get(color.lower(), colors["white"])
        print(f"\r{color_code}{message}{reset}")


    def stop(self):
        """Stop Node"""
        self.running = False
        if self.socket:
            self.socket.close()
        self.log("🔴 Node Stoped !" , "red")


