import socket
import threading
import json
import hashlib
import time
import os
from collections import defaultdict
from time import sleep

START_PORT = 5000
END_PORT = 5050

class P2PNode:
    def __init__(self, host='localhost'):
        self.host = host
        self.port = START_PORT
        self.peers = set()  # ip:port
        self.chunks = {}  # {chunk_hash: data}
        self.chunk_locations = defaultdict(set)  # {chunk_hash: {peer_addresses}}
        self.running = False
        self.socket = None
        
    def start(self):
        """Start Node"""
        self.running = True

        self._create_start_listening_socket()

        # Thread برای گوش دادن به اتصالات جدید
        listener_thread = threading.Thread(target=self._listen_for_connections)
        listener_thread.daemon = True
        listener_thread.start()
        
        # Thread برای کشف Nodeهای دیگر
        discovery_thread = threading.Thread(target=self._discover_peers)
        discovery_thread.daemon = True
        discovery_thread.start()

        self.main_loop()

    def main_loop(self):
        while(self.running):

            command = input("Enter command (h for help):")
            if(command in ["h", 'help']):
                print(
                    """
                    /create-job <file>: creating new job
                    /attach-job <job-serial>: attching to a job
                    """
            )
            sleep(2)

    def _create_start_listening_socket(self):

        while(END_PORT+1 - self.port ):
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.socket.bind((self.host, self.port))
                self.socket.listen(5)
                print(f"🟢 Node starts on:{self.host}:{self.port}")
                break

            except:
                self.port +=1
            
        else:
            print('❌ Not available port !')
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
                print("Error listening ...")
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
            print(f"❌ Problem handling client!: {e}")
        finally:
            client_socket.close()
            
    def _process_message(self, message):
        msg_type = message.get('type')
        
        if msg_type == 'PEER_DISCOVERY':
            # adding new peer
            peer_addr = message.get('address')
            self.peers.add(peer_addr)

            print(f"🤝 New Peer found:{peer_addr}")
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
            # اجرای task پردازشی
            chunk_hash = message.get('chunk_hash')
            task_type = message.get('task_type')
            chunk_data = self.chunks.get(chunk_hash, '')
            
            result = self._execute_map_task(chunk_data, task_type)
            return {'status': 'ok', 'result': result}
            
        elif msg_type == 'LIST_CHUNKS':
            # لیست chunkهای موجود
            return {'status': 'ok', 'chunks': list(self.chunks.keys())}
            
        return {'status': 'unknown_command'}
    
    def _execute_map_task(self, data, task_type):
        """اجرای وظیفه پردازشی"""
        if task_type == 'word_count':
            # شمارش کلمات
            words = data.split()
            word_count = defaultdict(int)
            for word in words:
                word = word.lower().strip('.,!?;:')
                if word:
                    word_count[word] += 1
            return dict(word_count)
        
        elif task_type == 'line_count':
            # شمارش خطوط
            return {'lines': len(data.split('\n'))}
            
        return {}
    
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
                                    print(f"🤝 New Peer found:{peer_addr}")
                            except:
                                pass
            except:
                print("Error finding node ...")
                break
            finally:
                sleep(2)
        
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
    
    def split_and_store(self, data, chunk_size=100):
        """تقسیم داده به chunkها و ذخیره توزیع‌شده"""
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            chunk_hash = hashlib.md5(chunk.encode()).hexdigest()
            chunks.append((chunk_hash, chunk))
        
        # ذخیره محلی
        for chunk_hash, chunk_data in chunks:
            self.chunks[chunk_hash] = chunk_data
        
        # توزیع بین peerها
        peer_list = list(self.peers)
        for idx, (chunk_hash, chunk_data) in enumerate(chunks):
            if peer_list:
                # انتخاب peer برای ذخیره
                peer = peer_list[idx % len(peer_list)]
                response = self._send_message(peer, {
                    'type': 'STORE_CHUNK',
                    'hash': chunk_hash,
                    'data': chunk_data
                })
                if response and response.get('status') == 'stored':
                    self.chunk_locations[chunk_hash].add(peer)
        
        print(f"📦 {len(chunks)} chunk ایجاد و توزیع شد")
        return [h for h, _ in chunks]
    
    def map_reduce(self, chunk_hashes, task_type='word_count'):
        """اجرای MapReduce روی chunkها"""
        results = []
        
        # Map Phase
        print("🗺️  Map Phase شروع شد...")
        for chunk_hash in chunk_hashes:
            # اگر chunk محلی داریم
            if chunk_hash in self.chunks:
                result = self._execute_map_task(self.chunks[chunk_hash], task_type)
                results.append(result)
            # اگر نه، از peer دیگر بگیریم
            elif chunk_hash in self.chunk_locations:
                peers = list(self.chunk_locations[chunk_hash])
                for peer in peers:
                    response = self._send_message(peer, {
                        'type': 'MAP_TASK',
                        'chunk_hash': chunk_hash,
                        'task_type': task_type
                    })
                    if response and response.get('status') == 'ok':
                        results.append(response['result'])
                        break
        
        # Reduce Phase
        print("🔄 Reduce Phase شروع شد...")
        final_result = defaultdict(int)
        for result in results:
            for key, value in result.items():
                final_result[key] += value
        
        return dict(final_result)
    
    def stop(self):
        """توقف Node"""
        self.running = False
        if self.socket:
            self.socket.close()
        print("🔴 Node Stoped !")





# مثال استفاده
if __name__ == "__main__":
    # شروع Node
    node = P2PNode()
    node.start()
    
    time.sleep(2)  # زمان برای کشف peerها
    
    # مثال: تقسیم و ذخیره یک متن
    sample_text = """
        Lorem ipsum was conceived as filler text, form for for for atted in a certain way to enable the presentation of graphic elements in documents, without the need for fo
    """
    
    # print("\n📝 شروع پردازش...")
    # chunk_hashes = node.split_and_store(sample_text, chunk_size=50)
    
    # # اجرای MapReduce برای شمارش کلمات
    # result = node.map_reduce(chunk_hashes, task_type='word_count')
    
    # print("\n📊 نتیجه شمارش کلمات:")
    # for word, count in sorted(result.items(), key=lambda x: x[1], reverse=True):
    #     print(f"  {word}: {count}")
    
    # نگه داشتن Node برای تست
    input("\n⏸️  Enter for exit ...")
    node.stop()