from setting import Dflow_chunks_queue_limit , chunk_size
import json
import os
from datetime import datetime
from typing import List, Optional, Dict
from flexibleChunkReader import FlexibleChunkReader
from chunklist import Chunk
import random
from functions import is_file_in_my_disk
from tqdm import tqdm
import sqlite3


class DFlow:

    def __init__(self, filepath: str, file_hash: str, 
                 total_chunks: int, chunk_size: int,
                 mode: str = 'line', delimiter: str = '\n',
                 metadata: Optional[Dict] = None,
                 fileHandle: FlexibleChunkReader | None = None,
                 script:str = 
                 '''from tqdm import tqdm\nfinal=0\nfor input in tqdm(range(len(inputs))):\n final = inputs[input]\n output.append(final)'''
                 ):
        self.fileHandle=fileHandle
        self.filepath = filepath
        self.file_hash = file_hash
        self.total_chunks = total_chunks
        self.chunk_size = chunk_size
        self.mode = mode
        self.delimiter = delimiter
        self.added_at = datetime.now().isoformat()
        self.metadata = metadata or {}
        self.script = script
        self.chunks_queue_limit = Dflow_chunks_queue_limit

        print(1111111111112222222222)
        self.conn = sqlite3.connect(f"{self.file_hash}.db")
        print(1111111111112222222222333333333333)
        self.DBcreate_table()

        self.unused_chunck_list = []
        self.update_unused_chunck_list()        


    @classmethod
    def from_dict(cls, data: dict , fileHandle: FlexibleChunkReader | None = None):
        dflow = cls(
            filepath=data['filepath'],
            file_hash=data['file_hash'],
            total_chunks=data['total_chunks'],
            chunk_size=data['chunk_size'],
            mode=data.get('mode', 'line'),
            delimiter=data.get('delimiter', '\n'),
            metadata=data.get('metadata', {}),
            fileHandle = fileHandle,
            script = data.get('script', '')
        )
        dflow.added_at = data.get('added_at', datetime.now().isoformat())
        return dflow
    
    def to_dict(self) -> dict:
        return {
            'filepath': self.filepath,
            'file_hash': self.file_hash,
            'total_chunks': self.total_chunks,
            'chunk_size': self.chunk_size,
            'mode': self.mode,
            'delimiter': self.delimiter,
            'added_at': self.added_at,
            'metadata': self.metadata,
            'script': self.script,
        }

    def DBcreate_table(self):
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", 
                ('chunks',)
            )
            if cur.fetchone() is None: 
                cur.execute("""
                CREATE TABLE chunks (
                    chunk_index INTEGER PRIMARY KEY,
                    content BLOB NOT NULL,
                    result BLOB NOT NULL,
                    status INTEGER NOT NULL DEFAULT 0
                );
                """)
                self.conn.commit()
        finally:
            cur.close()

    def get_chunks_queue(self):
        cur = self.conn.cursor()
        try:
            cur.execute(
                "select chunk_index from chunks where status=0 order by chunk_index"
                )
            rows = cur.fetchall()
            out = []
            for row in rows:
                out.append(row[0])
            return out
        
        finally:
            cur.close()    
            
    def get_finished_queue(self):
        cur = self.conn.cursor()
        try:
            cur.execute(
                "select chunk_index from chunks where status=1 order by chunk_index"
                )
            rows = cur.fetchall()
            out = []
            for row in rows:
                out.append(row[0])
            return out
        
        finally:
            cur.close()

    def set_chunk_finished(self, chunk_index, result):
        cur = self.conn.cursor()
        result_blob = json.dumps(result).encode("utf-8")
        try:
            cur.execute(
                "UPDATE chunks SET status=1 ,result=? WHERE chunk_index = ?;", (result_blob,chunk_index)
                )
            self.conn.commit()
        
        finally:
            cur.close()    
            
    def set_chunk_error(self, chunk_index):
        cur = self.conn.cursor()
        try:
            cur.execute(
                "UPDATE chunks SET status=2 WHERE chunk_index = ?;", (chunk_index,)
                )
            self.conn.commit()
        
        finally:
            cur.close()



    def start_queue(self):
        while (True):
            chunks_queue = self.get_chunks_queue()
            try:
                for chunk_index in chunks_queue:
                    chunk_data = self.fileHandle.read_items(chunk_index)
                    chunk = Chunk(chunk_index, chunk_data)
                    if(self.run_over_chunk(chunk)):
                        self.set_chunk_finished(chunk_index, chunk.result)
                        self.get_new_chunks()
                        break
                    else:
                        self.set_chunk_finished(chunk_index)
                        self.get_new_chunks()

            except Exception as e:
                print(e)
                break

    def fill_chunks_queue(self):
        while(True):
            chunks_queue = self.get_chunks_queue()
            if(len(chunks_queue) < self.chunks_queue_limit):
                self.get_new_chunks()
            else:
                break

    def get_new_chunks(self):
        index = self.get_random_unused_chunck()
        if(self.fileHandle):
            chunk_data = self.fileHandle.read_items(index)
            chunk = Chunk(index, chunk_data)
            self.add_to_chunks_queue(chunk)
            self.unused_chunck_list.remove(index)
        else:
            #get from network
            pass
        

    def add_to_chunks_queue(self, chunk:Chunk):
        chunks_queue = self.get_chunks_queue()
        print(len(chunks_queue))
        if(len(chunks_queue) > self.chunks_queue_limit):return
        
        content_blob = json.dumps(chunk.content).encode("utf-8")
        result_blob = json.dumps([]).encode("utf-8")  # empty result
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO chunks (chunk_index, content, result, status)
                VALUES (?, ?, ?, 0)
                """,
                (chunk.index, content_blob, result_blob)
            )
            self.conn.commit()
        finally:
            cur.close()
    
    def run_over_chunk(self, chunk:Chunk):
        inputs = chunk.content
        output = []

        print(f"Chunk {chunk.index} running ...")
        try:
            exec(self.script)
        except Exception as e:

            print(f"Chunck failed: {e} {self.script}")
            return False
        
        print(f'chunck {chunk.index} done')
        chunk.result = output
        return True

    def update_unused_chunck_list(self):

        existing_indexes = set()
        cur = self.conn.cursor()
        try:
            cur.execute(
                "select chunk_index from chunks"
                )
            rows = cur.fetchall()
            for row in rows:
                existing_indexes.add(row[0])
        finally:
            cur.close()

        all_indexes = set(range(0, self.total_chunks))
        available = list(all_indexes - existing_indexes)
        self.unused_chunck_list = available


    def get_random_unused_chunck(self):
        if not self.unused_chunck_list:
            raise ValueError("No available index in the given range")
        return random.choice( self.unused_chunck_list)



class DFlowManager:
    
    def __init__(self, json_file: str = 'dflows.json'):
        self.json_file = json_file
        self.dflows: List[DFlow] = []
        self.load()



    def load(self):
        if os.path.exists(self.json_file):
            try:
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for dflow in data:
                        if(is_file_in_my_disk(dflow['filepath'], dflow['file_hash'])):
                            reader = FlexibleChunkReader(dflow['filepath'], items_per_chunk=dflow["chunk_size"], 
                                 mode=dflow["mode"], delimiter=dflow["delimiter"], total_items=dflow["metadata"]['total_items'])
                            self.dflows.append(DFlow.from_dict(dflow, reader))
                        else:
                            self.dflows.append(DFlow.from_dict(dflow))

                print(f"✅ {len(self.dflows)} DFlow loaded from:{self.json_file}")
            except Exception as e:
                print(f"❌ Loading Error: {e}")
                self.dflows = []
        else:
            print(f"ℹ️  file:{self.json_file} an empty list created.")
            self.dflows = []
    
    def save(self):
        try:
            data = [df.to_dict() for df in self.dflows]
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"💾 {len(self.dflows)} DFlow saved in {self.json_file}")
        except Exception as e:
            print(f"❌ Saving Error: {e}")
    
    def add(self, dflow: DFlow):
        # base on unique hash
        if self.get_by_hash(dflow.file_hash):
            print(f"⚠️  DFlow with hash {dflow.file_hash[:8]} already exist... ")
            return False
        
        self.dflows.append(dflow)
        print(f"✅ DFlow added: {dflow.filepath}")
        self.save()  # auto-save
        return True
    
    def remove(self, file_hash: str) -> bool:
        """remove base on hash"""
        initial_len = len(self.dflows)
        self.dflows = [df for df in self.dflows if df.file_hash != file_hash]
        
        if len(self.dflows) < initial_len:
            print(f"🗑️  DFlow with hash {file_hash[:8]} deleted...")
            self.save()
            return True
        
        print(f"⚠️  DFlow with hash {file_hash[:8]} not fonud...")
        return False
    
    def get_by_hash(self, file_hash: str) -> Optional[DFlow]:
        for df in self.dflows:
            if df.file_hash == file_hash:
                return df
        return None
    
    def get_by_filepath(self, filepath: str) -> Optional[DFlow]:
        for df in self.dflows:
            if df.filepath == filepath:
                return df
        return None
    
    def list_all(self) -> List[DFlow]:
        return self.dflows.copy()
    
    
    def clear(self):
        """ clear all DFlows"""
        self.dflows = []
        self.save()
        print("🗑️  All DFlows deleted!")
    
    def get_stats(self) -> dict:
        if not self.dflows:
            return {'total': 0}
        
        total_chunks = sum(df.total_chunks for df in self.dflows)
        modes = {}
        for df in self.dflows:
            modes[df.mode] = modes.get(df.mode, 0) + 1
        
        return {
            'total_dflows': len(self.dflows),
            'total_chunks': total_chunks,
            'modes': modes,
            'avg_chunks_per_dflow': total_chunks / len(self.dflows)
        }
    
    def print_summary(self):
        print("\n" + "="*60)
        print(f"📊 Summary DFlows")
        print("="*60)
        
        if not self.dflows:
            print("There is no DFlow")
            return
        
        for i, df in enumerate(self.dflows, 1):
            print(f"\n{i}. {os.path.basename(df.filepath)}")
            print(f"   Hash: {df.file_hash[:16]}...")
            print(f"   Chunks: {df.total_chunks}")
            print(f"   Mode: {df.mode}")
            print(f"   Added: {df.added_at[:19]}")
        
        stats = self.get_stats()
        print(f"\n📈 General statistics:")
        print(f"   total DFlows: {stats['total_dflows']}")
        print(f"   total Chunk: {stats['total_chunks']}")




def add_file_as_dflow(manager: DFlowManager, filepath: str, 
                      items_per_chunk: int = 512, 
                      mode: str = 'line', delimiter: str = '\n'):
    
    filepath.replace('\\ ', ' ').strip()
    if filepath.startswith('"') and filepath.endswith('"'):
        filepath = filepath[1:-1]
    elif filepath.startswith("'") and filepath.endswith("'"):
        filepath = filepath[1:-1]

    if not os.path.exists(filepath):
        print(f"❌ Not found: {filepath}")
        return None
    

    reader = FlexibleChunkReader(filepath, items_per_chunk=items_per_chunk, 
                                 mode=mode, delimiter=delimiter)
    
    info = reader.get_file_info()
    
    dflow = DFlow(
        filepath=filepath,
        file_hash=info['file_hash'],
        total_chunks=info['total_chunks'],
        chunk_size=items_per_chunk,
        mode=mode,
        delimiter=delimiter,
        fileHandle=reader,
        metadata={
            'file_size': info['file_size'],
            'total_items': info.get('total_items', 0)
        }
    )
    
    if manager.add(dflow):
        return dflow
    return None


# manager = DFlowManager('dflows_real.json')
# dflow = add_file_as_dflow(manager, test_file, items_per_chunk=500, mode='line')


