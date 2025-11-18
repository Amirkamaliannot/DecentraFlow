from setting import Dflow_chunks_queue_limit , chunk_size
import json
import os
from datetime import datetime
from typing import List, Optional, Dict
from flexibleChunkReader import FlexibleChunkReader
from chunklist import Chunk , ChunkList , FileChunkList
import random
from functions import is_file_in_my_disk
    

class DFlow:

    def __init__(self, filepath: str, file_hash: str, 
                 total_chunks: int, chunk_size: int,
                 mode: str = 'line', delimiter: str = '\n',
                 metadata: Optional[Dict] = None,
                 fileHandle: FlexibleChunkReader | None = None,
                 script:str = 
                 '''sum=0
                        for input in inputs:
                            sum += input
                        output.append(sum)'''
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
        self.chunks_all:FileChunkList = FileChunkList(file_hash)
        self.chunks_queue:ChunkList = ChunkList(file_hash)
        self.chunks_queue_limit = Dflow_chunks_queue_limit
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
            fileHandle = fileHandle
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

    def start_queue(self):
        while (True):
            try:
                for chunk in self.chunks_queue[:]:
                    if(self.run_over_chunk(chunk)):
                        self.chunks_all.add(chunk)
                        self.chunks_queue.remove_by_index(chunk.index)
                        self.get_new_chunks()
                        break
            except:
                break

    def fill_chunks_queue(self):
        print(len(self.unused_chunck_list))
        print(len(self.chunks_queue._chunks))
        print(self.total_chunks)
        while(True):
            if(len(self.chunks_queue) < self.chunks_queue_limit):
                self.get_new_chunks()
            else:
                break
        print(len(self.unused_chunck_list))

    def get_new_chunks(self):
        index = self.get_random_unused_chunck()
        print(index)
        if(self.fileHandle):
            chunk_data = self.fileHandle.read_items(index)
            chunk = Chunk(index, chunk_data)
            self.add_to_chunks_queue(chunk)
            self.unused_chunck_list.remove(index)
        else:
            #get from network
            pass
        

    def add_to_chunks_queue(self, chunk:Chunk):
        if(len(self.chunks_queue) < self.chunks_queue_limit):
            if(self.chunks_queue.add(chunk)):
                return True
        return False
    
    def run_over_chunk(self, chunk:Chunk):
        inputs = chunk.content
        output = []

        try:
            exec(self.script)
        except:
            return False
        
        chunk.result = output
        return True

    def update_unused_chunck_list(self):
        existing_indexes = set()
        if os.path.exists(self.file_hash+".data2"):
            try:
                with open(self.file_hash+".data2", 'r', encoding='utf-8') as f:
                    for line in f:
                        c = json.loads(line)
                        existing_indexes.add(c["index"])
            except:pass
        
        for i in self.chunks_queue:
            existing_indexes.add(i.index)

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


