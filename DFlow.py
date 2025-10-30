import json
import os
from datetime import datetime
from typing import List, Optional, Dict
from flexibleChunkReader import FlexibleChunkReader

class DFlow:

    def __init__(self, filepath: str, file_hash: str, 
                 total_chunks: int, chunk_size: int,
                 mode: str = 'line', delimiter: str = '\n',
                 metadata: Optional[Dict] = None):
        self.filepath = filepath
        self.file_hash = file_hash
        self.total_chunks = total_chunks
        self.chunk_size = chunk_size
        self.mode = mode
        self.delimiter = delimiter
        self.added_at = datetime.now().isoformat()
        self.metadata = metadata or {}
    
    def to_dict(self) -> dict:
        return {
            'filepath': self.filepath,
            'file_hash': self.file_hash,
            'total_chunks': self.total_chunks,
            'chunk_size': self.chunk_size,
            'mode': self.mode,
            'delimiter': self.delimiter,
            'added_at': self.added_at,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        dflow = cls(
            filepath=data['filepath'],
            file_hash=data['file_hash'],
            total_chunks=data['total_chunks'],
            chunk_size=data['chunk_size'],
            mode=data.get('mode', 'line'),
            delimiter=data.get('delimiter', '\n'),
            metadata=data.get('metadata', {})
        )
        dflow.added_at = data.get('added_at', datetime.now().isoformat())
        return dflow
    
    def __repr__(self):
        return f"DFlow({os.path.basename(self.filepath)}, {self.total_chunks} chunks)"


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
                    self.dflows = [DFlow.from_dict(d) for d in data]
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
    
    if not os.path.exists(filepath):
        print(f"❌ Not found: {filepath}")
        return None
    

    reader = FlexibleChunkReader(filepath, items_per_chunk=items_per_chunk, 
                                 mode=mode, delimiter=delimiter)
    
    file_hash = reader.get_file_hash('md5')
    
    info = reader.get_file_info()
    
    dflow = DFlow(
        filepath=filepath,
        file_hash=file_hash,
        total_chunks=info['total_chunks'],
        chunk_size=items_per_chunk,
        mode=mode,
        delimiter=delimiter,
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


