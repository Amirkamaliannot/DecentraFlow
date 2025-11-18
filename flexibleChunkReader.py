import os
import hashlib
from typing import Iterator, Optional, Tuple, Union
# import array
# import zlib
import mmap
import sqlite3

class FlexibleChunkReader:
    
    def __init__(self, filepath: str, 
                 items_per_chunk: int = 2048, 
                 delimiter: Union[str, bytes, None] = '\n',
                 mode: str = 'line',
                 total_items = 0 #just for loading
                 ):

        self.filepath = filepath
        self.items_per_chunk = items_per_chunk
        self.delimiter = delimiter
        self.mode = mode
        self.file_size = os.path.getsize(filepath)
        self.total_items = total_items

        if mode == 'line':
            self.delimiter = '\n'
        elif mode == 'csv':
            self.delimiter = '\n'
        
        # creating and saving indexes
        self.hash = self.get_file_hash()
        self.conn = sqlite3.connect(f"{self.hash}.db")
        self.DBcreate_table()
        self._load_item_positions_DB()
        if(hasattr(self, "item_positions")):
            self.total_items = len(self.item_positions) - 1
            self.total_chunks = (self.total_items + self.items_per_chunk - 1) // self.items_per_chunk
        else:
            self._build_index()
            self._save_item_positionsDB()

    # def handle_index

    def _build_index(self):
        print(f"🔍 Creating index ({self.mode} mode)...")
        
        if self.mode == 'byte':
            self.total_chunks = (self.file_size + self.items_per_chunk - 1) // self.items_per_chunk
            self.item_positions = None
            print(f"✅ Byte mod: {self.total_chunks} chunk")
            return
        
        self.item_positions = [0]
        if self.mode in ['line', 'csv']:
            with open(self.filepath, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    pos = 0
                    while pos < len(mm):
                        next_pos = mm.find(b'\n', pos)
                        if next_pos == -1:
                            if pos < len(mm):
                                self.item_positions.append(len(mm))
                            break
                        self.item_positions.append(next_pos + 1)
                        pos = next_pos + 1
        
        elif self.mode == 'token':
            delimiter_bytes = self.delimiter.encode('utf-8') if isinstance(self.delimiter, str) else self.delimiter
            
            with open(self.filepath, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    pos = 0
                    while pos < len(mm):
                        next_pos = mm.find(delimiter_bytes, pos)
                        if next_pos == -1:
                            if pos < len(mm):
                                self.item_positions.append(len(mm))
                            break
                        self.item_positions.append(next_pos + len(delimiter_bytes))
                        pos = next_pos + len(delimiter_bytes)
        
        self.total_items = len(self.item_positions) - 1
        self.total_chunks = (self.total_items + self.items_per_chunk - 1) // self.items_per_chunk
        
        print(f"✅ index created:{self.total_items:,} items، {self.total_chunks} chunk")
    
    def DBcreate_table(self):
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", 
                ('indexes',)
            )
            if cur.fetchone() is None: 
                cur.execute("""
                CREATE TABLE indexes (
                    id INTEGER PRIMARY KEY,
                    offset INTEGER NOT NULL
                );
                """)
                self.conn.commit()
        finally:
            cur.close()

    def _load_item_positions_DB(self):
        cur = self.conn.cursor()
        try:
            cur.execute(
                "select * from indexes order by id"
                )
            rows = cur.fetchall()
            if(len(rows)):
                self.item_positions = [0]
                for row in rows:
                    self.item_positions.append(row[1])
        finally:
            cur.close()  

    def _save_item_positionsDB(self):
        if not self.item_positions:
            return
        cur = self.conn.cursor()
        try:
            for idx, p in enumerate(self.item_positions):
                cur.execute(
                    """
                    INSERT INTO indexes (id, offset)
                    VALUES (?, ?)
                    """,
                    (idx, p)
                )

            # Commit once after all inserts
            self.conn.commit()

        finally:
            cur.close()
                    
    def read_chunk(self, chunk_index: int) -> Optional[str]:
        """reading one chunk"""
        if chunk_index < 0 or chunk_index >= self.total_chunks:
            return None
        
        if self.mode == 'byte':
            return self._read_chunk_bytes(chunk_index)
        else:
            return self._read_chunk_items(chunk_index)
    
    def _read_chunk_bytes(self, chunk_index: int) -> str:
        """reading chunk base on byte"""
        start_pos = chunk_index * self.items_per_chunk
        end_pos = min(start_pos + self.items_per_chunk, self.file_size)
        
        with open(self.filepath, 'rb') as f:
            f.seek(start_pos)
            data = f.read(end_pos - start_pos)
        
        return data.decode('utf-8', errors='ignore')
    
    def _read_chunk_items(self, chunk_index: int) -> str:
        """reading chunk base on item"""
        start_item = chunk_index * self.items_per_chunk
        end_item = min(start_item + self.items_per_chunk, self.total_items)
        
        if start_item >= len(self.item_positions) - 1:
            return ""
        
        start_pos = self.item_positions[start_item]
        end_pos = self.item_positions[min(end_item, len(self.item_positions) - 1)]
        
        with open(self.filepath, 'rb') as f:
            f.seek(start_pos)
            data = f.read(end_pos - start_pos)
        
        return data.decode('utf-8', errors='ignore')
    
    def read_items(self, chunk_index: int) -> list:
        """
        return each chuck items in list foramt 
        """
        chunk_data = self.read_chunk(chunk_index)
        if not chunk_data:
            return []
        
        if self.mode == 'csv':
            lines = chunk_data.strip().split('\n')
            return [line.split(',') for line in lines if line.strip()]
        
        elif self.mode == 'token':
            items = chunk_data.split(self.delimiter)
            return [item.strip() for item in items if item.strip()]
        
        elif self.mode == 'line':
            return [line for line in chunk_data.split('\n') if line.strip()]
        
        else:  # byte mode
            return [chunk_data]
    
    def iter_chunks(self, start_chunk: int = 0, end_chunk: Optional[int] = None) -> Iterator[Tuple[int, str]]:
        """Iterator for reading mutiple chunk"""
        if end_chunk is None:
            end_chunk = self.total_chunks
        
        for chunk_idx in range(start_chunk, min(end_chunk, self.total_chunks)):
            yield chunk_idx, self.read_chunk(chunk_idx)
    
    def get_chunk_hash(self, chunk_index: int) -> Optional[str]:
        """return chunk's hash"""
        chunk_data = self.read_chunk(chunk_index)
        if chunk_data is None:
            return None
        
        return hashlib.md5(chunk_data.encode('utf-8')).hexdigest()
    
    def get_chunk_metadata(self, chunk_index: int) -> Optional[dict]:
        """return meta data informations of a chunk"""
        if chunk_index < 0 or chunk_index >= self.total_chunks:
            return None
        
        chunk_data = self.read_chunk(chunk_index)
        chunk_size = len(chunk_data.encode('utf-8')) if chunk_data else 0
        
        if self.mode == 'byte':
            return {
                'chunk_index': chunk_index,
                'start_byte': chunk_index * self.items_per_chunk,
                'end_byte': min((chunk_index + 1) * self.items_per_chunk, self.file_size),
                'size_bytes': chunk_size,
                'file_hash': self.get_chunk_hash(chunk_index)
            }
        else:
            start_item = chunk_index * self.items_per_chunk
            end_item = min(start_item + self.items_per_chunk, self.total_items)
            
            return {
                'chunk_index': chunk_index,
                'start_item': start_item,
                'end_item': end_item,
                'num_items': end_item - start_item,
                'size_bytes': chunk_size,
                'hash': self.get_chunk_hash(chunk_index)
            }
    
    
    def get_file_info(self) -> dict:
        info = {
            'filepath': self.filepath,
            'file_size': self.file_size,
            'mode': self.mode,
            'delimiter': repr(self.delimiter),
            'items_per_chunk': self.items_per_chunk,
            'total_chunks': self.total_chunks,
            'file_hash': self.hash,
        }
        
        if self.mode != 'byte':
            info['total_items'] = self.total_items
            info['avg_chunk_size'] = self.file_size // self.total_chunks if self.total_chunks > 0 else 0
        
        return info
    
    def get_file_hash(self, algorithm: str = 'md5') -> str:
        if algorithm == 'md5':
            hasher = hashlib.md5()
        elif algorithm == 'sha256':
            hasher = hashlib.sha256()
        else:
            raise ValueError(f"unknouwn: {algorithm}")
        
        # استفاده از buffering سیستم‌عامل
        with open(self.filepath, 'rb', buffering=0) as f:
            # خواندن 128MB تکه‌ها
            for chunk in iter(lambda: f.read(128*1024*1024), b''):
                hasher.update(chunk)
        
        return hasher.hexdigest()

# reader = FlexibleChunkReader('best-dns-wordlist.txt', items_per_chunk=256, mode='line')