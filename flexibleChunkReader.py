import os
import hashlib
import json
from typing import Iterator, Optional, Tuple, Union

class FlexibleChunkReader:
    
    def __init__(self, filepath: str, 
                 items_per_chunk: int = 512, 
                 delimiter: Union[str, bytes, None] = '\n',
                 mode: str = 'line'):
        """
        Args:
            filepath: مسیر فایل
            items_per_chunk: تعداد آیتم در هر chunk
            delimiter: جداکننده (مثل '\n', ',', ' ', '\t')
            mode: حالت خواندن:
                - 'line': بر اساس خط (delimiter='\n')
                - 'token': بر اساس جداکننده دلخواه
                - 'byte': بر اساس تعداد بایت ثابت
                - 'csv': برای فایل‌های CSV
        """
        self.filepath = filepath
        self.items_per_chunk = items_per_chunk
        self.delimiter = delimiter
        self.mode = mode
        self.file_size = os.path.getsize(filepath)
        
        # تنظیم delimiter بر اساس mode
        if mode == 'line':
            self.delimiter = '\n'
        elif mode == 'csv':
            self.delimiter = '\n'  # CSV هم خط به خط
        
        # ساخت ایندکس
        self._build_index()
    
    def _build_index(self):
        """creating index base on mode"""
        print(f"🔍 Creating index ({self.mode} mode)...")
        
        if self.mode == 'byte':
            self.total_chunks = (self.file_size + self.items_per_chunk - 1) // self.items_per_chunk
            self.item_positions = None
            print(f"✅ Byte mode {self.total_chunks} chunk")
            return
        
        self.item_positions = [0]
        
        if self.mode in ['line', 'csv']:
            # line by line
            with open(self.filepath, 'rb') as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    self.item_positions.append(f.tell())
        
        elif self.mode == 'token':
            delimiter_bytes = self.delimiter.encode('utf-8') if isinstance(self.delimiter, str) else self.delimiter
            
            with open(self.filepath, 'rb') as f:
                buffer = b''
                while True:
                    chunk = f.read(8192)  # read 8kb
                    if not chunk:
                        break
                    
                    buffer += chunk
                    parts = buffer.split(delimiter_bytes)
                    
                    # ذخیره موقعیت‌ها به جز آخرین قسمت
                    for i in range(len(parts) - 1):
                        current_pos = f.tell() - len(buffer) + sum(len(p) + len(delimiter_bytes) for p in parts[:i+1])
                        self.item_positions.append(current_pos)
                    
                    buffer = parts[-1]  # نگه داشتن آخرین قسمت ناقص
                
                if buffer:
                    self.item_positions.append(f.tell())
        
        self.total_items = len(self.item_positions) - 1
        self.total_chunks = (self.total_items + self.items_per_chunk - 1) // self.items_per_chunk
        
        print(f"✅ Indexes created: {self.total_items:,} item ، {self.total_chunks} chunk")
    
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
                'hash': self.get_chunk_hash(chunk_index)
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
        }
        
        if self.mode != 'byte':
            info['total_items'] = self.total_items
            info['avg_chunk_size'] = self.file_size // self.total_chunks if self.total_chunks > 0 else 0
        
        return info

# reader = FlexibleChunkReader('best-dns-wordlist.txt', items_per_chunk=256, mode='line')