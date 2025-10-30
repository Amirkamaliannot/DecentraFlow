import os
import hashlib
import json
from typing import Iterator, Optional, Tuple, Union
import numpy as np
import array
import zlib
import struct
import mmap

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
        
        if mode == 'line':
            self.delimiter = '\n'
        elif mode == 'csv':
            self.delimiter = '\n'
        
        # creating and saving indexes
        self.hash = self.get_file_hash()
        self._build_index()
        self._save_item_positions()

    
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
    
    # def _save_item_positions(self):
    #         self.index_file = self.hash + '.index'
    #         with open(self.index_file, 'wb') as f:
    #             pickle.dump(self.item_positions, f)

    # def _save_item_positions(self):
    #     self.index_file = self.hash + '.index'
    #     # تبدیل به numpy array و ذخیره
    #     np.array(self.item_positions, dtype=np.int64).tofile(self.index_file)

    # def _save_item_positions(self):
    #     self.index_file = self.hash + '.index'
    #     # ذخیره delta (تفاوت بین موقعیت‌ها)
    #     deltas = [self.item_positions[0]]
    #     for i in range(1, len(self.item_positions)):
    #         deltas.append(self.item_positions[i] - self.item_positions[i-1])
        
    #     with open(self.index_file, 'wb') as f:
    #         # تعداد آیتم‌ها
    #         f.write(struct.pack('Q', len(deltas)))
    #         # delta ها (معمولاً عددهای کوچک)
    #         for d in deltas:
    #             f.write(struct.pack('I', d))  # 4 byte به جای 8 byte

    # def _save_item_positions(self):
    #     self.index_file = self.hash + '.index'

    #     # داده‌ها رو به آرایه‌ی عددی تبدیل می‌کنیم (۸ بایت برای هر عدد)
    #     arr = array.array('Q', self.item_positions)

    #     # فشرده‌سازی با حداکثر سطح (می‌تونی level=3 بذاری برای سرعت بیشتر)
    #     compressed = zlib.compress(arr.tobytes(), level=9)

    #     # نوشتن در فایل
    #     with open(self.index_file, 'wb') as f:
    #         f.write(compressed)

    def _save_item_positions(self):
        self.index_file = self.hash + '.index'

        if not self.item_positions:
            return

        # محاسبه‌ی اختلاف‌ها (delta encoding)
        diffs = [self.item_positions[0]]
        for i in range(1, len(self.item_positions)):
            diffs.append(self.item_positions[i] - self.item_positions[i - 1])

        max_val = max(diffs)
        typecode = 'I' if max_val <= 0xFFFFFFFF else 'Q'

        arr = array.array(typecode, diffs)
        compressed = zlib.compress(arr.tobytes(), level=9)

        with open(self.index_file, 'wb') as f:
            f.write(typecode.encode('ascii'))
            f.write(compressed)

    def _load_item_positions(self):
        self.index_file = self.hash + '.index'

        with open(self.index_file, 'rb') as f:
            typecode = f.read(1).decode('ascii')  # نوع داده ('I' یا 'Q')
            compressed = f.read()

        data = zlib.decompress(compressed)
        arr = array.array(typecode)
        arr.frombytes(data)
        diffs = arr.tolist()

        # بازسازی لیست اصلی با جمع تجمعی (cumulative sum)
        positions = []
        total = 0
        for diff in diffs:
            total += diff
            positions.append(total)

        self.item_positions = positions
                    
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
        
        print (hasher.hexdigest())
        return hasher.hexdigest()

# reader = FlexibleChunkReader('best-dns-wordlist.txt', items_per_chunk=256, mode='line')