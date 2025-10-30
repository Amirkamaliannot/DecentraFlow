import os
import array
import zlib

class Chunk:
    def __init__(self, index, content:list):
        self.index = index
        self.content = content
        self.result:list = None

import os
import json

class ChunkList:
    def __init__(self, DflowHash, json_key="chunklist"):
        self.DflowHash = DflowHash
        self._chunks: list[Chunk] = []
        self._index_map = {}
        self._filename = f"{DflowHash}.data"
        self._json_key = json_key

        if os.path.exists(self._filename):
            self._load_file()


    def add(self, chunk: Chunk):
        if chunk.index in self._index_map:
            return False
        self._chunks.append(chunk)
        self._index_map[chunk.index] = chunk
        self._save_file()  
        return True

    def remove_by_index(self, index):
        chunk = self._index_map.pop(index, None)
        if chunk:
            self._chunks.remove(chunk)
            self._save_file()  


    def _save_file(self):
        data = {}
        if os.path.exists(self._filename):
            with open(self._filename, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = {}

        chunk_data = [{"index": c.index} for c in self._chunks]

        data[self._json_key] = chunk_data

        with open(self._filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

    def _load_file(self):
        with open(self._filename, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}

        chunk_data = data.get(self._json_key, [])
        self._chunks = []
        self._index_map = {}
        for c in chunk_data:
            chunk = Chunk(c["index"], None)  # یا دیتای خودت
            self._chunks.append(chunk)
            self._index_map[chunk.index] = chunk

    def get_chunk_by_index(self, chunk: 'Chunk'):
        try:
            pos = self._chunks.index(chunk)
            return self._chunks[pos]
        except ValueError:
            return None

    def __iter__(self):
        return iter(self._chunks)

    def __len__(self):
        return len(self._chunks)

    def __getitem__(self, i):
        return self._chunks[i]

    def __repr__(self):
        return repr(self._chunks)