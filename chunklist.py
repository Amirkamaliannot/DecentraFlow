import os
import json
import tempfile
import mmap
import shutil
import hashlib

class Chunk:
    def __init__(self, index, content:list):
        self.index = index
        self.content = content
        self.result:list = None
        # self.hash = hashlib.md5(content.encode('utf-8')).hexdigest()


class ChunkList:
    def __init__(self, DflowHash, json_key="chunklist"):
        self.DflowHash = DflowHash
        self._chunks: list[Chunk] = []
        self._index_map = {}
        self._filename = f"{DflowHash}.data"
        self._json_key = json_key

        if os.path.exists(self._filename):
            self._load_file()

    def add(self, chunk: 'Chunk'):
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

    def get_chunk_by_index(self, index):
        return self._index_map.get(index)

    def _save_file(self):
        data = {}
        if os.path.exists(self._filename):
            with open(self._filename, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = {}

        chunk_data = []
        for c in self._chunks:
            chunk_data.append({
                "index": c.index,
                "content": c.content,
                "result": c.result
            })

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
            chunk = Chunk(
                c["index"],
                c.get("content", []),
            )
            chunk.result = c.get("result", None)
            self._chunks.append(chunk)
            self._index_map[chunk.index] = chunk

    def __iter__(self):
        return iter(self._chunks)

    def __len__(self):
        return len(self._chunks)

    def __getitem__(self, i):
        return self._chunks[i]

    def __repr__(self):
        return repr(self._chunks)



class FileChunkList:
    def __init__(self, DflowHash):

        self.DflowHash = DflowHash
        self._filename = DflowHash+".data2"
        if not os.path.exists(self._filename):
            open(self._filename, 'a').close()


    def add(self, chunk: Chunk):
        """Append a Chunk to file if index is unique"""
        if self.get_by_index(chunk.index):
            return False 

        with open(self._filename, 'a', encoding='utf-8') as f:
            json_line = json.dumps({
                "index": chunk.index,
                "content": chunk.content,
                "result": chunk.result
            })
            f.write(json_line + "\n")
        return True

    def remove_by_index(self, index):
        temp_fd, temp_path = tempfile.mkstemp()
        found = False
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as tmp_file:
            with open(self._filename, 'r', encoding='utf-8') as f:
                for line in f:
                    c = json.loads(line)
                    if c["index"] != index:
                        tmp_file.write(json.dumps(c) + "\n")
                    else:
                        found = True
        if found:
            shutil.move(temp_path, self._filename)
        else:
            os.remove(temp_path)
        return found

    def get_by_index(self, index):
        print('index')
        with open(self._filename, 'r+b') as f:
            # Check file size
            f.seek(0, 2) 
            size = f.tell()
            if size == 0:
                return False 
            f.seek(0)
            mm = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
            start = 0
            while True:
                end = mm.find(b'\n', start)
                if end == -1:
                    break
                line = mm[start:end].decode('utf-8')
                c = json.loads(line)
                if c["index"] == index:
                    chunk = Chunk(c["index"], c.get("content", []))
                    chunk.result = c.get("result", None)
                    mm.close()
                    return chunk
                start = end + 1
            mm.close()
        return False

    def __iter__(self):
        f = open(self._filename, 'r+b')

        # Check file size before mmap
        f.seek(0, 2)
        size = f.tell()
        if size == 0:
            f.close()
            return  # Empty iterator

        f.seek(0)
        mm = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
        start = 0
        try:
            while True:
                end = mm.find(b'\n', start)
                if end == -1:
                    break
                line = mm[start:end].decode('utf-8')
                c = json.loads(line)

                chunk = Chunk(c["index"], c.get("content", []))
                chunk.result = c.get("result", None)

                yield chunk
                start = end + 1
        finally:
            mm.close()
            f.close()

    def __len__(self):
        return sum(1 for _ in self.__iter__())

    def __repr__(self):
        return f"FileChunkList(DflowHash={self.DflowHash})"