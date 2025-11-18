import os
import json
import tempfile
import mmap
import shutil
import hashlib
import sqlite3

class Chunk:
    def __init__(self, index, content:list):
        self.index = index
        self.content = content
        self.result:list = None
        # self.hash = hashlib.md5(content.encode('utf-8')).hexdigest()

