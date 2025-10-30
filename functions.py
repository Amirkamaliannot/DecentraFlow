import hashlib
import os

def get_file_hash(filepath, algorithm: str = 'md5') -> str:
    if algorithm == 'md5':
        hasher = hashlib.md5()
    elif algorithm == 'sha256':
        hasher = hashlib.sha256()
    else:
        raise ValueError(f"unknouwn: {algorithm}")
    
    with open(filepath, 'rb', buffering=0) as f:
        # خواندن 128MB تکه‌ها
        for chunk in iter(lambda: f.read(128*1024*1024), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


def is_file_in_my_disk(path, hash):
    if os.path.exists(path):
        file_hash = get_file_hash(path)
        if(file_hash, hash):
            return True
    return False