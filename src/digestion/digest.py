from pathlib import Path


class Digestion:
    def __init__(self, fp: Path):
        self.file_path = fp
        self.files = self.list_existing_files()

    def list_existing_files(self):
        fls = [fl.as_posix() for fl in self.file_path.iterdir()]
        if len(fls) > 0:
            return fls
