import subprocess
import sys
import os
import re
import threading
import queue
import urllib.parse
from urllib.parse import unquote

class Downloader:
    def __init__(self, output_dir, max_file_workers=4):
        dir_path = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)

        self.wget_binary = os.path.join(dir_path, 'wget.exe')
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.max_file_workers = max_file_workers
        self.download_queue = queue.Queue()
        self.processes = []
        self.cancel_flag = threading.Event()


    def _download_file(self, file_idx, url, progress_callback):
        filename = unquote(os.path.basename(url))
        filepath = os.path.join(self.output_dir, filename)

        cmd = [self.wget_binary, "-O", filepath, "--progress=dot:mega", url]

        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NO_WINDOW

        process = subprocess.Popen(
            cmd,
            #stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            creationflags=creationflags
        )
        self.processes.append(process)

        total_bytes = None
        downloaded_bytes = 0

        process_finished = False
        try:
            for line in process.stderr:
                if self.cancel_flag.is_set():
                    process.terminate()
                    break

                m_total = re.search(r"Length: (\d+)", line)
                if m_total:
                    total_bytes = int(m_total.group(1))

                m_prog = re.search(r"(\d+)%", line)
                if m_prog and total_bytes:
                    percent = int(m_prog.group(1))
                    downloaded_bytes = int(percent / 100 * total_bytes)
                    progress_callback(file_idx, url, downloaded_bytes, total_bytes)

            process.wait()
            if not self.cancel_flag.is_set():
                process_finished = True
                progress_callback(file_idx, url, total_bytes or downloaded_bytes, total_bytes or downloaded_bytes)

        finally:
            if not process_finished:
                self.clean_up_partial_files(filepath)


    def start(self, progress_callback, done_callback):
        total_files = self.download_queue.qsize()
        completed_files = 0
        file_idx_counter = 0
        lock = threading.Lock()

        def worker():
            nonlocal completed_files, file_idx_counter
            while not self.cancel_flag.is_set():
                try:
                    url = self.download_queue.get_nowait()
                except queue.Empty:
                    break

                with lock:
                    idx = file_idx_counter
                    file_idx_counter += 1

                self._download_file(idx, url, progress_callback)

                with lock:
                    completed_files += 1
                    if completed_files == total_files:
                        done_callback(completed_files, total_files)

                self.download_queue.task_done()

        threads = []
        for _ in range(min(self.max_file_workers, total_files)):
            t = threading.Thread(target=worker, daemon=True)
            threads.append(t)
            t.start()

        self.download_queue.join()


    def add_url(self, url):
        self.download_queue.put(url)


    def all_stopped(self):
        return self.download_queue.empty()


    def cancel_all(self):
        self.cancel_flag.set()
        while not self.download_queue.empty():
            try:
                self.download_queue.get_nowait()
            except queue.Empty:
                break
        for p in self.processes:
            try:
                p.terminate()
            except Exception:
                pass


    def clean_up_partial_files(self, filepath):
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"Deleting {filepath}")
        except Exception as e:
            print(f"Failed to clean up file: {filepath}: {e}")
