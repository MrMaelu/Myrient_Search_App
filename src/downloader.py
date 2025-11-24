"""Downloader for the Myrient Search App."""
import contextlib
import queue
import re
import subprocess
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from urllib.parse import unquote, urlparse


class Downloader:
    """Downloader class for the Myrient Search App."""

    def __init__(self, output_dir:str, max_file_workers:int=4) -> None:
        """Initialize variables."""
        dir_path = Path(
            sys.executable if getattr(sys, "frozen", False) else __file__).parent

        self.wget_binary = Path(dir_path, "wget.exe")
        self.output_dir = Path(output_dir)
        Path.mkdir(self.output_dir, parents=True, exist_ok=True)

        self.max_file_workers = max_file_workers
        self.download_queue = queue.Queue()
        self.processes = []
        self.cancel_flag = threading.Event()
        self.download_running = False
        self.file_idx_counter = 0
        self.lock = threading.Lock()


    def _download_file(self,
                       file_idx:str,
                       url:str,
                       progress_callback:Callable|None,
                       )-> None:
        """Start a wget process to download a single file."""
        parsed = urlparse(url)
        filename = Path(unquote(parsed.path)).name
        filepath = Path(self.output_dir, filename + ".incomplete")
        wget_args = [
            "-m",
            "-np",
            "-c",
            "-e", "robots=off",
            "-R", "index.html*",
            "--progress=dot:mega",
            "-O", filepath,
        ]

        cmd = [self.wget_binary, *wget_args, url]

        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NO_WINDOW

        process = subprocess.Popen(  # noqa: S603
            cmd,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            creationflags=creationflags,
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
                Path.rename(filepath, Path(self.output_dir, filename))
                progress_callback(
                    file_idx,
                    url,
                    total_bytes or downloaded_bytes,
                    total_bytes or downloaded_bytes,
                    )

        finally:
            if not process_finished:
                self.clean_up_partial_files(filepath)


    def start(self,
              progress_callback:Callable|None,
              ) -> None:
        """Start the downloading process."""
        if self.download_running:
            return

        self.download_running = True

        def worker() -> None:
            while not self.cancel_flag.is_set():
                try:
                    idx, url = self.download_queue.get(timeout=1)
                except queue.Empty:
                    if not self.download_running and self.download_queue.empty():
                        break
                    continue

                if not self.cancel_flag.is_set():
                    self._download_file(idx, url, progress_callback)
                    self.download_queue.task_done()

        threads = []
        for _ in range(self.max_file_workers):
            t = threading.Thread(target=worker, daemon=True)
            threads.append(t)
            t.start()


    def add_url(self, url:str) -> int:
        """Add a new URL to the download queue."""
        with self.lock:
            self.download_queue.put((self.file_idx_counter, url))
            self.file_idx_counter += 1
        return self.download_queue.qsize()


    def all_stopped(self) -> bool:
        """Check if the download queue is empty."""
        return self.download_queue.empty()


    def cancel_all(self) -> None:
        """Cancel all current downloads."""
        self.cancel_flag.set()
        while not self.download_queue.empty():
            try:
                self.download_queue.get_nowait()
            except queue.Empty:
                break
        for p in self.processes:
            with contextlib.suppress(Exception):
                p.terminate()


    def clean_up_partial_files(self, filepath:str) -> None:
        """Remove unfinished downloads."""
        path = Path(filepath)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except PermissionError:
            pass
        except OSError:
            pass
