"""
Layer 1: File Watcher
Monitors input directory for new .dsx / .isx files and triggers the pipeline.
"""

import os
import time
import logging
from pathlib import Path
from typing import Callable, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

logger = logging.getLogger(__name__)


class DSXFileHandler(FileSystemEventHandler):
    """Handles filesystem events for DataStage job files."""

    SUPPORTED_EXTENSIONS = {".dsx", ".isx", ".xml"}

    def __init__(self, callback: Callable[[Path], None]):
        """
        Args:
            callback: Function to call when a new DSX/ISX file is detected.
                      Receives the Path to the new file.
        """
        super().__init__()
        self.callback = callback

    def on_created(self, event: FileCreatedEvent) -> None:
        if not event.is_directory:
            path = Path(event.src_path)
            if path.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                logger.info(f"[FileWatcher] New file detected: {path.name}")
                self.callback(path)

    def on_moved(self, event: FileMovedEvent) -> None:
        """Also handle files moved into the watched directory."""
        if not event.is_directory:
            path = Path(event.dest_path)
            if path.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                logger.info(f"[FileWatcher] File moved in: {path.name}")
                self.callback(path)


class FileWatcher:
    """
    Watches a directory for new IBM DataStage job files (.dsx / .isx).
    Calls a callback when a new file appears.
    """

    def __init__(
        self,
        watch_dir: str | Path,
        callback: Callable[[Path], None],
        poll_interval: float = 5.0,
    ):
        self.watch_dir = Path(watch_dir)
        self.callback = callback
        self.poll_interval = poll_interval
        self._observer: Optional[Observer] = None

        if not self.watch_dir.exists():
            self.watch_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"[FileWatcher] Created input directory: {self.watch_dir}")

    def start(self) -> None:
        """Start watching the directory (non-blocking)."""
        handler = DSXFileHandler(self.callback)
        self._observer = Observer()
        self._observer.schedule(handler, str(self.watch_dir), recursive=False)
        self._observer.start()
        logger.info(f"[FileWatcher] Watching {self.watch_dir} for .dsx/.isx files...")

    def stop(self) -> None:
        """Stop the file watcher."""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            logger.info("[FileWatcher] Stopped.")

    def scan_existing(self) -> list[Path]:
        """
        Scan watch directory for already-present DSX/ISX files.
        Useful for processing files that arrived before the watcher started.
        """
        found = []
        for ext in DSXFileHandler.SUPPORTED_EXTENSIONS:
            found.extend(self.watch_dir.glob(f"*{ext}"))
        if found:
            logger.info(f"[FileWatcher] Found {len(found)} existing file(s) in {self.watch_dir}")
        return sorted(found)

    def run_forever(self) -> None:
        """Start watching and block until KeyboardInterrupt."""
        self.start()
        try:
            while True:
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            self.stop()
