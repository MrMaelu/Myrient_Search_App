"""Entry point for the Myrient GUI application."""

import sys
from pathlib import Path
from typing import Final

import gui

BASE_URL: Final[str] = "https://myrient.erista.me/files/"
DB_FILE: Final[Path] = Path("myrient_index.db")
DOWNLOAD_DIR: Final[Path] = Path("downloads")


def main() -> None:
    """Initialize paths and run the GUI application."""
    base_dir: Path = (
        Path(sys.executable).parent
        if getattr(sys, "frozen", False)
        else Path(__file__).parent
    )
    db_file: Path = base_dir / DB_FILE
    download_dir: Path = base_dir / DOWNLOAD_DIR

    # Run GUI
    app: gui.MyrientApp = gui.MyrientApp(BASE_URL, db_file, download_dir)
    app.mainloop()


if __name__ == "__main__":
    main()
