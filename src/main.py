import os
import sys
import gui

BASE_URL = "https://myrient.erista.me/files/"
DB_FILE = "myrient_index.db"
DOWNLOAD_DIR = 'downloads'

if __name__ == "__main__":
    base_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)
    db_file = os.path.join(base_dir, DB_FILE)
    download_dir = os.path.join(base_dir, DOWNLOAD_DIR)

    # Run GUI
    app = gui.MyrientApp(BASE_URL, DB_FILE, DOWNLOAD_DIR)
    app.mainloop()
