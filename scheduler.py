import threading
import time
from datetime import datetime
from core.database import Database

class Scheduler:

    def __init__(self):
        self.db = Database()
        self.stop_signal = False

    def start(self):
        threading.Thread(target=self._run_weekly_vacuum, daemon=True).start()

    def _run_weekly_vacuum(self):
        while not self.stop_signal:
            now = datetime.utcnow()
            if now.weekday() == 6 and now.hour == 3:
                self._perform_vacuum()
                time.sleep(3600)
            time.sleep(300)

    def _perform_vacuum(self):
        with self.db.transaction("WEEKLY_VACUUM"):
            self.db.execute("PRAGMA optimize;")
            self.db.execute("VACUUM;")