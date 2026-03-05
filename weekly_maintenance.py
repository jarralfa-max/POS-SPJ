from core.database import Database

def run_weekly_maintenance():
    db = Database()
    db.execute("PRAGMA optimize;")
    db.execute("VACUUM;")

if __name__ == "__main__":
    run_weekly_maintenance()