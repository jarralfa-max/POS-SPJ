from core.database import Database
from core.services.integrity_engine import IntegrityEngine

def run_nightly_check():
    db = Database()
    engine = IntegrityEngine(db)
    result = engine.generate_report()

    if not result:
        raise SystemExit("INTEGRITY_CHECK_FAILED")

if __name__ == "__main__":
    run_nightly_check()