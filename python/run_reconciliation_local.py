import csv
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "processed" / "ar_recon.db"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
QUERIES_DIR = REPO_ROOT / "sql" / "queries"

SQL_FILES_IN_ORDER = [
    "02_recon_results_active_sqlite.sql",
    "03_recon_needs_review_active_sqlite.sql",
    "04_recon_dashboard_summary_active_sqlite.sql",
]

EXPORTS = [
    ("recon_results_active", PROCESSED_DIR / "recon_results_active.csv"),
    ("recon_needs_review_active", PROCESSED_DIR / "recon_needs_review_active.csv"),
    ("recon_dashboard_summary_active", PROCESSED_DIR / "recon_dashboard_summary_active.csv"),
]

def run_sql_file(conn: sqlite3.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    conn.executescript(sql)

def export_query(conn: sqlite3.Connection, query: str, out_path: Path) -> int:
    cur = conn.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    return len(rows)

def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}. Run python/python/load_sqlite.py first.")

    conn = sqlite3.connect(DB_PATH)
    try:
        # build/refresh views
        for fname in SQL_FILES_IN_ORDER:
            path = QUERIES_DIR / fname
            if not path.exists():
                raise FileNotFoundError(f"Missing SQL file: {path}")
            run_sql_file(conn, path)
        conn.commit()

        # export
        for view_name, out_path in EXPORTS:
            n = export_query(conn, f"SELECT * FROM {view_name};", out_path)
            print(f"Exported {n} rows to {out_path.relative_to(REPO_ROOT)}")

        print("Local reconciliation complete.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()