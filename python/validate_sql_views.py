import sqlite3
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "processed" / "ar_recon.db"
QUERIES_DIR = REPO_ROOT / "sql" / "queries"

SQL_FILES_IN_ORDER = [
    "02_recon_results_active_sqlite.sql",
    "03_recon_needs_review_active_sqlite.sql",
    "04_recon_dashboard_summary_active_sqlite.sql",
]

REQUIRED_VIEWS = [
    "recon_results_active",
    "recon_needs_review_active",
    "recon_dashboard_summary_active",
]

def run(cmd: list[str]) -> None:
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)

def exec_sql_file(conn: sqlite3.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    conn.executescript(sql)

def table_or_view_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type IN ('table','view') AND name = ?
        LIMIT 1
        """,
        (name,),
    )
    return cur.fetchone() is not None

def get_columns(conn: sqlite3.Connection, view_name: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({view_name});")
    return {row[1] for row in cur.fetchall()}  # row[1] = column name

def scalar(conn: sqlite3.Connection, q: str) -> int:
    return int(conn.execute(q).fetchone()[0])

def main() -> int:
    # Ensure DB exists (and is current) by running the standard local workflow
    if not DB_PATH.exists():
        run([sys.executable, "python/generate_data.py"])
        run([sys.executable, "python/load_sqlite.py"])

    conn = sqlite3.connect(DB_PATH)
    try:
        # Build/refresh views
        for fname in SQL_FILES_IN_ORDER:
            path = QUERIES_DIR / fname
            if not path.exists():
                raise FileNotFoundError(f"Missing SQL file: {path}")
            exec_sql_file(conn, path)
        conn.commit()

        # Verify views exist
        for v in REQUIRED_VIEWS:
            if not table_or_view_exists(conn, v):
                raise RuntimeError(f"Expected view not found: {v}")

        # Basic sanity checks
        payments = scalar(conn, "SELECT COUNT(*) FROM payments;")
        results = scalar(conn, "SELECT COUNT(*) FROM recon_results_active;")
        needs_review = scalar(conn, "SELECT COUNT(*) FROM recon_needs_review_active;")
        summary = scalar(conn, "SELECT COUNT(*) FROM recon_dashboard_summary_active;")

        if payments <= 0:
            raise RuntimeError("payments table is empty; did load_sqlite.py run correctly?")

        if results != payments:
            raise RuntimeError(
                f"Expected recon_results_active to have 1 row per payment: "
                f"payments={payments}, recon_results_active={results}"
            )

        if needs_review > results:
            raise RuntimeError(
                f"Expected recon_needs_review_active to be a subset of recon_results_active: "
                f"needs_review={needs_review}, results={results}"
            )

        # Column check: ensure gap_amount exists (we use it for troubleshooting NO_CUTOFF_IN_WINDOW)
        cols = get_columns(conn, "recon_results_active")
        if "gap_amount" not in cols:
            raise RuntimeError("Expected column gap_amount in recon_results_active (did you run the updated SQL?).")

        # Print match type distribution (useful output for CI/logs)
        print("\nMatch type distribution:")
        for match_type, n in conn.execute(
            "SELECT match_type, COUNT(*) FROM recon_results_active GROUP BY match_type ORDER BY COUNT(*) DESC;"
        ).fetchall():
            print(f"  {match_type}: {n}")

        print(
            "\nOK: SQL views validated.\n"
            f"  DB: {DB_PATH.relative_to(REPO_ROOT)}\n"
            f"  payments={payments}, results={results}, needs_review={needs_review}, summary_rows={summary}\n"
        )
        return 0
    finally:
        conn.close()

if __name__ == "__main__":
    raise SystemExit(main())