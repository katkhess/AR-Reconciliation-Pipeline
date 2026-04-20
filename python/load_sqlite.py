import csv
import re
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
DB_PATH = PROCESSED_DIR / "ar_recon.db"
SQLITE_SCHEMA_PATH = REPO_ROOT / "sql" / "schema_sqlite.sql"

TABLE_FILES = {
    "customers": RAW_DIR / "customers.csv",
    "invoices": RAW_DIR / "invoices.csv",
    "payments": RAW_DIR / "payments.csv",
    "returns": RAW_DIR / "returns.csv",
    "credits": RAW_DIR / "credits.csv",
}

INV_PATTERN = re.compile(r"\bINV(\d+)\b", re.IGNORECASE)

def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SQLITE_SCHEMA_PATH.read_text(encoding="utf-8"))

def load_csv(conn: sqlite3.Connection, table: str, csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames or []
        if not cols:
            raise ValueError(f"No header found in CSV: {csv_path}")

        col_list = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["?"] * len(cols))
        sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'

        rows = [[row.get(c) for c in cols] for row in reader]

    conn.executemany(sql, rows)
    return len(rows)

def build_payment_invoice_links(conn: sqlite3.Connection) -> int:
    # Clear and rebuild each run
    conn.execute("DELETE FROM payment_invoice_links;")

    cur = conn.execute("SELECT payment_id, reference_notes FROM payments;")
    links = []
    for payment_id, notes in cur.fetchall():
        if not notes:
            continue
        invoice_ids = {int(m.group(1)) for m in INV_PATTERN.finditer(notes)}
        for inv_id in invoice_ids:
            links.append((payment_id, inv_id))

    conn.executemany(
        "INSERT OR IGNORE INTO payment_invoice_links(payment_id, invoice_id) VALUES (?, ?);",
        links,
    )
    return len(links)

def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        apply_schema(conn)

        # Load base tables
        for table, path in TABLE_FILES.items():
            conn.execute(f'DELETE FROM "{table}";')
            n = load_csv(conn, table, path)
            print(f"Loaded {n} rows into {table} from {path.name}")
        conn.commit()

        # Build parsed links
        link_count = build_payment_invoice_links(conn)
        conn.commit()
        print(f"Built {link_count} payment→invoice links in payment_invoice_links")

        print(f"SQLite DB created at: {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()