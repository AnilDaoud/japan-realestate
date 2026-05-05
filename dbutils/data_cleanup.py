"""
Post-load data quality cleanup for MLIT real estate database.
Identifies and flags suspect records without deleting them.
"""

import psycopg2
from psycopg2.extras import execute_values
import os
from typing import Dict, List, Tuple

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/mlit_realestate")

ISSUE_TYPES = {
    "sentinel_area_9999": "Area field contains placeholder value 9999",
    "sentinel_area_8888": "Area field contains placeholder value 8888",
    "sentinel_price_extreme_low": "Trade price < 1000 yen (likely data error)",
    "missing_municipality_code": "Missing municipality code (but has district name)",
    "missing_both_location": "Missing both municipality code AND district name",
}


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def create_data_quality_flag_table(conn):
    """Create table to track flagged records (idempotent)."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_flags (
                id BIGSERIAL PRIMARY KEY,
                transaction_id BIGINT UNIQUE NOT NULL REFERENCES transactions(id),
                issue_code VARCHAR(50) NOT NULL,
                issue_description TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                reviewed BOOLEAN DEFAULT FALSE,
                notes TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_flags_issue ON data_quality_flags(issue_code);
            CREATE INDEX IF NOT EXISTS idx_flags_reviewed ON data_quality_flags(reviewed);
        """)
    conn.commit()
    print("✅ Data quality flag table ready")


def identify_data_quality_issues(conn) -> Dict[str, List[Tuple[int, str]]]:
    """Run all data quality checks and return issues found."""
    issues = {issue_code: [] for issue_code in ISSUE_TYPES.keys()}

    with conn.cursor() as cur:
        # 1. Sentinel value: Area = 9999
        cur.execute("""
            SELECT id FROM transactions WHERE area_m2 = 9999
        """)
        issues["sentinel_area_9999"] = [(row[0],) for row in cur.fetchall()]
        print(f"  🚩 Area = 9999: {len(issues['sentinel_area_9999']):,}")

        # 2. Sentinel value: Area = 8888
        cur.execute("""
            SELECT id FROM transactions WHERE area_m2 = 8888
        """)
        issues["sentinel_area_8888"] = [(row[0],) for row in cur.fetchall()]
        print(f"  🚩 Area = 8888: {len(issues['sentinel_area_8888']):,}")

        # 3. Suspiciously cheap total price
        cur.execute("""
            SELECT id FROM transactions
            WHERE trade_price IS NOT NULL AND trade_price > 0 AND trade_price < 1000
        """)
        issues["sentinel_price_extreme_low"] = [(row[0],) for row in cur.fetchall()]
        print(f"  🚩 Price < ¥1,000: {len(issues['sentinel_price_extreme_low']):,}")

        # 4. Missing municipality code (but has district)
        # Note: This is normal for Hokkaido and some other prefectures
        cur.execute("""
            SELECT id FROM transactions
            WHERE municipality_code IS NULL AND district_name IS NOT NULL
        """)
        issues["missing_municipality_code"] = [(row[0],) for row in cur.fetchall()]
        print(f"  ℹ️  Missing municipality code (has district): {len(issues['missing_municipality_code']):,}")

        # 5. Missing both location fields
        cur.execute("""
            SELECT id FROM transactions
            WHERE municipality_code IS NULL AND (district_name IS NULL OR district_name = '')
        """)
        issues["missing_both_location"] = [(row[0],) for row in cur.fetchall()]
        print(f"  🚩 Missing both municipality & district: {len(issues['missing_both_location']):,}")

    return issues


def flag_issues_in_database(conn, issues: Dict[str, List[Tuple[int, str]]]):
    """Insert flags into data_quality_flags table."""
    total_flagged = 0

    with conn.cursor() as cur:
        for issue_code, transaction_ids in issues.items():
            if not transaction_ids:
                continue

            # Prepare data for insert
            rows = [
                (tx_id[0], issue_code, ISSUE_TYPES[issue_code])
                for tx_id in transaction_ids
            ]

            execute_values(
                cur,
                """
                INSERT INTO data_quality_flags (transaction_id, issue_code, issue_description)
                VALUES %s
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                rows
            )

            inserted = cur.rowcount
            total_flagged += inserted
            if inserted > 0:
                print(f"  ✅ Flagged {inserted} records for: {issue_code}")

    conn.commit()
    print(f"\n📊 Total records flagged: {total_flagged:,}")
    return total_flagged


def generate_data_quality_report(conn):
    """Generate summary report of data quality issues."""
    print("\n" + "="*70)
    print("DATA QUALITY REPORT")
    print("="*70)

    with conn.cursor() as cur:
        # Total transactions
        cur.execute("SELECT COUNT(*) FROM transactions")
        total = cur.fetchone()[0]
        print(f"\n📈 Total transactions in database: {total:,}")

        # Total flagged
        cur.execute("SELECT COUNT(DISTINCT transaction_id) FROM data_quality_flags")
        total_flagged = cur.fetchone()[0]
        print(f"🚩 Total unique records flagged: {total_flagged:,} ({100*total_flagged/total:.2f}%)")

        # By issue type
        print("\n📋 Issues by type:")
        cur.execute("""
            SELECT issue_code, COUNT(*) as count
            FROM data_quality_flags
            GROUP BY issue_code
            ORDER BY count DESC
        """)
        for issue_code, count in cur.fetchall():
            pct = 100 * count / total
            severity = "🚩" if issue_code.startswith("sentinel_") or issue_code.startswith("missing_both_") else "ℹ️ "
            print(f"  {severity} {issue_code}: {count:,} ({pct:.2f}%)")

        # Records with multiple issues
        print("\n🔗 Records with multiple flags:")
        cur.execute("""
            SELECT transaction_id, COUNT(*) as issue_count
            FROM data_quality_flags
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
            ORDER BY issue_count DESC
            LIMIT 5
        """)
        for tx_id, issue_count in cur.fetchall():
            print(f"  Transaction {tx_id}: {issue_count} issues")

        # Hokkaido special case
        print("\n🏘️  Hokkaido (01) analysis:")
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN municipality_code IS NULL AND district_name IS NOT NULL THEN 1 END) as missing_muni,
                COUNT(CASE WHEN id IN (SELECT transaction_id FROM data_quality_flags WHERE issue_code = 'missing_both_location') THEN 1 END) as both_missing
            FROM transactions
            WHERE prefecture_code = '01'
        """)
        total_hokkaido, missing_muni, both_missing = cur.fetchone()
        print(f"  Total records: {total_hokkaido:,}")
        print(f"  Missing municipality (normal): {missing_muni:,}")
        print(f"  Missing both (problematic): {both_missing:,}")

    print("\n" + "="*70)
    print("NEXT STEPS:")
    print("  1. Review flagged records: SELECT * FROM data_quality_flags WHERE reviewed = FALSE")
    print("  2. Mark as reviewed: UPDATE data_quality_flags SET reviewed = TRUE WHERE issue_code = '...'")
    print("  3. Filter in analysis: JOIN transactions WITH data_quality_flags to exclude flagged records")
    print("="*70 + "\n")


def cleanup(conn):
    """Run full data quality cleanup process."""
    print("\n🔍 MLIT Real Estate Data Quality Cleanup")
    print("="*70)

    # Step 1: Create flag table
    create_data_quality_flag_table(conn)

    # Step 2: Identify issues
    print("\n🔎 Scanning for data quality issues...")
    issues = identify_data_quality_issues(conn)

    # Step 3: Flag in database
    print("\n💾 Storing flags in database...")
    flag_issues_in_database(conn, issues)

    # Step 4: Generate report
    generate_data_quality_report(conn)


def report_only(conn):
    """Generate report from existing flags without re-scanning."""
    print("\n" + "="*70)
    print("DATA QUALITY REPORT (from existing flags)")
    print("="*70)

    with conn.cursor() as cur:
        # Total transactions
        cur.execute("SELECT COUNT(*) FROM transactions")
        total = cur.fetchone()[0]
        print(f"\n📈 Total transactions in database: {total:,}")

        # Total flagged
        cur.execute("SELECT COUNT(DISTINCT transaction_id) FROM data_quality_flags")
        total_flagged = cur.fetchone()[0]
        print(f"🚩 Total unique records flagged: {total_flagged:,} ({100*total_flagged/total:.2f}%)")

        # By issue type
        print("\n📋 Issues by type:")
        cur.execute("""
            SELECT issue_code, COUNT(*) as count
            FROM data_quality_flags
            GROUP BY issue_code
            ORDER BY count DESC
        """)
        for issue_code, count in cur.fetchall():
            pct = 100 * count / total
            severity = "🚩" if issue_code.startswith("sentinel_") or issue_code.startswith("missing_both_") else "ℹ️ "
            print(f"  {severity} {issue_code}: {count:,} ({pct:.2f}%)")

        # Hokkaido special case
        print("\n🏘️  Hokkaido (01) analysis:")
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN municipality_code IS NULL AND district_name IS NOT NULL THEN 1 END) as missing_muni,
                COUNT(CASE WHEN id IN (SELECT transaction_id FROM data_quality_flags WHERE issue_code = 'missing_both_location') THEN 1 END) as both_missing
            FROM transactions
            WHERE prefecture_code = '01'
        """)
        total_hokkaido, missing_muni, both_missing = cur.fetchone()
        print(f"  Total records: {total_hokkaido:,}")
        print(f"  Missing municipality (normal): {missing_muni:,}")
        print(f"  Missing both (problematic): {both_missing:,}")

    print("\n" + "="*70)
    print("NEXT STEPS:")
    print("  1. Review flagged records: SELECT * FROM data_quality_flags WHERE reviewed = FALSE")
    print("  2. Mark as reviewed: UPDATE data_quality_flags SET reviewed = TRUE WHERE issue_code = '...'")
    print("  3. Filter in analysis: JOIN transactions WITH data_quality_flags to exclude flagged records")
    print("="*70 + "\n")


if __name__ == "__main__":
    conn = get_db_connection()
    try:
        import sys
        if len(sys.argv) > 1 and sys.argv[1] == "--report-only":
            report_only(conn)
        else:
            cleanup(conn)
    finally:
        conn.close()
