"""
Deactivate jobs older than 30 days based on posted_date (not days_since_posted).
This ensures accurate age calculation regardless of when the job was imported.
"""
import psycopg2
from urllib.parse import quote_plus
from datetime import datetime, timedelta

def get_db_connection():
    """Get database connection"""
    password = quote_plus("Asd123asd!!!")
    db_url = f"postgresql://postgres.bjrewhbjaluetqgwtjgg:{password}@aws-0-eu-north-1.pooler.supabase.com:6543/postgres"
    return psycopg2.connect(db_url)

def cleanup_old_jobs(days_threshold=30):
    """Deactivate jobs older than specified days based on posted_date"""
    conn = get_db_connection()
    cur = conn.cursor()

    cutoff_date = datetime.now() - timedelta(days=days_threshold)

    print(f"🧹 Deactivating jobs posted before {cutoff_date.strftime('%Y-%m-%d')}...")
    print(f"   (older than {days_threshold} days)\n")

    # Check how many will be affected
    cur.execute("""
        SELECT COUNT(*) FROM jobs
        WHERE is_active = true
        AND posted_date < %s
    """, (cutoff_date,))

    to_deactivate = cur.fetchone()[0]
    print(f"📊 Found {to_deactivate} jobs to deactivate")

    if to_deactivate == 0:
        print("✅ No jobs to deactivate")
        cur.close()
        conn.close()
        return

    # Deactivate old jobs
    cur.execute("""
        UPDATE jobs
        SET is_active = false, last_updated = NOW()
        WHERE is_active = true
        AND posted_date < %s
    """, (cutoff_date,))

    affected = cur.rowcount
    conn.commit()

    print(f"✅ Deactivated {affected} old jobs\n")

    # Show final stats
    cur.execute("SELECT COUNT(*) FROM jobs WHERE is_active = true")
    active_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM jobs WHERE is_active = false")
    inactive_count = cur.fetchone()[0]

    print(f"{'='*50}")
    print(f"📊 Database Stats:")
    print(f"   Active jobs:   {active_count}")
    print(f"   Inactive jobs: {inactive_count}")
    print(f"{'='*50}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    cleanup_old_jobs(days_threshold=30)
