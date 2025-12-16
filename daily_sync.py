"""
Daily job sync script - Fetches jobs from priority countries and imports to Supabase.
Rotates between countries to stay within 10K/day API limit.

Priority: US, India, MEA (UAE, Saudi, Egypt, etc.), Canada, UK
"""
import requests
import psycopg2
import sys
import os
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urlparse

API_KEY = os.getenv("HIREBASE_API_KEY", "hb_2b5e7594-c13d-465b-8981-d7cdce7d1bbe")
API_URL = "https://api.hirebase.org/v2/jobs/search"
MAX_DAILY_API_CALLS = 10000

def get_today_api_usage():
    """Get today's API usage from database"""
    conn = get_db_connection()
    cur = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    # Create tracking table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hirebase_api_usage (
            date DATE PRIMARY KEY,
            api_calls INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()

    cur.execute("SELECT api_calls FROM hirebase_api_usage WHERE date = %s", (today,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    return result[0] if result else 0

def update_api_usage(calls_made):
    """Update today's API usage in database"""
    conn = get_db_connection()
    cur = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    cur.execute("""
        INSERT INTO hirebase_api_usage (date, api_calls, last_updated)
        VALUES (%s, %s, NOW())
        ON CONFLICT (date) DO UPDATE SET
            api_calls = hirebase_api_usage.api_calls + %s,
            last_updated = NOW()
    """, (today, calls_made, calls_made))

    conn.commit()
    cur.close()
    conn.close()

def check_daily_limit():
    """Check if we've hit our daily limit - returns remaining calls or 0 if at limit"""
    used = get_today_api_usage()
    remaining = MAX_DAILY_API_CALLS - used
    return max(0, remaining), used

def get_last_sync_date(country):
    """Get the last successful sync date for a country"""
    conn = get_db_connection()
    cur = conn.cursor()

    # Create tracking table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hirebase_sync_tracker (
            country VARCHAR(100) PRIMARY KEY,
            last_sync_date DATE,
            last_sync_time TIMESTAMP DEFAULT NOW(),
            jobs_fetched INTEGER DEFAULT 0
        )
    """)
    conn.commit()

    cur.execute("SELECT last_sync_date FROM hirebase_sync_tracker WHERE country = %s", (country,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if result and result[0]:
        return result[0].strftime('%Y-%m-%d')
    else:
        # First time syncing this country - get jobs from last 7 days
        return (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

def update_sync_tracker(country, jobs_fetched):
    """Update the last sync date for a country"""
    conn = get_db_connection()
    cur = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    cur.execute("""
        INSERT INTO hirebase_sync_tracker (country, last_sync_date, last_sync_time, jobs_fetched)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (country) DO UPDATE SET
            last_sync_date = %s,
            last_sync_time = NOW(),
            jobs_fetched = hirebase_sync_tracker.jobs_fetched + %s
    """, (country, today, jobs_fetched, today, jobs_fetched))

    conn.commit()
    cur.close()
    conn.close()

def get_favicon_from_website(website):
    try:
        if not website:
            return None
        website = website.strip()
        if not website.startswith(('http://', 'https://')):
            website = 'https://' + website
        parsed = urlparse(website)
        domain = parsed.netloc or parsed.path.split('/')[0]
        if not domain:
            return None
        return f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    except:
        return None

def get_db_connection():
    password = quote_plus(os.getenv("SUPABASE_PASSWORD", "Asd123asd!!!"))
    db_url = f"postgresql://postgres.bjrewhbjaluetqgwtjgg:{password}@aws-0-eu-north-1.pooler.supabase.com:6543/postgres"
    return psycopg2.connect(db_url)

def normalize_country(country_name):
    mapping = {
        'United Arab Emirates': 'UAE',
        'United States': 'USA',
        'United Kingdom': 'UK',
    }
    return mapping.get(country_name, country_name)

def parse_date_posted(date_str):
    if not date_str:
        return None, None
    try:
        date_str = date_str.replace(' U', '').strip()
        if ' ' in date_str:
            posted_date = datetime.strptime(date_str.split()[0], '%Y-%m-%d')
        else:
            posted_date = datetime.strptime(date_str, '%Y-%m-%d')
        days_since = (datetime.now() - posted_date).days
        return posted_date, max(0, days_since)
    except:
        return None, None

def format_salary(salary_range):
    if not salary_range:
        return None
    if isinstance(salary_range, dict):
        min_sal = salary_range.get('min')
        max_sal = salary_range.get('max')
        currency = salary_range.get('currency', 'USD')
        if min_sal and max_sal:
            return f"${min_sal:,} - ${max_sal:,} {currency}"
        elif min_sal:
            return f"${min_sal:,}+ {currency}"
        elif max_sal:
            return f"Up to ${max_sal:,} {currency}"
    return str(salary_range) if salary_range else None

def format_seniority(yoe_range):
    if not yoe_range:
        return None
    min_yoe = yoe_range.get('min', 0)
    max_yoe = yoe_range.get('max', 0)
    if max_yoe <= 2:
        return "Entry Level"
    elif max_yoe <= 5:
        return "Mid Level"
    elif max_yoe <= 8:
        return "Senior"
    else:
        return "Lead/Principal"

def insert_job(cur, job, source_country):
    """Insert new job or update existing with new date (mark as reposted)"""
    try:
        external_url = job.get('application_link')
        if not external_url:
            return 'skipped'

        # Parse the new date from API
        posted_date, days_since = parse_date_posted(job.get('date_posted'))

        # Check if job already exists
        cur.execute("SELECT id, posted_date FROM jobs WHERE external_job_url = %s", (external_url,))
        existing = cur.fetchone()

        if existing:
            existing_id = existing[0]
            existing_date = existing[1]

            # If API has a newer date, update the job and mark as reposted
            if posted_date and existing_date and posted_date > existing_date:
                cur.execute("""
                    UPDATE jobs
                    SET posted_date = %s,
                        days_since_posted = %s,
                        job_posting_time = %s,
                        is_reposted = true,
                        last_updated = NOW()
                    WHERE id = %s
                """, (posted_date, days_since, job.get('date_posted'), existing_id))
                return 'reposted'
            else:
                return 'exists'  # Same or older date, skip

        company_name = job.get('company_name') or 'Unknown'
        job_title = job.get('job_title') or 'Position Available'

        locations = job.get('locations', [])
        if locations:
            loc = locations[0]
            location_str = loc.get('address') or f"{loc.get('city', '')}, {loc.get('region', '')}, {loc.get('country', '')}".strip(', ')
            country = normalize_country(loc.get('country', source_country))
        else:
            location_str = source_country
            country = normalize_country(source_country)

        company_data = job.get('company_data', {}) or {}
        industries = company_data.get('industries', [])
        industry = ', '.join(industries) if industries else None

        company_linkedin = company_data.get('linkedin_link')
        size_range = company_data.get('size_range', {}) or {}
        if size_range:
            min_size = size_range.get('min', '')
            max_size = size_range.get('max', '')
            company_size = f"{min_size}-{max_size}" if min_size and max_size else str(min_size or max_size or '')
        else:
            company_size = None
        services = company_data.get('services', [])
        company_services = ', '.join(services) if services else None
        language = job.get('language')
        job_categories_list = job.get('job_categories', [])
        job_categories = ', '.join(job_categories_list) if job_categories_list else None

        # Insert new job
        company_favicon = job.get('company_logo') or get_favicon_from_website(job.get('company_link'))
        cur.execute("""
            INSERT INTO jobs (
                title, company, location, job_description, external_job_url,
                company_website, employment_type, salary, job_posting_time,
                work_mode, company_favicon, seniority_level, industry,
                created_at, last_updated, is_active, country, posted_date,
                days_since_posted, source, company_linkedin, company_size,
                company_services, language, job_categories
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            job_title, company_name, location_str,
            job.get('description', '')[:4000] if job.get('description') else None,
            external_url, job.get('company_link'), job.get('job_type'),
            format_salary(job.get('salary_range')), job.get('date_posted'),
            job.get('location_type'), company_favicon,
            format_seniority(job.get('yoe_range')), industry,
            datetime.now(), datetime.now(), True, country, posted_date, days_since,
            'hirebase', company_linkedin, company_size, company_services, language, job_categories
        ))
        return 'inserted'
    except Exception as e:
        print(f"    Error: {e}")
        return 'error'

def fetch_and_import(country, max_jobs, is_weekly_refresh=False):
    """Fetch jobs from a country - only jobs posted since yesterday"""

    # Use yesterday's date to catch new jobs
    last_sync = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"\n{'='*60}")
    print(f"🌍 Fetching jobs from {country}")
    print(f"📅 Jobs posted after: {last_sync}")
    print(f"🏭 Industries: Construction, Corporate Services, Education, Media & Communications, Tech/Software/IT")
    if is_weekly_refresh:
        print(f"🔄 Mode: WEEKLY REFRESH (full scan)")
    else:
        print(f"⚡ Mode: INCREMENTAL (only new jobs since last sync)")
    print(f"{'='*60}")

    conn = get_db_connection()
    cur = conn.cursor()

    api_calls = 0
    inserted = 0
    reposted = 0
    existing = 0
    skipped = 0

    while api_calls < max_jobs:
        batch_size = min(100, max_jobs - api_calls)
        page = (api_calls // 100) + 1

        try:
            # Build request with date filter
            request_json = {
                "geo_locations": [{"country": country}],
                "limit": batch_size,
                "page": page,
                "sort": "date_posted",
                "sort_order": "desc",
                "industry": ["Construction", "Corporate Services", "Education", "Media & Communications", "Tech, Software & IT Services"],
                "date_posted_after": last_sync  # Only jobs posted after last sync
            }

            response = requests.post(
                API_URL,
                headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
                json=request_json,
                timeout=30
            )

            if response.status_code != 200:
                print(f"  ❌ API error: {response.status_code} - {response.text[:200]}")
                break

            data = response.json()
            jobs = data.get('jobs', [])
            total_available = data.get('total_count', 0)

            if page == 1:
                print(f"  📊 Total jobs available since {last_sync}: {total_available}")

            if not jobs:
                print(f"  ✅ No more jobs available (fetched all {api_calls} jobs posted since {last_sync})")
                break

            # Track jobs this page
            page_inserted = 0
            page_reposted = 0
            page_existing = 0

            # Process each job immediately
            for job in jobs:
                result = insert_job(cur, job, country)
                if result == 'inserted':
                    inserted += 1
                    page_inserted += 1
                elif result == 'reposted':
                    reposted += 1
                    page_reposted += 1
                elif result == 'exists':
                    existing += 1
                    page_existing += 1
                else:
                    skipped += 1

                # Commit after each job
                conn.commit()

            api_calls += len(jobs)

            # Progress update after each batch
            print(f"  ✓ Page {page}: {page_inserted} new, {page_reposted} reposted, {page_existing} unchanged", flush=True)

        except Exception as e:
            print(f"  ❌ Error fetching batch: {e}")
            break

    cur.close()
    conn.close()

    # Always update sync tracker with today's date (even if no new inserts)
    # This ensures we don't re-scan the same date range tomorrow
    update_sync_tracker(country, inserted + reposted)

    print(f"\n✅ {country} complete:")
    print(f"   New jobs added:  {inserted}")
    print(f"   Reposted/updated:{reposted}")
    print(f"   Unchanged:       {existing}")
    print(f"   API calls used:  {api_calls}")

    return inserted, reposted, existing, skipped

def get_daily_schedule():
    """
    Returns country schedule in priority order.
    Each country gets up to 10K calls - but will stop early when it runs out of new jobs.
    Leftover calls cascade to next country in priority.
    """
    # Priority order - each gets up to 10K (will use less if fewer new jobs available)
    schedule = [
        # Top priority
        ("United Arab Emirates", 10000),
        ("Saudi Arabia", 10000),
        # Secondary
        ("USA", 10000),
        ("India", 10000),
        ("UK", 10000),
        ("Canada", 10000),
        ("Pakistan", 10000),
        ("Australia", 10000),
        # European countries
        ("Germany", 10000),
        ("France", 10000),
        ("Netherlands", 10000),
        ("Spain", 10000),
        ("Italy", 10000),
        ("Sweden", 10000),
        ("Switzerland", 10000),
        ("Ireland", 10000),
        ("Poland", 10000),
        ("Belgium", 10000),
    ]
    return schedule


def main():
    """Main sync process"""
    print(f"\n{'='*60}")
    print(f"🚀 Daily Job Sync - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"DEBUG: API_KEY = {API_KEY[:15]}...{API_KEY[-5:]}")

    # CHECK DAILY LIMIT FIRST - before doing anything
    remaining_calls, used_today = check_daily_limit()
    print(f"\n📊 API Usage Check:")
    print(f"   Already used today: {used_today}/{MAX_DAILY_API_CALLS}")
    print(f"   Remaining:          {remaining_calls}")

    if remaining_calls == 0:
        print(f"\n🛑 STOPPING: Daily API limit already reached ({used_today}/{MAX_DAILY_API_CALLS})")
        print(f"   No API calls will be made. Try again tomorrow.\n")
        sys.exit(0)

    # Check if it's a weekly refresh day (Sunday)
    day_name = datetime.now().strftime('%A')
    is_weekly_refresh = (day_name == "Sunday")

    # Get today's schedule
    schedule = get_daily_schedule()
    print(f"\n📅 Today is {day_name}")
    if is_weekly_refresh:
        print(f"🔄 WEEKLY REFRESH MODE - doing deeper scan")

    # Show countries in priority order
    print(f"\n📋 Countries (in priority order):")
    for i, (country, _) in enumerate(schedule[:8], 1):  # Show first 8
        last_sync = get_last_sync_date(country)
        print(f"   {i}. {country} (last sync: {last_sync})")
    print(f"   ... and {len(schedule) - 8} more European countries")

    total_inserted = 0
    total_reposted = 0
    total_existing = 0
    total_api_calls = 0

    # Fetch from each country in priority order until we hit 10K limit
    for country, _ in schedule:
        # Check remaining calls before each country
        current_remaining, current_used = check_daily_limit()

        if current_remaining <= 0:
            print(f"\n🛑 Daily limit reached ({MAX_DAILY_API_CALLS} calls). Stopping.")
            break

        print(f"\n💰 Remaining API budget: {current_remaining}")

        # Give this country all remaining calls (it will stop when it runs out of new jobs)
        ins, repost, exist, skip = fetch_and_import(country, current_remaining, is_weekly_refresh)
        total_inserted += ins
        total_reposted += repost
        total_existing += exist

        # Update API usage tracking in database
        actual_calls = ins + repost + exist + skip
        if actual_calls > 0:
            update_api_usage(actual_calls)
            total_api_calls += actual_calls

        # If this country used 0 calls (no new jobs), continue to next
        if actual_calls == 0:
            print(f"  ℹ️  No new jobs in {country}, trying next country...")
            continue

    # Final summary
    final_remaining, final_used = check_daily_limit()
    print(f"\n{'='*60}")
    print(f"🎉 Daily Sync Complete!")
    print(f"   New jobs added:     {total_inserted}")
    print(f"   Reposted/updated:   {total_reposted}")
    print(f"   Unchanged:          {total_existing}")
    print(f"   API calls this run: {total_api_calls}")
    print(f"   Total used today:   {final_used}/{MAX_DAILY_API_CALLS}")
    print(f"   Remaining today:    {final_remaining}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
