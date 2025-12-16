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
    """Insert new job only - skip if already exists"""
    try:
        external_url = job.get('application_link')
        if not external_url:
            return 'skipped'

        # Check if job already exists - if so, skip it (don't update daily)
        cur.execute("SELECT id FROM jobs WHERE external_job_url = %s", (external_url,))
        existing = cur.fetchone()

        if existing:
            return 'exists'  # Skip existing jobs - we only want NEW jobs

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

        posted_date, days_since = parse_date_posted(job.get('date_posted'))

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

def fetch_and_import(country, max_jobs):
    """Fetch jobs from a country - only add NEW jobs, skip existing ones"""
    print(f"\n{'='*60}")
    print(f"🌍 Fetching up to {max_jobs} API calls from {country}")
    print(f"🏭 Industry filters: Construction, Corporate Services, Education, Media & Communications, Tech/Software/IT")
    print(f"🔍 Mode: NEW JOBS ONLY (skipping existing)")
    print(f"{'='*60}")

    conn = get_db_connection()
    cur = conn.cursor()

    api_calls = 0
    inserted = 0
    existing = 0
    skipped = 0

    while api_calls < max_jobs:
        batch_size = min(100, max_jobs - api_calls)
        page = (api_calls // 100) + 1

        try:
            response = requests.post(
                API_URL,
                headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
                json={
                    "geo_locations": [{"country": country}],
                    "limit": batch_size,
                    "page": page,
                    "industry": ["Construction", "Corporate Services", "Education", "Media & Communications", "Tech, Software & IT Services"]
                },
                timeout=30
            )

            if response.status_code != 200:
                print(f"  ❌ API error: {response.status_code} - {response.text[:200]}")
                break

            data = response.json()
            jobs = data.get('jobs', [])

            if not jobs:
                print(f"  ℹ️  No more jobs available from API (page {page})")
                break

            # Process each job immediately
            for job in jobs:
                result = insert_job(cur, job, country)
                if result == 'inserted':
                    inserted += 1
                elif result == 'exists':
                    existing += 1
                else:
                    skipped += 1

                # Commit after each job
                conn.commit()

            api_calls += len(jobs)

            # Progress update after each batch
            print(f"  ✓ Page {page}: {api_calls} API calls, {inserted} new jobs found, {existing} already in DB", flush=True)

        except Exception as e:
            print(f"  ❌ Error fetching batch: {e}")
            break

    cur.close()
    conn.close()

    print(f"\n✅ {country} complete: {inserted} NEW jobs added, {existing} already existed, {skipped} skipped")
    return inserted, existing, skipped

def get_daily_schedule():
    """
    Returns country schedule - same every day.
    Stays within 10K/day API limit.
    Priority: UAE 80%, USA 10%, Saudi 5%, UK 5%
    """
    # Same schedule every day
    schedule = [
        ("United Arab Emirates", 8000),  # 80%
        ("USA", 1000),                    # 10%
        ("Saudi Arabia", 500),            # 5%
        ("UK", 500),                      # 5%
    ]
    return schedule

def cleanup_old_jobs(days_threshold=30):
    """Deactivate jobs older than threshold based on posted_date"""
    conn = get_db_connection()
    cur = conn.cursor()

    print(f"\n{'='*60}")
    print(f"🧹 Cleaning up jobs older than {days_threshold} days...")
    print(f"{'='*60}")

    from datetime import timedelta
    cutoff_date = datetime.now() - timedelta(days=days_threshold)

    cur.execute("""
        UPDATE jobs
        SET is_active = false, last_updated = NOW()
        WHERE is_active = true
        AND posted_date < %s
    """, (cutoff_date,))

    affected = cur.rowcount
    conn.commit()

    print(f"✅ Deactivated {affected} old jobs (posted before {cutoff_date.strftime('%Y-%m-%d')})")

    cur.execute("SELECT COUNT(*) FROM jobs WHERE is_active = true")
    active_count = cur.fetchone()[0]
    print(f"📊 Total active jobs: {active_count}")

    cur.close()
    conn.close()

def main():
    """Main sync process"""
    print(f"\n{'='*60}")
    print(f"🚀 Daily Job Sync - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"DEBUG: API_KEY = {API_KEY[:15]}...{API_KEY[-5:]}")

    # HARD LIMIT: Never exceed 10K jobs/day to prevent overcharges
    MAX_DAILY_JOBS = 10000

    # Get today's schedule
    schedule = get_daily_schedule()
    day_name = datetime.now().strftime('%A')
    print(f"\n📅 Today is {day_name}")
    print(f"📋 Scheduled countries:")

    # Calculate total and enforce limit
    total_planned = sum(count for _, count in schedule)
    if total_planned > MAX_DAILY_JOBS:
        print(f"\n⚠️  FATAL: Schedule exceeds daily limit!")
        print(f"   Planned: {total_planned}, Max: {MAX_DAILY_JOBS}")
        print(f"   ABORTING to prevent overcharges\n")
        sys.exit(1)

    for country, count in schedule:
        print(f"   - {country}: {count} API calls")

    total_inserted = 0
    total_existing = 0
    total_api_calls = 0

    # Fetch and import jobs from each country
    for country, max_jobs in schedule:
        # Safety check before each fetch
        if total_api_calls + max_jobs > MAX_DAILY_JOBS:
            print(f"\n⚠️  STOPPING: Would exceed {MAX_DAILY_JOBS} API call limit")
            break

        ins, exist, skip = fetch_and_import(country, max_jobs)
        total_inserted += ins
        total_existing += exist
        total_api_calls += max_jobs

    # Cleanup old jobs
    cleanup_old_jobs(days_threshold=30)

    # Final summary
    print(f"\n{'='*60}")
    print(f"🎉 Daily Sync Complete!")
    print(f"   New jobs added:     {total_inserted}")
    print(f"   Already in DB:      {total_existing}")
    print(f"   API calls used:     ~{total_api_calls}/10000")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
