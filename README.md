# Portals AutoCareer - Hirebase Sync

Daily automated job sync from Hirebase API to Supabase.

## Features
- Runs daily at 2 AM UTC
- Syncs US, India, MEA, Canada, UK jobs
- Auto-cleanup of jobs >30 days
- GitHub issue alert on failures
- 10K jobs/day within API limit

## Setup
1. Add secrets: `HIREBASE_API_KEY`, `SUPABASE_PASSWORD`
2. Enable Actions
3. Done!
