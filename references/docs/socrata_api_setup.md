# Socrata API Setup

This project downloads data from [NY Open Data](https://data.ny.gov/) via the
Socrata SODA3 API.  The API works without authentication, but anonymous
requests are subject to **strict throttling** (rate limits).  Registering for a
free app token raises those limits significantly.

## Getting a Socrata App Token

1. Go to <https://data.ny.gov/signup> and create a free account (or sign in).
2. After signing in, navigate to **Developer Settings**:
   <https://data.ny.gov/profile/edit/developer>
3. Click **Create New App Token**.
4. Fill in a name (e.g. `mta-ridership-pipeline`) and an optional description.
5. Click **Save**.  You will see two values:
   - **App Token** — this is the value you need (`SOCRATA_APP_TOKEN`).
   - **Secret Token** — optional; only needed for OAuth-based writes.  You can
     record it as `SOCRATA_SECRET_TOKEN` if you wish, but it is not required
     for downloading public data.

## Configuring the Project

### Option A — `.env` file (recommended)

1. Copy the template:

   ```bash
   cp .env.example .env
   ```

2. Open `.env` and replace the placeholder values with your real tokens:

   ```dotenv
   SOCRATA_APP_TOKEN=your_app_token_here
   SOCRATA_SECRET_TOKEN=your_secret_token_here
   ```

   `.env` is listed in `.gitignore` and will **not** be committed.

### Option B — Environment variables

Export the variables in your shell (or add them to your shell profile):

```bash
export SOCRATA_APP_TOKEN=your_app_token_here
```

### Option C — CLI argument

Pass the token directly when running a script:

```bash
python scripts/local/data/update_ridership_data.py --app-token YOUR_TOKEN
```

> **Note:** CLI arguments appear in shell history and process listings.
> Prefer `.env` or environment variables for routine use.

## Resolution Order

When a script needs a token it checks, in order:

1. `--app-token` / `--secret-token` CLI argument (highest priority)
2. `SOCRATA_APP_TOKEN` / `SOCRATA_SECRET_TOKEN` environment variables
3. `.env` file at the repository root (fills unset vars only via `python-dotenv`)

If no token is found the scripts still run, but you may encounter HTTP 429
(rate-limit) errors.  When that happens the scripts will print a warning and
retry with exponential back-off.

## Rate Limits

| Access type       | Approximate limit                       |
|-------------------|-----------------------------------------|
| Anonymous         | ~1 000 requests / hour (shared IP pool) |
| With app token    | ~10 000+ requests / rolling window      |

Exact limits are set by NY Open Data and may change.  See
<https://dev.socrata.com/docs/app-tokens.html> for the latest details.

## Scripts That Use the Token

| Script | Purpose |
|--------|---------|
| `scripts/local/data/update_ridership_data.py` | Download monthly ridership CSVs |
| `scripts/local/data/update_turnstile_data.py` | Download monthly turnstile CSVs |
| `scripts/api/calculate_ridership_by_station.py` | Build station-level ridership from API |
