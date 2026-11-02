# Running on Databricks Free Edition (no cost)

Free Edition is serverless-only and can only reach a short list of trusted domains, so the
TfL API is not reachable from inside Databricks. The split is:

| Piece | Where it runs |
|---|---|
| Collector (TfL poll every 5 min) | GitHub Actions, `.github/workflows/collector.yml` |
| Raw files | Unity Catalog volume `workspace.landing.raw`, uploaded by the workflow |
| Bronze / Silver / Gold | Databricks pipeline job, hourly |

The Azure path (`infra/main.bicep`, `scripts/bootstrap.ps1`) is unchanged and is the
production design: ADLS Gen2, Access Connector, external location, collector inside Databricks.

## Steps

1. Sign up at <https://www.databricks.com/learn/free-edition> and note the workspace URL.
2. Deploy: `powershell -ExecutionPolicy Bypass -File scripts\bootstrap_free.ps1 -WorkspaceUrl <url> -Email <you>`
3. In the workspace, create a personal access token (Settings > Developer > Access tokens).
4. Add repository secrets (each command prompts for the value, so nothing lands in shell history):
   ```
   gh secret set DATABRICKS_HOST
   gh secret set DATABRICKS_TOKEN
   gh secret set TFL_APP_KEY      # optional
   ```
5. Merge the branch to `main` (scheduled workflows only run on the default branch), then
   run the `collector` workflow once by hand from the Actions tab.
6. Check Catalog > workspace > landing > raw for files, then run
   `databricks bundle run -t free pulsecity_pipeline` and query `workspace.gold.*`.

## Problems we hit (and the fixes)

- **`databricks auth login` hangs** in a terminal tool with no browser: use a token instead,
  `databricks configure --host <url> --profile pulsecity`.
- **Pasted tokens can carry an invisible leading character**, which fails with
  `invalid header field value for "Authorization"`. A real token starts with `dapi`. Copy it with
  the copy button, not by selecting text, and strip anything outside `[A-Za-z0-9_.-]`.
- **Piping a secret into `gh secret set` from Windows PowerShell 5.1 adds a BOM** to the value
  (`parse "\ufeffhttps://..."` in the workflow log). Use `--body "<value>"` instead of a pipe.
- **Collector crashed with `No module named 'urllib3'`**: the workflow installs the wheel with
  `--no-deps`, so `requests` must be installed separately with its own dependencies.
- **`CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC` in `build_silver`**: serverless client `"2"`
  rejects `DataFrame.cache()`. `resources/jobs.yml` uses client `"5"`.
- **`gh workflow run` returns 404 for a new workflow** until the workflow file exists on `main`.
- **Tests on Windows**: PySpark needs Hadoop's `winutils.exe` there. Rely on the `ci` workflow
  (Linux, runs on every push) or use WSL; native Windows is not supported.
- **Windows long paths**: install the dev dependencies into a short venv path such as `C:\pcvenv`,
  or pyspark installs incompletely.

## Keeping the collector running

A `*/5` cron on a new repository fired only 3 times in 15 hours, so the workflow does not rely on it:

- **One run = 30 minutes.** It polls 6 times, 5 minutes apart, then starts the next run itself
  (`gh workflow run` with the built-in token). Runs queue behind each other, so polls never
  overlap or come closer than 5 minutes.
- **Cron is the restart.** `17,47 * * * *` (off-peak minutes) revives the chain if a run is
  cancelled, times out or a dispatch fails.
- **To stop it:** disable the `collector` workflow in the Actions tab (cancelling one run alone
  stops that run's hand-off, but the cron will restart it).
- The repository is public, so the runner minutes are free. On a private repo this would use
  about 720 hours a month, far over the free allowance.

Optional, a stronger trigger from outside GitHub: create a fine-grained token limited to this
repository with **Actions: read and write**, then have a free cron service (for example
cron-job.org) `POST https://api.github.com/repos/<owner>/<repo>/actions/workflows/collector.yml/dispatches`
with body `{"ref":"main"}` every 30 minutes. Extra triggers are harmless: the concurrency group
keeps only one queued run. Databricks itself cannot do this call, because Free Edition blocks
outbound internet.

## Reporting on thin or gappy data

Build the report on `gold.agg_network_health_hourly` and `gold.agg_line_reliability_daily` (hourly
and daily grain), not on the per-poll fact. A missed poll then shows up as lower `coverage_pct`
in the daily table instead of looking like an outage.

## Limits to know

- Free Edition has a fair-use compute quota. If the hourly pipeline exhausts it, compute stops
  for the rest of the day. Lower the pipeline cadence in `resources/jobs.yml` if that happens.
- GitHub cron is best effort: runs can start a few minutes late or be skipped under load, which
  shows up as gaps in the data. The reliability score already treats missing polls as gaps.
- Free Edition cannot be used commercially.
