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

## Limits to know

- Free Edition has a fair-use compute quota. If the hourly pipeline exhausts it, compute stops
  for the rest of the day. Lower the pipeline cadence in `resources/jobs.yml` if that happens.
- GitHub cron is best effort: runs can start a few minutes late or be skipped under load, which
  shows up as gaps in the data. The reliability score already treats missing polls as gaps.
- Free Edition cannot be used commercially.
