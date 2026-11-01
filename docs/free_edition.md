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

## Limits to know

- Free Edition has a fair-use compute quota. If the hourly pipeline exhausts it, compute stops
  for the rest of the day. Lower the pipeline cadence in `resources/jobs.yml` if that happens.
- GitHub cron is best effort: runs can start a few minutes late or be skipped under load, which
  shows up as gaps in the data. The reliability score already treats missing polls as gaps.
- Free Edition cannot be used commercially.
