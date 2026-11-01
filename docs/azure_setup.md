# Deploying to Azure

About an hour, start to finish. You need an Azure subscription where you can create resources.

> **Start the collector as early as you can.** Reliability scores need history: aim for at least
> two weeks of polls before you take dashboard screenshots.

## 1. Azure resources

Create these in one resource group (for example `rg-pulsecity`, region UK South):

1. **Storage account** (ADLS Gen2)
   - Performance: Standard, Redundancy: LRS
   - Advanced tab: turn on **Hierarchical namespace** (this is what makes it ADLS Gen2)
   - After creation, add a container called `lakehouse`
2. **Azure Databricks workspace**, Premium tier (required for Unity Catalog). New workspaces
   have Unity Catalog enabled automatically.
3. **Access Connector for Azure Databricks** (a managed identity Databricks uses to reach
   storage). Then, on the storage account, go to Access control (IAM) and grant the access
   connector the **Storage Blob Data Contributor** role.

Set a budget alert on the resource group (Cost Management, then Budgets) before going further.

## 2. Unity Catalog storage

In the Databricks workspace, open **Catalog**, then **External data**:

1. **Credentials**: create a storage credential of type Azure Managed Identity, using the
   access connector's resource ID. Name it `pulsecity_mi`.
2. **External locations**: create `pulsecity_lakehouse` with URL
   `abfss://lakehouse@<storage-account>.dfs.core.windows.net/pulsecity` and the credential above.
   Click **Test connection**.

Your account needs `CREATE CATALOG` on the metastore. Workspace admins usually have it.

## 3. TfL API key

1. Register at <https://api-portal.tfl.gov.uk>, subscribe to the free product and copy the
   primary key.
2. Install the Databricks CLI and log in:

```bash
databricks auth login --host https://<your-workspace>.azuredatabricks.net
```

3. Store the key in a secret scope:

```bash
databricks secrets create-scope pulsecity
```

```bash
databricks secrets put-secret pulsecity tfl_app_key
```

TfL also answers without a key, at a lower rate limit, so the collector still runs if this step
is skipped. A 5-minute poll is far below either limit.

## 4. Deploy

```bash
export BUNDLE_VAR_notification_email="you@example.com"
export BUNDLE_VAR_storage_root="abfss://lakehouse@<storage-account>.dfs.core.windows.net/pulsecity"
```

```bash
databricks bundle deploy -t prod
```

```bash
databricks bundle run -t prod pulsecity_setup
```

The collector starts on its next 5-minute mark and the pipeline runs hourly. Check that files are
arriving in **Catalog → pulsecity → landing → raw**.

The `dev` target deploys the same jobs with schedules paused, into the `pulsecity_dev` catalog,
for trying changes safely. Run a job on demand with:

```bash
databricks bundle run -t dev pulsecity_pipeline
```

## 5. Power BI

1. In Databricks, open **SQL Warehouses** and use the serverless starter warehouse (or create a
   small serverless one, auto-stop 5 minutes).
2. Copy its **Server hostname** and **HTTP path** from Connection details.
3. In Power BI Desktop: Get data, Azure Databricks, paste both, sign in with Microsoft Entra ID.
4. Pick tables from `pulsecity.gold`. See [powerbi.md](powerbi.md) for the model and measures.

## 6. Watch the cost

Serverless jobs bill per use; nothing runs between job runs. The collector is the main cost
because it runs 288 times a day. After the first day, check what it actually costs:

```sql
SELECT usage_date, sku_name, sum(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_metadata.job_name LIKE 'pulsecity%'
GROUP BY ALL
ORDER BY usage_date DESC;
```

To lower the cost, poll less often: change `quartz_cron_expression` in `resources/jobs.yml`
(for example `0 0/10 * * * ?`) and `poll_interval_minutes` in `src/pulsecity/cities/london.yml`
together, then redeploy.

## Tear down

```bash
databricks bundle destroy -t prod
```

Then delete the resource group in the Azure portal.
