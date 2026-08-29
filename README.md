# Microsoft Fabric Skills Showcase — Airbnb UK Portfolio Project

A hands-on, end-to-end tour of Microsoft Fabric, built around a single consistent dataset (Inside Airbnb data for **London**, **Bristol** and **Manchester**) so that every core Fabric capability has a real, working example behind it rather than a disconnected demo. This is not a narrow single-pipeline project, it's a structured walkthrough of the platform, written up as applied engineering work rather than a tutorial.

> **How to use this document**: it's written as a single reference so it prints cleanly from GitHub (Print → Save as PDF works well) or reads top to bottom as a study log. Each module is self-contained, so feel free to jump straight to the section you need from the table of contents below.

## Table of contents

1. [Project overview](#1-project-overview)
2. [Setting up a Microsoft Fabric environment](#2-setting-up-a-microsoft-fabric-environment)
3. [Fabric fundamentals](#3-fabric-fundamentals)
4. [Lakehouse](#4-lakehouse)
5. [Fabric Data Factory](#5-fabric-data-factory)
6. [OneLake and shortcuts](#6-onelake-and-shortcuts)
7. [Fabric Synapse Data Engineering](#7-fabric-synapse-data-engineering)
8. [Migration to Microsoft Fabric](#8-migration-to-microsoft-fabric)
9. [Fabric Capacity Metrics app](#9-fabric-capacity-metrics-app)
10. [Fabric Synapse Data Warehouse](#10-fabric-synapse-data-warehouse)
11. [Fabric access control and permissions](#11-fabric-access-control-and-permissions)
12. [Microsoft Fabric Power BI](#12-microsoft-fabric-power-bi)
13. [Troubleshooting log and lessons learned](#13-troubleshooting-log-and-lessons-learned)
14. [Certification readiness: DP-700 (Fabric Data Engineer Associate)](#14-certification-readiness-dp-700-fabric-data-engineer-associate)
15. [Glossary](#15-glossary)

---

## 1. Project overview

### Objective

Showcase Microsoft Fabric data engineering skills through a project that touches every major part of the platform: environment setup and licensing, the Lakehouse, Data Factory, OneLake shortcuts, Synapse Data Engineering, a Synapse-to-Fabric migration, capacity monitoring, the Data Warehouse, access control, and Power BI reporting, all exercised against one real dataset rather than twelve disconnected tutorials.

### Scope (12 modules)

| # | Module | Covers |
|---|--------|--------|
| 1 | Environment setup | Resource group, Fabric capacity, ADLS Gen2, Synapse workspace, Spark pool |
| 2 | Fabric fundamentals | Evolution of data architectures, Delta Lake, licensing and capacity SKUs |
| 3 | Lakehouse | Workspaces, ingesting data, SQL analytics endpoint, OneLake file explorer |
| 4 | Fabric Data Factory | Gateways, connections, pipelines, Dataflow Gen2 |
| 5 | OneLake and shortcuts | Shortcut mechanics, update/deletion behaviour, Files vs Tables |
| 6 | Synapse Data Engineering | Spark pools and sessions, `mssparkutils`, managed/external tables, Domains |
| 7 | Synapse → Fabric migration | Notebooks, pipelines, ADLS → OneLake |
| 8 | Capacity Metrics app | Compute monitoring, throttling and smoothing |
| 9 | Synapse Data Warehouse | `COPY INTO`, pipelines, Dataflow Gen2, Lakehouse vs Warehouse |
| 10 | Access control and permissions | Tenant, capacity, workspace and item-level roles, row/column security |
| 11 | Power BI | Reporting layer on top of the Lakehouse/Warehouse |
| 12 | *(full scope, no further modules expected)* | — |

### Dataset

[Inside Airbnb](http://insideairbnb.com/) data, one snapshot per city (London, Bristol, Manchester):

- **Listings** *(static/dimension)* — property and host attributes.
- **Neighbourhoods** *(static/dimension)* — geographic reference data.
- **Calendar** *(fact)* — date-level price and availability. This is a point-in-time snapshot rather than genuinely incremental history: a single scrape captures roughly the next 365 days of availability, and Airbnb's own calendar doesn't distinguish a guest-booked night from a host-blocked one (both show as "unavailable").
- **Reviews** *(fact)* — genuinely incremental and date-stamped, so it's the table actually split into `year-month` batches (`2021-01`, `2021-02`, …) for the incremental-load exercises.

Landing structure in ADLS, Hive-style partitioning:

```
bronze/
  city=London/
    static/
      listings/listings.csv
      neighbourhoods/neighbourhoods.csv
    fact/
      calendar/year=2021/month=01/calendar.csv
      reviews/year=2021/month=01/reviews.csv
  city=Bristol/...
  city=Manchester/...
```

---

## 2. Setting up a Microsoft Fabric environment

1. **Create a resource group** in Azure to hold every resource this project touches (storage account, Synapse workspace, Key Vault) — keeping everything in one resource group makes it easy to tear down or track cost later.
2. **Provision a Microsoft Fabric capacity** (or start a Fabric trial capacity) — this is what Fabric workspaces attach to for compute.
3. **Create an Azure Data Lake Storage Gen2 account**, with hierarchical namespace enabled, to act as the external "source system" this project migrates data from.
4. **Create an Azure Synapse Analytics workspace**:
   - In the Azure portal, search for **Azure Synapse Analytics** → **Create**.
   - Choose the resource group from step 1, give the workspace a name, and select (or create) an ADLS Gen2 account and file system for it to use as its primary storage.
   - Set a SQL admin login and password for the built-in serverless SQL pool.
   - Review networking settings (public endpoint is fine for a learning project) and **Create**.
5. **Create a new Spark pool** inside the Synapse workspace (Synapse Studio → **Manage** → **Apache Spark pools** → **New**), sized modestly (e.g. small nodes, autoscale off) since this is only used for occasional notebook runs during the migration exercises, not production workloads.

---

## 3. Fabric fundamentals

### Evolution of data architectures

Fabric sits at the end of a fairly linear evolution: traditional **data warehouses** (structured, schema-on-write, great for BI but expensive to scale and rigid to change) gave way to **data lakes** (cheap, schema-on-read storage for any file type, but without transactional guarantees or governance). The **lakehouse** pattern, built on open table formats like **Delta Lake**, combines the two: files sitting in cheap object storage, but with ACID transactions, schema enforcement, time travel and BI-grade performance layered on top via a transaction log (`_delta_log`). Microsoft Fabric is Microsoft's SaaS implementation of this idea: one underlying storage layer (**OneLake**, effectively "OneDrive for data") shared across every Fabric workload (Lakehouse, Warehouse, Data Factory, Power BI, Real-Time Intelligence), so the same Delta tables are queryable from any engine without copying data between them.

### Delta Lake, in brief

A Delta table is just a folder of Parquet files plus a `_delta_log` subfolder recording every transaction (inserts, updates, deletes, schema changes) as an ordered sequence of JSON/checkpoint files. This is what gives Fabric's Lakehouse tables ACID guarantees, time travel, and the ability for multiple engines (Spark, SQL, Power BI) to read the same physical data consistently.

### Enabling and accessing Fabric

Fabric is enabled per Microsoft 365/Entra tenant, either by starting a **Fabric trial** (free for 60 days, no capacity purchase required) or by provisioning a paid **Fabric capacity** (an Azure resource, SKUs run from F2 up to F2048, each F-SKU number roughly corresponding to a Power BI Premium P-SKU equivalent). Once enabled, users access Fabric at `app.fabric.microsoft.com` with their organisational Entra ID account.

### Licensing and costing, conceptually

Fabric capacity is billed by **Capacity Units (CUs)**, a measure of compute-seconds consumed across every workload attached to that capacity, not per-user licensing for creators (though **Power BI Pro/PMU** licences are still needed for *consumers* viewing certain Power BI content). Capacities can be **paused** when not in use to stop billing, and can be scaled up or down without recreating the workspace. See [§9](#9-fabric-capacity-metrics-app) for how consumption is actually monitored.

---

## 4. Lakehouse

A **Lakehouse** is a Fabric item that combines a Delta Lake storage layer (organised into a **Tables** folder for managed Delta tables and a **Files** folder for anything else) with an automatically generated **SQL analytics endpoint**, so the same data is queryable both from Spark notebooks and from T-SQL without any duplication.

- **Workspaces** are the organisational container for Fabric items (Lakehouses, Warehouses, notebooks, pipelines, reports) and are what get attached to a **Fabric capacity** for compute.
- **Ingesting data to a Lakehouse** can be done by uploading files directly through the UI, running a notebook that writes a DataFrame with `.saveAsTable()`, running a pipeline **Copy activity**, or using **Dataflow Gen2** (see [§5](#5-fabric-data-factory)).
- The **SQL analytics endpoint** is a read-only, auto-generated SQL surface over every Delta table in the Lakehouse, letting BI tools and T-SQL users query the data without needing Spark.
- The **OneLake file explorer** is a Windows Explorer-style desktop app that mounts OneLake as a mapped drive, letting you drag and drop files into a Lakehouse's Files or Tables folders exactly like a local folder.

---

## 5. Fabric Data Factory

### Data gateways

A gateway is what lets Fabric reach data that isn't already in the cloud. There are two types:

| Gateway type | Purpose |
|---|---|
| **On-premises data gateway** | Secure bridge between on-premises data (e.g. an on-prem SQL Server) and Microsoft cloud services such as Fabric, Power BI and Power Apps. |
| **Virtual network (VNet) data gateway** | Connects Microsoft cloud services to Azure data services that sit inside a private virtual network, without needing an on-premises machine to host the gateway. |

**Installing an on-premises data gateway**:

1. From the Fabric home page, click the **download** icon → **Data Gateway** → **Download (standard mode)**.
2. Install it, sign in with your Fabric account, and register a new gateway name.
3. Record the recovery key somewhere safe, it's needed if the gateway ever needs restoring on a different machine.
4. To confirm it's live: **Settings** → **Manage connections and gateways** → **On-premises data gateways**, status should read **Online**.

### Connections

A connection is how Fabric authenticates to a data source, conceptually the same idea as a *linked service* in Azure Data Factory or Synapse. There are three connection types: **on-premises**, **VNet**, and **cloud**.

**Connecting Fabric's SQL analytics endpoint to SSMS**: in the Lakehouse's SQL analytics endpoint, copy the SQL connection string, then in SQL Server Management Studio, **Connect → Database Engine**, paste the string as the server name, and authenticate with **Microsoft Entra password** using your Fabric email and password.

**Creating a connection to an on-premises SQL Server**:

1. In Fabric: **Settings** → **Manage connections and gateways** → **Connections** → **New connection**.
2. Choose **On-premises**, select the gateway cluster created above, and give the connection a name.
3. Connection type: **SQL Server**. Paste in the server name (from SSMS), choose the database, and set the authentication method to **Basic**, supplying the SQL Server username and password.

### Pipeline to ingest on-premises SQL data into a Lakehouse

A pattern for copying *every* table in a source database without hardcoding table names:

1. New **Data pipeline** → add a **Lookup** activity, pointed at the on-premises SQL connection, running:
   ```sql
   SELECT schema_name(t.schema_id) AS schema_name, t.name AS table_name
   FROM sys.tables t
   ORDER BY table_name;
   ```
2. Add a **ForEach** activity, set **Sequential** as needed, with items set to `@activity('Lookup1').output.value`.
3. Inside the ForEach, add a **Copy** activity:
   - **Source**: the on-premises SQL connection, database selected, with `schema_name` and `table_name` set as dynamic parameters: `@item().schema_name` and `@item().table_name`.
   - **Destination**: the target Lakehouse, with the table name set dynamically to `@item().table_name` and the write behaviour set to **Append**.
4. Save and run. Every table returned by the Lookup query gets copied into the Lakehouse in one pipeline run.

### Dataflow Gen2

1. New item → **Dataflow Gen2** → **Get data (more)** → **Azure Data Lake Storage**.
2. Before this will work, grant the Fabric user (or the identity running the dataflow) the **Storage Blob Data Contributor** role on the ADLS account (**Access control (IAM)** → **Add role assignment**).
3. Build the source URL from the storage account's file **Properties** → **Copy URL**, then edit it: change `blob` to `dfs` in the hostname, and remove the specific file name from the end so it points at the container/folder.
4. Set authentication to **Organizational account**, privacy level **Public**, then **Combine or create**.
5. Apply transformations with Power Query, then add a **data destination**: connect to an existing Lakehouse, name the output table, and save.

**Fabric Data Factory vs Azure Data Factory**: conceptually the same engine and pipeline model, but Fabric Data Factory is workspace-native (no separate ADF resource to provision), shares OneLake directly as a destination, and bundles Dataflow Gen2 as a first-class Power Query experience rather than the older Mapping Data Flows model in classic ADF.

---

## 6. OneLake and shortcuts

A **shortcut** is a OneLake object that points at data living somewhere else (another Lakehouse, a Warehouse, ADLS Gen2, Amazon S3, Dataverse, GCS) without copying it. Two consequences follow from that: updates at the source are reflected wherever the shortcut is read, and the data is accessed virtually rather than duplicated, so no extra storage cost.

### Prerequisites to create a shortcut

1. A source location (Lakehouse, Warehouse, ADLS, S3, Dataverse, GCS).
2. The right authentication against that source.
3. A destination: a Lakehouse or a KQL database.

### Shortcuts in the Files section

Straightforward: point a new shortcut at any container/subfolder/file in ADLS (grant the Fabric identity **Storage Blob Data Contributor** on the storage account first), build the URL from the file's **Properties** in ADLS, swap `blob` for `dfs`, and remove the file name from the end. Any format is fine here, CSV, JSON, Excel, loose Parquet.

### Shortcuts in the Tables section

This is stricter, and it's worth understanding *why*, since it's easy to hit "**Unable to identify these objects as tables. To keep these objects in the lakehouse, move them to Files.**" Fabric's Tables folder only auto-recognises a shortcut as a table when the shortcut points **directly at the root of a single, valid Delta table**, that is, a folder containing that table's data files *and* its `_delta_log` subfolder, sitting at the top level of Tables (not nested inside a subfolder). Pointing a Tables shortcut at a plain container, a folder holding loose Parquet or CSV files with no `_delta_log`, or a folder containing more than one dataset, gives Fabric nothing it can confidently identify as one table's structure, so it falls back to treating the shortcut as an unidentified object.

Practical takeaway confirmed through testing:

- **Table-format data** (Delta/Parquet with a proper `_delta_log`) → shortcut it directly into **Tables**, pointed at the exact table root.
- **Everything else** (loose CSV, JSON, Excel, or Parquet without a Delta log) → shortcut it into **Files** instead.
- Shortcuts in **Tables** are only supported at the *top level* of the Tables folder, not in subdirectories.

### Updating data through a shortcut

Whichever identity is running the update needs **Storage Blob Data Contributor** on the source ADLS account (grant via **Access control (IAM)**). From a Lakehouse notebook:

```python
df = spark.sql("SELECT * FROM LH_Fabric.shortcuttable")
display(df)

spark.sql("UPDATE LH_Fabric.shortcuttable SET column_name = 'value' WHERE column_name = 'old_value'")

# confirm
spark.sql("SELECT COUNT(*) FROM LH_Fabric.shortcuttable WHERE column_name = 'value'").show()
```

An update made through the Lakehouse layer is reflected back at the ADLS source, because the shortcut isn't a copy. The same is true in reverse: updating the data from a Synapse Spark notebook against the same ADLS location is reflected back in the Fabric Lakehouse the next time it's queried.

### Deletion behaviour (tested scenarios)

| Scenario | Effect |
|---|---|
| Delete data *through* the shortcut in the Lakehouse | Deleted at the ADLS source too — a shortcut isn't a copy, so deleting through it deletes the real data. |
| Delete specific rows/columns directly in the ADLS source file | Reflected back in the Lakehouse (and vice versa) — same underlying data either way. |
| Delete rows/columns of a shortcut **table** in the Lakehouse | Reflected in Synapse/ADLS, for the same reason. |
| Delete a row directly from Synapse Analytics | Reflected in both Fabric and ADLS. |
| Delete the **shortcut object itself** in the Lakehouse | The shortcut link is removed, but the underlying data in Synapse/ADLS is untouched, deleting a shortcut never deletes the source. |

---

## 7. Fabric Synapse Data Engineering

### Spark pools in Fabric

Each workspace has Spark compute settings managed under **Workspace settings** → **Data Engineering/Data Science** → **Spark settings**.

- **Starter pool** — the default pool. Sessions start in roughly 5–10 seconds because the underlying clusters are kept warm; you're only billed while a session is active. Default configuration: memory-optimised nodes, medium size, autoscale between 1–10 nodes, dynamic allocation on. An idle starter-pool session disconnects after 20 minutes.
- **Custom pool** — created for a specific project's needs, useful when budget is tight and a small, fixed node size is preferable to the starter pool's defaults. Created under **Workspace settings** → **Spark pool** → **New pool**, choosing node size, autoscale, and node count. Custom pools take longer to spin up (a couple of minutes, versus ~10 seconds for the starter pool).

### Standard vs high-concurrency sessions

By default, every notebook gets its own **standard** Spark session, each taking 2–3 minutes to initialise. A **high-concurrency session** lets a second notebook attach to a session already started by a first one, cutting startup from minutes to a few seconds, useful when one notebook's logic depends on another and you don't want to pay the initialisation cost twice. High-concurrency sessions are scoped to a single user, they can't be shared across different people. Benefits: one user can run multiple notebooks against a single shared session (avoiding repeated start-up delay), the session boundary stays within that one user for security, and it's more cost-effective through better resource utilisation.

### `mssparkutils` / `notebookutils`

A built-in package for common notebook tasks, file system operations, environment variables, chaining notebooks together, and working with secrets. `mssparkutils.help()` lists everything available; the main groups are:

- **Notebook utilities** — chaining Fabric notebooks together.
- **File system utilities** — filesystem operations inside Fabric (`mssparkutils.fs.help()`), including mounting external storage:
  ```python
  from notebookutils import mssparkutils

  accountKey = "<storage-access-key>"
  mssparkutils.fs.mount(
      "abfss://mycontainer@<accountname>.dfs.core.windows.net",
      "/test",
      {"accountKey": accountKey}
  )
  # listing a mounted path:
  mssparkutils.fs.ls(f"file://{mssparkutils.fs.getMountPath('/test')}/")
  ```
  In practice, **shortcuts are preferred over mounts** for real projects: a mount becomes unusable the moment the Spark session that created it stops, whereas a shortcut is a persistent OneLake object.
- **`fastcp`** — a high-performance file copy utility (built on AzCopy under the hood), useful for copying large volumes of data faster than a standard copy.
- **Credential utilities** — obtaining tokens/keys for Fabric resources, including `mssparkutils.credentials.getSecret(akvName, secretName)` for pulling secrets out of Azure Key Vault (see below).
- **`notebook.run` / `notebook.runMultiple`** — running one notebook from another (`run`), or running several concurrently with dependency ordering defined via JSON (`runMultiple`), which also lets Spark compute be shared more efficiently across the batch and gives a snapshot view of each notebook's run history.

### Ingesting data from ADLS into a Lakehouse: authentication options

1. The current user's own Microsoft Entra identity (simplest, but not suitable for unattended/scheduled jobs).
2. A service principal (an app registration) — not something you can authenticate with directly and interactively inside a notebook cell; it needs to be wired in via Spark configuration.
3. A service principal, with its secret pulled securely from Azure Key Vault at runtime, rather than hardcoded (the recommended pattern).

In every case, the identity actually reading the data needs **Storage Blob Data Contributor** on the ADLS account.

**Service principal, hardcoded (fine for learning, not for production)**:

1. **Entra ID** → **App registrations** → **New registration**, single tenant, register this in whichever tenant actually hosts the storage account and Fabric workspace.
2. From the app's Overview page, note the **Application (client) ID** and the **Directory (tenant) ID**. Generate a secret under **Certificates & secrets** → **New client secret**, and copy the secret's *value* immediately, it's only ever shown once.
3. Grant that app **Storage Blob Data Contributor** on the ADLS account.
4. In the notebook:
   ```python
   spark.conf.set(f"fs.azure.account.auth.type.{storageAccount}.dfs.core.windows.net", "OAuth")
   spark.conf.set(f"fs.azure.account.oauth.provider.type.{storageAccount}.dfs.core.windows.net",
                  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
   spark.conf.set(f"fs.azure.account.oauth2.client.id.{storageAccount}.dfs.core.windows.net", Appid)
   spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storageAccount}.dfs.core.windows.net", secretKey)
   spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storageAccount}.dfs.core.windows.net",
                  f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
   ```

**Service principal via Key Vault (recommended)**:

1. Create a Key Vault (**Resource group** → **Create** → **Key Vault**, Standard tier, Azure RBAC permission model).
2. Grant the Fabric user (or the identity running the notebook) the **Key Vault Secrets Officer** role on the vault via **Access control (IAM)**, otherwise you'll get an authorisation error trying to read or write secrets.
3. Store three secrets in the vault: the Application (client) ID, the Tenant ID, and the client secret value, each under its own secret *name*.
4. Retrieve them at runtime (arguments are positional, not keyword):
   ```python
   vault_url = "https://<vault-name>.vault.azure.net/"

   kv_app_id    = mssparkutils.credentials.getSecret(vault_url, "<secret-name-for-app-id>")
   kv_tenant_id = mssparkutils.credentials.getSecret(vault_url, "<secret-name-for-tenant-id>")
   kv_key       = mssparkutils.credentials.getSecret(vault_url, "<secret-name-for-client-secret>")
   ```
   > Fabric automatically redacts anything returned by `getSecret` from notebook output, so don't rely on `print()` to sanity-check a value, check it directly in the Key Vault (or via the Azure CLI) instead.

### Calling a Fabric notebook from a Fabric pipeline

1. Create the connection first: **Workspace settings** → **Manage connections and gateways** → **New** → **Cloud**, connection type **ADLS**, server `https://<storage-account>.dfs.core.windows.net/`, full path `<container>/<folder>`, authentication method **Service principal** (tenant ID, client ID, client secret).
2. In a pipeline, add a **Notebook** activity, choose the workspace and the notebook to run.

### Managed vs external tables

| | Managed table | External table |
|---|---|---|
| Data + metadata | Both handled by the Fabric engine | Metadata handled by Fabric, data stored elsewhere |
| Storage location | Lakehouse's `Tables` folder | Wherever the data actually lives (e.g. an ADLS path) |
| Dropping the table | Removes both data and metadata | Removes only the table definition, the underlying files are untouched |

Creating a managed table, any of:
```python
df.write.format("delta").saveAsTable("nyc_taxi")
```
```sql
CREATE TABLE nyc_taxi (vendorID INT, fare_amount DOUBLE) USING DELTA;
```
or via the Delta `TableBuilder` API.

Creating an external table points at an existing location instead of letting Fabric manage storage:
```sql
CREATE TABLE nyc_taxi_external
USING DELTA
LOCATION 'abfss://container@storageaccount.dfs.core.windows.net/path/to/delta-table';
```

### Data Wrangler

A visual, notebook-integrated tool for exploratory data analysis and cleaning on pandas or Spark DataFrames, launched from the notebook's **Home** ribbon or directly from a displayed DataFrame's cell output. It shows summary statistics and per-column visualisations, supports 20-plus point-and-click cleaning operations (drop duplicates, fill or drop missing values, split/strip text, one-hot encode, group and aggregate, and more), and every operation generates the equivalent pandas/PySpark code, which can be copied back into the notebook or exported as a reusable function. The original DataFrame is left untouched until you explicitly apply the generated code.

### Environments in notebooks

An **Environment** item lets you pin custom libraries, runtime configuration and session-level Spark properties (executor memory/cores) so a notebook's compute behaviour is reproducible rather than ad hoc. Created under **Workspace** → **Environment**, choosing a default or custom Spark pool, then adding Spark properties before publishing.

### V-Order optimisation

A write-time optimisation applied to Parquet files (on by default) that sorts data, distributes row groups, and compresses more aggressively for faster reads under Fabric's compute engines, while remaining 100% standard-compliant, open-source Parquet.

### Spark job definitions

A way to submit batch or streaming Spark jobs (either an uploaded local file or a path on ADLS Gen2) rather than running everything interactively in a notebook. At least one Lakehouse must be attached to serve as the default file system context, and job definitions can be scheduled just like a pipeline.

### Data Mesh and Domains

**Data Mesh** is an architectural pattern: instead of a single central data team owning everything, data is organised and governed by business-unit/domain, with each domain owning its own data products. **Domains** are Fabric's concrete enabler for this: a way to group workspaces by business area so consumers can filter and discover content by domain, while keeping data well-governed and easy to find.

To use Domains, you need to be a **Fabric admin** (a directory role, distinct from being an Azure subscription Owner, see [§13](#13-troubleshooting-log-and-lessons-learned)):

1. **Admin portal** → **Domain management settings** → enable *"Allow tenant and domain admins to override workspace assignments"* for the organisation.
2. **Domains** tab → **Create new domain**, name it, assign a domain admin.
3. Assign one or more workspaces to the new domain.

---

## 8. Migration to Microsoft Fabric

### Migrating Synapse notebooks

**Option 1 — manual export/import** (fine for a handful of notebooks): in Synapse Studio, **Notebooks** → **Export** → `.ipynb`, then in Fabric, workspace → **New item** → **Import notebook** → upload.

**Option 2 — scripted migration via API** (for many notebooks at once):

Prerequisites: a Fabric workspace and Lakehouse, a Synapse workspace, a service principal with access granted on the Synapse workspace (**Synapse** → **Manage** → **Access control** → add the app as **Synapse Administrator**).

```python
client_id = ""
tenant_id = ""
client_secret = ""
synapse_workspace_name = ""

workspace_id = ""   # target Fabric workspace GUID
workspace_guid = mssparkutils.runtime.context["currentWorkspaceId"]  # current workspace
lakehouse_id = "LH_Fabric"
export_folder_name = f"export/{synapse_workspace_name}"
prefix = "mig"
output_folder = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}.Lakehouse/Files/{export_folder_name}"

sc.addPyFile("https://raw.githubusercontent.com/microsoft/fabric-migration/main/data-engineering/utils/utils.py")
from utils import *

# export every notebook from Synapse
utils.export_notebooks(client_id, tenant_id, client_secret, synapse_workspace_name, output_folder)

# import them all into Fabric
utils.import_notebooks(f"/lakehouse/default/Files/{export_folder_name}", workspace_guid, prefix)
```

### Migrating Synapse pipelines

1. Grant the service principal **Synapse Administrator** access on the Synapse workspace (as above).
2. In Fabric: **Workspace settings** → **Manage connections and gateways** → **New** → **Cloud**, connection type **Azure Synapse workspace**, paste the Synapse workspace name, authentication method **OAuth 2.0**.
3. In a Fabric pipeline, add an **Invoke Pipeline** activity, type **Synapse**, pick the connection created above, and select the pipeline to invoke.

### Migrating ADLS data into OneLake

Two broad approaches:

- **Shortcuts** (ADLS stays the system of record, Fabric just references it, see [§6](#6-onelake-and-shortcuts)).
- **A genuine copy into OneLake**, via `mssparkutils.fs.fastcp`, AzCopy, a Data Factory/Synapse/Fabric pipeline Copy activity, or **Azure Storage Explorer**.

**Using Azure Storage Explorer**: install it, sign in with the Fabric/Azure account, locate the ADLS container. In Fabric, open the Lakehouse's Files **Properties** and copy its URL. Back in Storage Explorer, **Attach to a resource** → complete the OAuth sign-in → **ADLS Gen2 container or directory**, give it a display name, paste the copied Lakehouse URL (with the trailing `files` segment removed), then **Connect**. OneLake now appears as a storage target in Explorer, and files can simply be copied and pasted across from the ADLS container into it.

---

## 9. Fabric Capacity Metrics app

Installed from the Fabric app source, this app monitors and manages how much of a capacity's purchased compute is actually being consumed. Every capacity has a fixed number of **Capacity Units (CUs)**, the measure of available compute-seconds.

**Setup**: install the app, then **Fabric** → **Settings** → **Admin portal** → **Fabric capacity** → **Actions** → copy the capacity ID, paste it into the app, and authenticate.

### Compute page

- **Multi-metric ribbon chart** — an hourly view of usage, drillable down to a specific date or hour, showing CU (processing time in seconds), duration, number of operations, and number of distinct users active in a given window.
- **Capacity Unit % over time** — a line/stacked chart, viewable as linear or logarithmic.
- **Items (matrix)** — every item attached to the capacity, with its CU consumption, duration, user count, billing type, and rejected-request count.

### Capacity units consumption

- **Utilisation** — capacity-unit-seconds actually used.
- **Throttling** — congestion that occurs once a tenant's capacity consumes more than it purchased; too much throttling degrades the end-user experience. Fabric manages this through **smoothing**.
- **Smoothing** — balances usage between over-utilised (peak) and under-utilised (idle) periods rather than hard-capping the moment a limit is hit.

### Throttling stages (overage protection policy)

| Stage | Trigger | Effect |
|---|---|---|
| **Overage (grace period)** | Usage exceeds 100% of purchased capacity, within a 10-minute window | No pause, no extra charge, no degraded performance yet — Fabric calculates the over-used seconds and schedules them to burn down as background usage over the next 24 hours. |
| **Interactive delay** | Overage continues beyond the 10-minute window | Interactive queries (notebook cells, SQL endpoint queries) still run, but take roughly an extra 25 seconds to respond. |
| **Interactive rejection** | Overage continues beyond roughly 60 minutes | Interactive requests are rejected outright with a "you have hit your capacity limit" style error. |
| **Background rejection** | Overage continues beyond roughly 24 hours | Everything is rejected, including background jobs, since the allocated resource has been over-utilised for too long. |

### Overages tab

Two views: **Overage (carryforward)**, showing the percentage of carryforward added/burned down in each 30-second window plus a cumulative carryforward line; and **Overage (billed)**, populated only when capacity overage billing is enabled, showing the rolling 24-hour billed overage against the overage billing limit.

### System events table

Lists capacity state changes over the last 14 days: **Time**, **State** (Active, Overloaded, Suspended, Deleted), and **State change reason** (e.g. `Created`, `ManuallyPaused`, `ManuallyResumed`, `InteractiveDelay`, `InteractiveRejected`, `SurgeProtectionActive`).

---

## 10. Fabric Synapse Data Warehouse

### Loading data with `COPY INTO`

```sql
CREATE TABLE <table_name> (<column_definitions>);

COPY INTO <workspace>.<schema>.<table_name> (<column_list>)
FROM 'https://<storage-account>.dfs.core.windows.net/<container>/<file_name>.<file_type>'
WITH (FILE_TYPE = '<FILE_TYPE>');
```

### Loading data via a pipeline

Ingesting from an on-premises SQL Server into a Warehouse follows the same shape as the Lakehouse version in [§5](#5-fabric-data-factory): an on-premises gateway, a connection to the SQL Server, a pipeline with a staging area, and a Copy activity targeting the Warehouse instead of a Lakehouse table.

### Loading data via Dataflow Gen2

New item → **Dataflow Gen2** → **Get data** → **SQL Server database** → choose the on-premises gateway connection → server and database → select the table → transform with Power Query → write the transformed output to a Warehouse table that already has a matching schema.

### Lakehouse vs Warehouse: when to choose which

Both store data as Delta Parquet underneath, the difference is in what sits on top:

| | Lakehouse | Warehouse |
|---|---|---|
| Best for | Structured, semi-structured or unstructured data | Structured data only |
| Primary engine | Spark (PySpark, Scala, Spark SQL, Spark R) | T-SQL (including stored procedures) |
| Writes transformed data as | Lakehouse tables or files | Warehouse tables |

---

## 11. Fabric access control and permissions

Permissions in Fabric are layered: **tenant** → **capacity** → **workspace** → **item**.

- **Tenant-level** settings are managed by Fabric/tenant admins.
- **Capacity-level** settings are managed by capacity admins; an organisation can own multiple capacities, each assignable to different workspaces.

### Workspace roles

| Role | Can add admins? | Can add members? | Can write data / create items? | Can read data? |
|---|---|---|---|---|
| Admin | Yes | Yes | Yes | Yes |
| Member | No | Yes | Yes | Yes |
| Contributor | No | No | Yes | Yes |
| Viewer | No | No | No | Yes |

| Workspace action | Admin | Member | Contributor | Viewer |
|---|---|---|---|---|
| Update/delete workspace | Yes | No | No | No |
| Add/remove users | Yes | No | No | No |
| Add/remove members | Yes | Yes | No | No |
| Allow others to re-share items | Yes | Yes | No | No |
| Schedule refresh (on-premises gateway) | Yes | Yes | Yes | No |
| Modify gateway connection strings | Yes | Yes | Yes | No |

**Data pipeline permissions**

| Action | Admin | Member | Contributor | Viewer |
|---|---|---|---|---|
| View content and output | Yes | Yes | Yes | Yes |
| Execute/cancel pipeline | Yes | Yes | Yes | No |
| Schedule refreshes | Yes | Yes | Yes | No |
| Create/modify/delete pipelines | Yes | Yes | Yes | No |

**Notebook / Spark job permissions**

| Action | Admin | Member | Contributor | Viewer |
|---|---|---|---|---|
| View content and outputs | Yes | Yes | Yes | Yes |
| Execute/cancel | Yes | Yes | Yes | No |
| Create/modify/delete | Yes | Yes | Yes | No |

**Data Warehouse permissions**

| Action | Admin | Member | Contributor | Viewer |
|---|---|---|---|---|
| Connect to SQL analytics endpoint | Yes | Yes | Yes | Yes |
| Read data/shortcuts through endpoint | Yes | Yes | Yes | Yes |
| Read through OneLake API | Yes | Yes | Yes | No |
| Read through Spark shortcut | Yes | Yes | Yes | No |
| Create/modify tables, views, etc. | Yes | Yes | Yes | No |

**Lakehouse permissions**

| Action | Admin | Member | Contributor | Viewer |
|---|---|---|---|---|
| Connect to SQL analytics endpoint | Yes | Yes | Yes | Yes |
| Read data/shortcuts through SQL endpoint | Yes | Yes | Yes | Yes |
| Read through Lakehouse explorer | Yes | Yes | Yes | No |
| Read through OneLake API | Yes | Yes | Yes | No |
| Read through Spark | Yes | Yes | Yes | No |
| Create/modify/delete tables/files | Yes | Yes | Yes | No |

### Tested: cross-workspace shortcut access

A three-user simulation, useful for understanding how shortcut visibility actually propagates:

1. **User A** owns Workspace 1/Lakehouse 1 (containing an `HR` table) and grants **User B** Contributor access to Workspace 1.
2. User B can now see Workspace 1 and successfully creates a **shortcut** in their own Workspace 2/Lakehouse 2 pointing at the `HR` table, because they hold Contributor rights on the source.
3. User B then grants **User C** Contributor access to Workspace 2.
4. User C can see the shortcut listed in Workspace 2's Lakehouse, but accessing it through the **Lakehouse explorer** is rejected, because User C has no rights on the *original* source (Workspace 1), only on Workspace 2 where the shortcut merely lives.
5. User C *can*, however, query the same shortcut successfully through the **SQL analytics endpoint** — endpoint access follows a different, more permissive path.
6. To fully unblock User C via the Lakehouse explorer too, User A must grant User C Contributor access directly on Workspace 1, the original source, not just on the intermediate workspace holding the shortcut.

**ADLS shortcuts behave differently**: a user needs **Storage Blob Data Contributor** on the ADLS account itself to access an ADLS shortcut through Spark or the OneLake API, but a *second* user with only Contributor access on the Fabric workspace (and no ADLS role at all) can still read the same shortcutted table through the notebook and the SQL analytics endpoint, because those paths are authorised at the workspace level rather than requiring a direct ADLS role.

### Item-level sharing

Individual items (Lakehouse, Warehouse, Notebook) can be shared directly via their **···** menu, useful either to collaborate with people who have no workspace role at all, or to grant *additional* item-level permissions to people who already have one. Some item types (pipelines, Dataflow Gen2, Eventstream) can't be shared at the item level at all.

**Warehouse sharing**

| Permission granted | Effect |
|---|---|
| *(none selected)* | Recipient gets a bare "read" permission: can connect to the SQL analytics endpoint but cannot query any table/view or execute any function/procedure. |
| **ReadData** | Recipient can read all objects in the warehouse via T-SQL, equivalent to SQL Server's `db_datareader` role. Can be further restricted with `GRANT`/`REVOKE`/`DENY`. |
| **ReadAll** | Recipient can read the warehouse's underlying OneLake files directly, via Spark, a pipeline/shortcut, or any other app reading OneLake data. |
| **Build** | Recipient can build a report on the warehouse's default semantic model. |

**Lakehouse sharing**

| Permission granted | Effect |
|---|---|
| *(none selected)* | Bare "read": SQL analytics endpoint connection only, no querying, nothing visible in Lakehouse explorer. |
| **Read all SQL endpoint data** | Read via the SQL analytics endpoint; cannot create or modify tables without an additional grant from an admin. |
| **Read all Apache Spark (ReadAll)** | Read via the OneLake API, Spark, and the Lakehouse explorer. |
| **Build** | Build a report on the default semantic model. |

**Notebook sharing**

| Permission granted | Effect |
|---|---|
| *(none selected)* | Recipient can view cells but not execute them. |
| **Share** | Recipient can re-share the notebook with others. |
| **Edit** | Recipient can edit all cells (write access). |
| **Run** | Recipient can execute all cells. |

### Manage OneLake data access (row/folder-level, preview)

By default, everyone with access to a Lakehouse gets a **DefaultReader** role that can read every folder. To restrict this: **Lakehouse** → **Manage OneLake data access (preview)** → turn on → remove the DefaultReader role → create a **new role**, selecting exactly which folders (under Tables and/or Files) it can read → assign it to specific users with a chosen permission level (read, write, reshare, execute, read-all).

A user with *only* this folder-level role and nothing at the workspace level won't be able to open the Lakehouse at all, it needs pairing with a workspace-level grant (**Lakehouse** → **Manage permissions** → add the user → grant **Read all SQL endpoint data** and **Read all Apache Spark**) before the folder-level restriction actually becomes usable.

### Row-level security (RLS)

Available in both the Warehouse and the Lakehouse's SQL analytics endpoint. The pattern: a security predicate defined as an inline table-valued function, enforced by a security policy, checked by the database on every access attempt.

```sql
CREATE FUNCTION dbo.tvf_securitycheck(@sales_rep AS NVARCHAR(90))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS tvf_securitycheck_result
WHERE @sales_rep = USER_NAME() OR USER_NAME() = 'user_a';

CREATE SECURITY POLICY sales_filter
ADD FILTER PREDICATE dbo.tvf_securitycheck(sales_rep)
ON dbo.orders
WITH (STATE = ON);
```

Result: `user_a` sees every row, `user_b` and `user_c` see only rows where `sales_rep` matches their own username.

### Dynamic data masking

Hides sensitive column values from users without the right permission, e.g. showing `XXXXXX89` instead of a full card number.

```sql
-- full masking (uses the column's data type default, e.g. 'xxxx' for strings)
ALTER TABLE dbo.orders ALTER COLUMN sales_rep ADD MASKED WITH (FUNCTION = 'default()');

-- email-shaped masking
ALTER TABLE dbo.orders ALTER COLUMN sales_rep ADD MASKED WITH (FUNCTION = 'email()');

-- partial masking, e.g. keep first digit and a fixed suffix pattern
ALTER TABLE dbo.orders ALTER COLUMN cc_number ADD MASKED WITH (FUNCTION = 'partial(1,"XXXX-XX-",0)');
```

> If a column already participates in a `SCHEMABINDING`-protected security policy (as `sales_rep` does above), masking it will fail until the dependent security policy is dropped first, then recreated afterwards.

**`default()` masking by data type**:

| Column type | Data types | Masked value |
|---|---|---|
| Strings | `char`, `nchar`, `varchar`, `nvarchar`, `text`, `ntext` | `xxxx` |
| Numeric | `bigint`, `bit`, `decimal`, `int`, `money`, `numeric`, `smallint`, `smallmoney`, `tinyint`, `float`, `real` | `0` |
| Date/DateTime | `date`, `datetime2`, `datetime`, `datetimeoffset`, `smalldatetime`, `time` | `1900-01-01` |
| Binary | `binary`, `varbinary`, `image` | ASCII value `0` |

### Column-level security

```sql
GRANT SELECT ([sale_id], [sales_rep], [product_name], [sales_date])
ON dbo.orders
TO "user_b@contoso.com";
```
This grants visibility of only the listed columns, `sales_amount` and `cc_number` stay hidden from `user_b`.

### OneLake security (updated model)

A newer, more granular alternative to workspace-level roles, independent of whatever workspace role a user holds:

1. **Lakehouse** → **Manage OneLake security** → remove the default **DefaultReader** role.
2. Create a role, select exactly which tables/files it covers, and choose **Read** or **Read/Write**.
3. Assign the role to a user who may have *no* workspace or Lakehouse access at all, they only get row/folder-level visibility from this role.
4. That alone isn't enough for the Lakehouse to load for them, pair it with a workspace-level grant (**Manage permissions** → add the user with no extra permissions needed beyond direct access) so the Lakehouse actually opens, then the OneLake security role governs exactly what they see inside it.

Best practice: create one role per schema/business area and add the relevant users to it, rather than managing access user-by-user.

**Row-level security via OneLake security**: create a role, select the data, and define the permission as a row filter, e.g. `SELECT * FROM hr.employees WHERE department = 'sales'`.

**Column-level security via OneLake security**: create a role, select the data, and explicitly choose which columns are visible versus hidden for members of that role.

---

## 12. Microsoft Fabric Power BI

*This module is part of the project's agreed scope but hasn't been worked through hands-on yet.* Planned coverage: building a semantic model directly over the Lakehouse/Warehouse via **Direct Lake** mode (querying OneLake's Delta tables without a separate import/refresh cycle), report authoring, and workspace-level Power BI permissions layered on top of the access model in [§11](#11-fabric-access-control-and-permissions). This section will be filled in once that work is done.

---

## 13. Troubleshooting log and lessons learned

Real problems hit while building this project, and what actually fixed them, kept here because the debugging process itself is as instructive as the finished pipeline.

**`Cannot reference a Notebook that attaching to a different default lakehouse`** — thrown by `mssparkutils.notebook.run()` when the calling and called notebooks have different Lakehouses attached as their default. Fix: either attach the same default Lakehouse to both, or pass `{"useRootDefaultLakehouse": True}` as the third argument to let the child notebook use its own attached Lakehouse.

**`PATH_NOT_FOUND` reading a wildcard path** — usually one of: a casing mismatch (ADLS paths are case-sensitive), the target files sitting one or more folders deeper than the wildcard reaches (a single-level `*.csv` glob doesn't recurse into subfolders), or the files genuinely not having landed yet.

**`AADSTS700016: Application ... was not found in the directory`** — nearly always one of: a literal placeholder string (like the word `Appid`) left in the config instead of the real GUID, the Application (client) ID confused with the Object ID on the app registration's Overview page, or the tenant ID in `oauth2/client.endpoint` genuinely pointing at a different tenant than the one the app was registered in. Confirmed useful diagnostic: compare the tenant ID quoted in the error against the app registration's own Directory (tenant) ID, character for character.

**Granting a data-plane role doesn't fix an authentication error** — Azure RBAC (e.g. Storage Blob Data Contributor) is only evaluated *after* a token has already been issued. If authentication itself is failing (AADSTS-prefixed errors), no role assignment will change the outcome; the fix has to be on the Entra ID/token side.

**A value stored in Key Vault can itself be wrong** — `getSecret()` will happily return exactly what's stored, if a secret named e.g. `TenantId` has its *value* also set to the literal text `TenantId` (a slip made when creating it), the retrieval "succeeds" while returning garbage. Fabric redacts anything returned by `getSecret` from notebook output, so verify the actual stored value directly in the Key Vault (or via `az keyvault secret show`), not by printing it from a notebook.

**Owner access ≠ tenant/Fabric admin** — Azure RBAC (Owner, Contributor, on a subscription/resource) and Microsoft Entra directory roles (Global Administrator, Fabric Administrator) are entirely separate systems that don't overlap by default. Features gated on a directory role (like creating a **Domain**, see [§7](#7-fabric-synapse-data-engineering)) stay invisible in the admin portal no matter how much Owner access is held, until the actual directory role is checked/assigned under **Entra ID → Roles and administrators**. Whoever originally signs up for a tenant is automatically its Global Administrator, so it's worth checking there first before assuming the role needs creating from scratch.

**Parquet schema mismatch across files (`Expected: int, Found: BINARY`)** — happens when reading multiple Parquet files together (a folder or wildcard) where the same column was physically encoded differently across files. The vectorized Parquet reader won't silently coerce between them. Quick fix: `spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")`; a more deliberate fix is to read with an explicit schema that pins the column to one consistent type.

**Reviews vs Calendar as "the incremental fact table"** — worth remembering going forward: Reviews carries genuine, non-overlapping historical dates, so it's the table that actually behaves like an incremental batch load. Calendar is a point-in-time snapshot (next ~365 days from the scrape date), so slicing it into monthly folders is a simulated historical pattern, not a real one, worth being explicit about that distinction anywhere this project gets described.

**Reviews (or Calendar) as a proxy for "bookings"** — neither is ground truth. Airbnb doesn't publish real booking data. Inside Airbnb's own published methodology assumes only ~50% of guests who book leave a review, and even that correction is described as conservative. Any "estimated bookings" metric built from this data should be labelled as an estimate with its assumption stated, not presented as an actual count.

---

## 14. Certification readiness: DP-700 (Fabric Data Engineer Associate)

This project was built partly as a hands-on companion to Microsoft's **Fabric Data Engineer Associate** certification, exam **DP-700: Implementing Data Engineering Solutions Using Microsoft Fabric** (100 minutes, three domains weighted roughly equally at 30–35% each). What follows maps the guide above against the exam's current published skills outline, so it's clear what this project already demonstrates and what's still genuinely missing before treating it as exam preparation.

> Exam blueprints get refreshed periodically. This mapping reflects the outline published at [learn.microsoft.com/credentials/certifications/resources/study-guides/dp-700](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700) as of writing, worth a quick re-check against the live guide before treating this as final.

### 14.1 Domain 1 — Implement and manage an analytics solution (30–35%)

| Skill | Status | Notes |
|---|---|---|
| Configure Spark workspace settings | Partially covered | Spark pools, sessions and Environments are covered (§7); broader workspace-level Spark settings (e.g. pool assignment policy, high-concurrency defaults) aren't written up as a dedicated config walkthrough. |
| Configure domain workspace settings | Covered | §7, Domains and Data Mesh. |
| Configure OneLake workspace settings | Not covered | Workspace-level OneLake settings (e.g. workspace disaster-recovery, OneLake sharing defaults) haven't been touched at all. |
| Configure Apache Airflow workspace settings | Not covered | Fabric's Apache Airflow Job item (DAG-based orchestration) doesn't appear anywhere in this project yet. |
| Configure version control | Not covered | No Git integration (Azure DevOps/GitHub) has been set up on the workspace. |
| Implement database projects | Not covered | SQL database projects (schema-as-code for the Warehouse) haven't been used. |
| Create and configure deployment pipelines | Not covered | No Dev → Test → Prod Fabric deployment pipeline has been built. |
| Workspace/item/row/column/object/folder-file access controls | Covered | §11, in depth (workspace roles, item sharing, RLS, column security, OneLake security). |
| Dynamic data masking | Covered | §11. |
| Apply sensitivity labels | Not covered | Not used anywhere in the project. |
| Endorse items | Not covered | Item promotion/certification hasn't been demonstrated. |
| Fabric audit logs | Not covered | The admin portal's audit log hasn't been explored. |
| OneLake security | Covered | §11. |
| Choose Dataflow Gen2 vs pipeline vs notebook | Partially covered | Each is used individually (§5, §7), but the decision criteria for choosing between them isn't written up as its own topic. |
| Schedules and event-based triggers | Not covered | Pipelines have been run manually; no schedule or event-based trigger (e.g. on file arrival) has been configured. |
| Orchestration patterns, parameters, dynamic expressions | Partially covered | Dynamic content parameters appear in the ForEach/Copy pattern (§5) and notebook chaining (§7), but not as a dedicated parameterisation/expressions topic. |

### 14.2 Domain 2 — Ingest and transform data (30–35%)

| Skill | Status | Notes |
|---|---|---|
| Full and incremental data loads | Partially covered | Reviews vs Calendar (genuine incremental vs point-in-time snapshot) is discussed conceptually (§1, §13), but no formal merge/upsert or watermarking pattern has been implemented in code. |
| Prepare data for a dimensional model | Partially covered | The dataset is implicitly split into dimension-shaped (Listings, Neighbourhoods) and fact-shaped (Calendar, Reviews) data, but no formal star schema (fact/dim tables, surrogate keys, a `dim_date`) has been built or documented. |
| Loading pattern for streaming data | Not covered | No streaming source exists in this project yet. |
| Choose an appropriate data store | Covered | Lakehouse vs Warehouse decision criteria, §10. |
| Dataflow Gen2 vs notebooks vs KQL vs T-SQL | Partially covered | Dataflow Gen2, notebooks and T-SQL are all used; **KQL isn't used anywhere**. |
| OneLake shortcuts | Covered | §6, in depth. |
| Implement mirroring | Not covered | Fabric Mirroring (e.g. of an Azure SQL database) hasn't been demonstrated. |
| Ingest data via pipelines | Covered | §5. |
| Transform via PySpark, SQL, KQL | Partially covered | PySpark and SQL are covered throughout; KQL is entirely absent. |
| Denormalize / group and aggregate data | Not explicitly covered | Implied as something Data Wrangler or Spark could do, but no worked transformation example exists in the guide. |
| Handle duplicate, missing, late-arriving data | Not covered | No explicit deduplication, null-handling or late-arrival pattern has been implemented. |
| Streaming ingestion (Eventstream, Spark structured streaming, KQL, windowing) | Not covered | This entire sub-domain, Real-Time Intelligence, is untouched by the project so far. |

### 14.3 Domain 3 — Monitor and optimize an analytics solution (30–35%)

| Skill | Status | Notes |
|---|---|---|
| Monitor data ingestion / transformation | Partially covered | The Capacity Metrics app (§9) covers tenant-wide compute monitoring, but Fabric's item-level **Monitoring hub** (individual pipeline/notebook/dataflow run history) isn't covered. |
| Monitor semantic model refresh | Not covered | Depends on the Power BI module (§12), which is itself still pending. |
| Configure alerts | Not covered | Data Activator/Fabric alerting hasn't been set up. |
| Resolve pipeline/notebook/OneLake shortcut errors | Covered | §13's troubleshooting log covers these in real depth. |
| Resolve Dataflow Gen2 / Eventhouse / Eventstream / T-SQL errors | Not covered | No hands-on errors from these specific items have been documented yet (Eventhouse/Eventstream tie back to the Real-Time Intelligence gap above). |
| Optimize a Lakehouse table | Partially covered | V-Order is described (§7); explicit `OPTIMIZE`/`VACUUM` maintenance hasn't been demonstrated. |
| Optimize a pipeline | Not covered | No pipeline-specific tuning (parallelism, staging, batch size) has been documented. |
| Optimize a data warehouse | Not covered | No statistics/indexing/query-plan tuning has been demonstrated on the Warehouse. |
| Optimize Eventstreams/Eventhouses | Not covered | Ties back to the Real-Time Intelligence gap. |
| Optimize Spark performance | Partially covered | Spark pool sizing and session concurrency are covered (§7); deeper tuning (partitioning, caching, broadcast joins) isn't. |
| Optimize query performance | Not covered | No query-tuning exercise has been written up. |

### 14.4 Priority gap list

Roughly in order of how much exam weight they close, and how much new ground each one covers:

1. **Real-Time Intelligence** — the single biggest gap, an entire sub-domain untouched. Since Airbnb data isn't naturally streaming, a practical way in is to replay `reviews.csv` rows as simulated events (one row every few seconds) through an **Eventstream** into a **KQL database**, then write a few KQL queries and a windowing function against it. This alone would close most of the streaming-ingestion, Eventhouse/Eventstream error-handling, and Eventstream/Eventhouse optimisation gaps at once.
2. **Lifecycle management** — connect the workspace to **Git** (Azure DevOps or GitHub), and build a **deployment pipeline** promoting items through Dev → Test → Prod. High exam weight, and directly reusable for the project's own repo hygiene.
3. **Formal dimensional model** — write up a proper star schema for the Airbnb data (`fact_calendar`, `fact_reviews`, `dim_listing`, `dim_neighbourhood`, `dim_date`), rather than leaving the dimension/fact split implicit. Closes the "prepare data for a dimensional model" gap and strengthens the project's own documentation regardless of the exam.
4. **Data quality patterns** — implement (and document) explicit handling for duplicate rows, missing values, and late-arriving Reviews batches, ideally in a Dataflow Gen2 or notebook step that's referenced back from §7.
5. **Governance extras** — apply a sensitivity label to at least one item, endorse (promote or certify) a key Lakehouse/Warehouse, and review the tenant's audit logs in the admin portal. Small effort, closes three separate bullets.
6. **Monitoring hub and alerts** — review pipeline/notebook run history in Fabric's Monitoring hub, and configure a Data Activator alert (e.g. notify on pipeline failure). Directly addresses the "monitor Fabric items" and "configure alerts" bullets.
7. **Warehouse and pipeline performance tuning** — document a concrete optimisation pass on the Warehouse (statistics, query plans) and on a pipeline (parallelism/staging), rather than relying on Spark/V-Order coverage alone.
8. **Mirroring** — a narrower feature; worth a short hands-on note (e.g. mirroring an Azure SQL database into Fabric) if time allows, lower priority than the items above.
9. **Apache Airflow jobs in Fabric** — a newer orchestration option alongside pipelines and Dataflow Gen2; worth at least a comparison note even without deep hands-on time.

Sources: [Microsoft Certified: Fabric Data Engineer Associate](https://learn.microsoft.com/en-us/credentials/certifications/fabric-data-engineer-associate/), [DP-700 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-700).

---

## 15. Glossary

| Term | Meaning |
|---|---|
| **OneLake** | Fabric's single, tenant-wide storage layer underlying every workload — "OneDrive for data." |
| **Lakehouse** | A Fabric item combining Delta Lake storage (Tables + Files) with an auto-generated SQL analytics endpoint. |
| **Delta table** | A folder of Parquet files plus a `_delta_log`, giving ACID transactions, schema enforcement and time travel. |
| **Shortcut** | A OneLake pointer to data stored elsewhere, read virtually without copying it. |
| **Capacity** | The billable Azure resource (an F-SKU) that Fabric workspaces attach to for compute. |
| **CU (Capacity Unit)** | The unit of compute-seconds a capacity provides and that workloads consume. |
| **Domain** | A Fabric governance grouping of workspaces by business area, enabling a data-mesh style structure. |
| **Dataflow Gen2** | Fabric's Power Query-based data preparation tool, writing output to a Lakehouse/Warehouse destination. |
| **Managed table** | A Lakehouse table whose data and metadata are both controlled by Fabric; dropping it deletes the data. |
| **External table** | A table whose metadata lives in Fabric but whose data is stored (and controlled) elsewhere. |
| **RLS** | Row-level security — restricting which rows a user can see via a security predicate and policy. |
| **Direct Lake** (Power BI) | A Power BI storage mode that queries OneLake's Delta tables directly, without an import/refresh cycle. |

