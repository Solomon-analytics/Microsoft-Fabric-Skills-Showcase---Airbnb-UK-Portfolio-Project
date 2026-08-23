Microsoft Fabric Skills Showcase — Airbnb UK Portfolio Project
Objective

A hands-on Microsoft Fabric portfolio project, built around a single consistent dataset (Inside Airbnb data for London, Bristol and Manchester) so that every core Fabric capability has a real, working example rather than a disconnected demo. Not a narrow single-pipeline project — it's a structured tour of the Fabric platform, framed as applied engineering work rather than a tutorial, for use as a GitHub portfolio piece and CV talking point.

Full scope agreed with user (12 modules)
Setting up a Microsoft Fabric environment
Fabric fundamentals: evolution of data architectures, Delta Lake structures, enabling/accessing Fabric, licensing and costing
Lakehouse: workspaces, Fabric capacity, ingesting data to a Lakehouse, SQL analytics endpoint, OneLake file explorer
Fabric Data Factory: ways of loading data into a Lakehouse, Fabric Data Factory vs Azure Data Factory, gateway types, connecting to SQL Server, pipeline to ingest on-prem data to Lakehouse, Dataflow Gen2, Dataflow Gen2 vs ADF Dataflows
OneLake and shortcuts: creating shortcuts in Lakehouse Files, criteria for shortcuts in the Tables section, creating Delta files, shortcuts with Parquet format, Delta Lake to Lakehouse
Fabric Synapse Data Engineering
Synapse migration to Microsoft Fabric
Fabric Capacity Metrics app
Fabric Synapse Data Warehouse
Fabric access and permissions
Microsoft Fabric Power BI (reporting layer)
This is the full agreed scope — no further modules expected unless the user adds them
Dataset

Inside Airbnb, one snapshot per city (London, Bristol, Manchester), comprising:

Listings (static/dimension — property and host attributes)
Neighbourhoods (static/dimension — geographic reference data)
Calendar (fact — date-level price and availability; a point-in-time snapshot, not genuinely incremental — booked vs host-blocked nights are indistinguishable per Inside Airbnb's own documentation)
Reviews (fact — genuinely incremental, date-stamped; used for monthly year-month batches, e.g. 2021-01, 2021-02)


# Setting up a Microsoft Fabric environment

1. Create a resource group in Azure
2. setting up a Microsoft Fabric Resource in Azure
3. Create a Azure Data Lake Storage Account
4. Create Azure Synapse Analytics Service
5. Create new Spark Pool in Azure Synapse Analytics Service

# Create Azure Synapse Analytics Service in Azure
to create this, Resource group -

