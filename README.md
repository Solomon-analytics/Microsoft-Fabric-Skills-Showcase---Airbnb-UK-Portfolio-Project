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


# Microsoft Fabric Data Factory

-- Data Gateways
There are two types:
- 1. On-premise Data Gateway: provides quick and secure data transfer between on-premise data and several Microsoft cloud services, such as Power BI, PowerApps
  2. V-Net Data Gateway: A virtual network data gateway helps to connect from Microsoft cloud services to Azure data services within a virtual network.
 
  -- Installing Data Gateway
  In Microsoft Fabric homepage, clock on the download icon - Data Gateway - Download standard mode - download and install - sign in with your Microsoft fabric username - register a new gateway name - input recovery keys
  - to ensure it's running/ready, go to microsoft fabric - settings - manage connections and gateways - on-premises data gateways - status should show online
 
-- Creating connections to SQL Serve
connections is away of establishing authentication to data sources like linked services ion Azure Data Factory / Synap se Analytics

there are three connections types
on-premises
v-net
cloud

- Establishing connection between Fabric SQL Analytics Endpoint and SSMS
- In Fabric SQL Analytics Endpoint - Copy (Copy SQL connection string) - In SSMS - Connect (database engine) - Server name(input the copied string) - Authentciation(Microsoft entra password - Username(Microsoft fabric email and password)

- - Creating connection: in microsoft fabirc - manage connections and gateways - connections - create new connection - choose on-premises - gateaway cluster name(choose the created gateway) - connecion name(give it a name) - connection type(SQL Server) - server(copy SSMS server name and paste) -- database -- authentication method (basic) - input the username and password of SQL serve --
 
-- Pipeline to ingest onprem SQL data to lakehouse
On fabric portal - workspace - new item - data pipeline - lookuup activity - choose commectoon(onpremsql connection) - connection type(SQL server) - choose database - query( SELECT schema_name(t.schema_id) AS schema_name, t.name as table_name FROM sys.tables t ORDER BY table_name) - activity(foreach) - sequential(yes) - items (@activity('lookup1').output.value) - inside for each - copy activity - source(onpremsqlconnecion) - connection type(sql server) - select database - table - schema name(@item().schema_name - table_name(@item().table_name) -- these are parameters - input the parameter for schema_name and table_name - destination - LH_fabric(fabirc is the destination) - tables(@item().table_name--this is a dynamic content param) - action(append) - save and run


# Creating Dataflow Gen2
in workspace - net item - dataflow gen2 - read data from ADLS - On ADLS - getting access to ADLS storage account - Access control IAM - Add - add role assignment - role(sotorage blob data contributor) -  select and add member - review + assign - back in fabric portal - dataflow gen 2 - get data (more) - ADLS - URL(Storage account- file-properties-copy url) - paste - change blob to dfs - remove the file name - organizational account - privacy level(public) -combine or create - perform the transformation - write data to destination(add data destination(conect to lakehouse - choose existing lakehouse) - give it a name and save in the lakehouse

# Fabric Onelake
Loading file into Onelake using shortcut
Shortcut: typically operates at the Onelake level: are objects in Onelake that point to other storage location
Benefits:
when data in source is update - updates reflects in the target path
data is accessed virtually without copying


- prerequisites to create shortcuts
shortcuts can be created in lakehouse tables or files
what is required to create shortcut
1. source location is needed (Lakehouse, Warehouse, Azure datalake, Amazon s3, dataverse, GCP)
2. right authentication to the data source
3. Destination(Lakehouse or KQL Database)

Creating shortcut in files of lakehouse
source: ADLS - container - subfolder - files - ensure fabric user do have the storage blob data contributor role
Create new container in existing storage ADLS - create a directory - upload files in this directory - give the fabric user access to this storege - through accss control (IAM) - blob storage data contributor
fabric portal - lakehouse - on files- three dots - new shortcut - source(ADLS) - url(in adls, copy the url of any of the file, replace blob with adls, remove file name.itsformat at the end) - paste - organizaational accunt - click on ADLS cntainer and next - 

-- criteria to create shortcut in lakehouse table section
Adls(container - subfolder - fils(role: Storage blob data contributor) - the steps are the same as files shortcut - likely to see this prompt(Unable to identify these objects as tables or views. To keep these objects in the lakehouse, move them to Files.) - typically, the table section of lakehouse, data are stored i this section  as delta parquet format, anyother format like csv, excel parquet or json should be stored under the folder section - 

- right way to create a shortcut in table's section of the lakehouse
- basaically - in azure synapse - notebook - read adls container dataset in its format - then write back to adls container in delta format - then create a table lakehouse shortcut - by accessing the delta format in adls container- ensure the azure synapse account is granted the blod data storege contributor access in adls
  
-- Creating a shortcut with delta in a subfolder
in Adls create a new container in storage - in Synapse Analytics - start the spark pool - read file from ADLS - then write to ADLS subfolder created in delta format - in microsoft fabric - in tablle - create shortcut - copy the parquet url in ADLS, renamed blob to dfs, remove the file name - once again this returns an error(unable to identify these objects as tables. To keep these objects in the lakehouse, move them to files. Shortcutes cannot be moved directly, but you can recreatre them in file and then delete them here) - why this error? considering the file has processed from synapse analytics and its format us un delta? - not entirely sure, i think this is because we are also bring in the file/container, rather than just the delta file??

-- Creating shortcut with only parquet format?
create a new container in ADLS storage and store file as parquet(to do this, read csv data from adls using synapse analytics and write back to the new container/folder as parquet format) - in microsoft fabric - lakehouse - in the table section - new shortcut - copy the parquet file existing in the newly created container/folder, copy the url - replace blob with dfs - once again, this went to the unidentified state with the same error as above
In conclusion, best practice is to create a short for files in Delta Parquet in the table(managed) section of the lakehouse, whil;e files such as csv, parquet, json, excel - shortcuts shpuld be created for this in the files section of the lakehouse

-- Requirements to create shortcuts in table and files section
Shortcuts in table:
in the table folder, you can only create shortcuts at the top level. Shortcuts are not supported in other subdirectories of the tables folders - in other words, short must be created in the table section as a delta format not in a subfolder in the table as any format be it in delta format
Data to be in Delta/parquet frmat so that lakehouse automatically synchronise the metadata and recognises the folder as a table

Shortcuts in files:
if your shortcut location is in form of sub-directories, go with storing them in files.
if they are not in delta-parquet format, store them in files

- Shortcut updating scenario:
To update file that was created as shirtcut in lakehouse from ADLS?
First, we need to grant an updating access on Azure - resource group - AFLS storage - Access control (IAM) - check access - input name of user - storage blob data contributor(allows to read, write and delete access to azure storage bloc container and data) - this user currently have all access to modify and delete data in the storage account - in the case a user/account does not have this access, you grant the acess as the admin or request admin to grant the access?
In the lakehouse - New notebook - df = spark.sql("select * from LH_Fabric.shortcuttable") display(df))
writing an update statement :  spark.sql("UPDATE LH_FABRIC.shortcuttable SET column_name = "value" WHERE column_name = "value)) - this will run sunncessfullly because user has a contributor access of the ADLS storage, if user had only read access or anythother access apart from admin, owner or contributor, it returns an error.
to comfirm update: spark.sql("SELECT COUNT(*) FROM LH_FABRIC.shortcuttable WHERE column_name = "value")
finally, update made on a shortcut file in the lakehouse layer would be reflected in the ADLS layer

-- Updating scenario 2 Datalake to Lakehouse
in synapse analytics - updating data in synapse analytics (df = spark.sql("select * from LH_Fabric.shortcuttable") - next(df.createorReplaceTempView('updateview') - Next(spark.sql("UPDATE updateview SET column_name = "value" WHERE column_name = "value)) - Next, (display(spark.sql("SELECT COUNT(*) FROM updateview WHERE column_name = "value"")) - back to fabric - confirm if changes made in synapse analytics was reflected in fabric lakehouse (display(spark.sql("SELECT COUNT(*) FROM LH_FABRIC.shortcuttable WHERE column_name = "value"")

-- Shortcut deletion scenarios 1
if shortcut data in lakehouse is deleted, what effect does it have with data in ADLS container? data present in the lakehouse through shortcut, when deleted in the lakehouse, it automatically deletes in the ADLS container

- shortcut deleteion scenario 2: deleting specific content in ADLS container: what happens to the content present in Lakehouse? deleteing rows or columns from a data in ADLS, chages is reflected in Lakehouse, the same is applied as deleting the whole data file, the same is also applied in reverse

- Shortcut scenario 3 - delete table data in Lakehouse: delete content in shortcut tables section: deleting rows, columns, or data table in lakehouse, this changes is reflected in synapse analytics or ADLS

- Deletion Scenario 4 - Delete table data in ADLS: Dekete a specfic content of delta table in ADLS
- delete a row in syanapse analytics - changes is reflected both in fabric and ADLS

- Deletion Scenario 5: deleting entire shortcut in Lakehouse
- when a shortcut file/table is deleted in Lakehouse - these files and tables remain present in Synapse analytics and ADLS, in other words, data is not deleted at the source level

## Fabric Synapse Data Engineering
-- Spark in Microsoft Fabric:
In microsoft fabric, each workspace is assigned a cluster, an admin can manage these settings in the spark cluster
Workspace settings - data engineering/data science - spark settings 
Pool: Starterpool - default pool - New notebook - workspace default(workspace settings(there you see the default spark pool)
spark pool refers to compute environment which can run workloads. Can be categorised into two types
starterpool - default pool, can initialise sessions within 5-10 seconds. Have spark clusters that are always running. Clusters are groups of machines which can run a workload. Only charged when in use
starter pool configuration:
Node family: memory optimised, node size: mdeium, min and max nodes 1-10, auto scale: No, dynamic allocation -On. sart pool has to be used within 20min, else, it get disconnected

customising starter pool:
workspace settings - date engineering/data science - spark settings - the only settings allowed to be adjusted is the Nodes and allocate executors

-- custom spark pool:
this is created based on project requirements and specification - cluster size based on customer specifications
under what scenario is custom pool choosen?
when there is a tight budget, this allows to create a small node slize
creating a custom pool, workspace settings - data engineering/data science - spark pool - new pool - select node size(small) - enable auto scale(No) - Number of nodes (3) - save - notebook - workspcace default(should see the custom pool) - it took 2-3 minutes to initialise the spark session

-- standard vs high concurrency sessions
in the present notebook with the custom pool - create a new notebook -  initialise spark session in new cell(sc) - by default, both notebooks takes a standard session - the spark session takes aboiut 2-3 minutes to run. is there a better way if doing this? yes, we initialise the session in notebook 1 and share the session in notebook 2 using the high concurrency session - the main benefit is - since first notebook initialise the spark session and took 2-3 minituers, initialising in the second notebook takes around 15 second with the help of high conceurrency session
in other to create a high cioncurrency session in notebook - stop the standard session - connect - new high concurrency session - in new cell (sc), session runs in 2min 40 seconds - back on the second notebook - stop - attach to high conceurrency session - in newcell (sc) and run , session runs in 2 seconds
High conceurrency session is onky for sinkge user, can not benshared with multiple users
what is the use case if using this: if ever there is a notebook and there is a requirement in the current notebook that refers to other notebook, you can have the session shared in the current notrebook with another notebok using the high concurrency session
Benefit of high concurrency session:
multi task?: one user can use multiple notebooks with one session. prevents delays due to session creation
security: session shared is within a single user biundary
cost-effective: better resource utilisation and cost-saving



 




