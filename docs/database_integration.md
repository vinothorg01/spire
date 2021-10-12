## Postgres Database Integration Overview  
  
Spire uses postgres databases for Workflow artifacts and Job Configs, among a small number of other features. See `spire.framework.workflows.__init__` for more on the Workflow Object Relational Mapping or the `SpireDBConnector` class in `spire.integrations.postgres` for more of the specific code implementations.  
  
This documentation is more so about how to get started with it and how to access it locally, as opposed to a detailed explanation of the code or how it's used.  
  
## Environments  
  
Spire has three environments; `production`, `staging`, and `development` (although NOTE: As of writing, the development database is out of date and will require database schema management via alembic and a data migration from production or staging or emptied clean).  
  
Certain environment parameters are hard-set via `spire.config`, but these depend on reading from environment variables, or in the case of the CLI, from `~/.spire/config.yml`. 
  
### Environment Variables  
  
The `VAULT`-related environment variables control access to the databases and is management by DevOps, and occasionally these tokens need to be updated.  
  
If you don't have access to these tokens, the following options are available to you:  
  
1. Retrieve it from the spire.config and/or the `conn_str` in a spireDBConnector instance such as `connector` imported from that same script, on an interactive cluster that already has these variables.  
  
2. Retrieve it from a cluster environment or cluster definition on Databricks, or possibly from one of the Astronomer deployment environment variables.  
  
3. Ask someone else who works on Spire.  
  
4. (If all else fails) Ask DevOps.   
  
**VAULT_ADDRESS:** A URL, the same for all environments  
  
**VAULT_READ_PATH:** A path to a `secret` db directory in `data-innovation`. Nearly the same for all three environments except for the environment-name level of the path  
  
**VAULT_TOKEN:** The token produced by DevOps and occasionally updated.  
  
In addition to `VAULT`, there is also `DB_HOSTNAME` and `DB_PASSWORD`.  

**DB_HOSTNAME:** AWS rds address that is nearly the same for all three environments but NOT identical. The same methods as mentioned above can be used to retrieve this.  
  
**DB_PASSWORD:** As of writing, this password is the same for all three environments, and you can potentially retrieve it with the same methods mentioned above, but may need to just ask someone.  
  
There are of course many more environment variables, but these are the primary ones relating to the databases.  
  
## PGAdmin  
  
We often use PGAdmin as a UI for the databases. We generally encourage users to write or interface with the database through Spire, but for developers it can be useful to also be able to access the databases directly and with more direct SQL querying tooling.  
  
### Loose Walkthrough  
  
1. Follow standard PGAdmin install steps via their documentation which you can Google.  
2. Create a Server (e.g. right-click on the Servers icon in the Browser, then `Create -> Server...`).  
3. Name it whatever you like, but probably something sensible and standard e.g. `Spire Development`, `Spire Staging`, etc.  
4. In the Connection tab, set `Host name/address` to the `DB_HOSTNAME` for that environment.  
5. In most cases leave default `Port` 5432.  
6. Change `Username` to `spiredevusr`, `spirestage`, or `spireprod`.  
7. Change `Password` to `DB_PASSWORD`.  
  
You should now be able to connect to that Server. From there, you can see `Databases -> spire_db -> Schemas -> Tables` to view the data or use the query tool to query specific rows (and joins or other more complex queries across tables).  
  
## Alembic  
  
Alembic is used for database schema management. You can follow the documentation in `README.md`, but the basic idea is that any time we create a new table or update the schema of a table (e.g. adding a new row), we want to make a versions script of the change with alembic, such that one could recreate the current schema or `update` a database via the versions scripts.  
  
**NOTE:** The database schema management via alembic is NOT the same thing as a database migration. A database can be backed up via the PGAdmin UI by right-clicking on the desired Server and following the logic in `Backup Server...` or via a direct SQL query.  
   