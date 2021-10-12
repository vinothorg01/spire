
# Spire Targets Data Sources

## [Adobe AAM](https://docs.adobe.com/content/help/en/analytics/integration/audience-analytics/mc-audiences-aam.html)
Adobe Audience Manager (our DMP) manages segments of network users from various origins - e.g. segments 
pushed by 3rd party vendors, onboarded client CRM files, behaviors of site users on our network, etc.
Adobe delivers daily files of incremental segment adds (users recently
assigned to segments) with files structured like so:
`User MCID | List[Segment1, Segment2, Segment3 ...]`.
<br></br>
Files are partitioned by the week, incremental files are added daily. 
On a defined schedule (now 30 days), Adobe pushes a *very big* 'refresh' file. 
The 'refresh' is a full snapshot (complete replacement) of the current segments (by user) - this 
is done periodically to 'sync' segment membership as users may be removed from segments. 

##### Location: s3://cn-data-vendor/adobe/aamsegsmcid


## [Acxiom](https://www.acxiom.com/customer-data/)

Acxiom was our offline data management vendor until April 2020.  They have been fully replaced by CDS.
We used Acxiom data for both Spire demographic modeling targets and for offline id conversion to xids 
for offline matched data sources - NCS and Polk via ga.norm_identity. Data engineering maintains the ingestion
and storage of consumed data into S3 - accessible via Databricks database 'acxiom'.  Axiom will be 
decommissioned soon (before EOY 2020).

##### Location: Databricks table -- `acxiom.<acxiom_mdw_cust>`  
                                    `acxiom.<acxiom_mag_level>`
 

## [CDS](https://www.cds-global.com/)

CDS fully replaced Acxiom as our offline data management vendor as of April 2020.  We've transitioned 
Spire demographic model targets to CDS/Expeiran sourced data.  Subsequent refreshes of offline matched data 
(NCS, Polk) will be converted to xids through CDS hosted ids (similar to prior Acxiom conversion) via ga.norm_identity.
Data Engineering maintains CDS data that is accessible via Databricks database 'cds' or on s3.

###### Location: Databricks table -- `cds.<alt_subscription_aggr>`, `cds.<alt_mdc_cust>`, `cds.alt_mdm_site`
###### 's3://cn-data-vendor/cds/inbound/active/'
>>>>>>> c21ca5fbe366ca4358ec6b927d94dc9f8c247467


## [DFP](https://admanager.google.com/home/)

Google Ad Manager (our ad server, formerly known as DFP, DoubleClick For Publishers) log data. 
We use campaign response to build Spire 'clicks' models - select order ID's are extracted from 
the primary data stream and used for labeled data in Spire modeling. Data Engineering provides streams 
in S3 partitioned by `click` events and by `impression` events. We manage the order IDs used in SpireDB 
in the `dfp_orders` table.

##### Location: `s3://cn-data-vendor/dfp/NetworkClicks/orc/` and `s3://cn-data-vendor/dfp/NetworkImpressions/orc/`


## [NCS](https://www.ncsolutions.com/solutions/target/)

NCS provides offline user-level data on product purchases (primarily CPG, mass drugstore/grocery).  
Two types: syndicated (~700 publically available segments) and custom - on demand, custom signals. 
DE facilitates data drops into S3.  Generally annually for syndicated (with potential for incremental), 
and on-demand for custom. We convert offline ids to xids via ga.norm_identity on an ongoing basis.

##### Location: `ncs.<ncs_segments_2020>` (syndicated) `s3://cn-data-reporting/spire/ncs/custom-parquet-v2/` (custom)


## [Polk]( https://ihsmarkit.com/products/polk-audiences.html)

Polk provides automobile purchase/lease data. Like NCS, Polk data is matched via offline PII matching and 
is refreshed annually (loosely).  We derive targets by loading the existing data from the Polk table 
and convert offline ids to xids via ga.norm_identity on an ongoing basis.

##### Location: https://www.polkcitydirectories.com/lp/auto-data/