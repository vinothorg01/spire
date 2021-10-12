# Spire Change Log

## Version 4.1.3
* Fix scoring; cd

## Version 4.1.2
* Update parquet writes

## Version 4.1.1
* Update spire-astronomer (#617)

## Version 4.1.0
* [SPIRE-370][CORE] Create AAM MCID Segments Table (#560)
* [SPIRE-372][CORE] Create AAM Segments Table (#559)
* [SPIRE-389][CORE]: Create tasks for mcid segments and active segments updates
* [SPIRE-371][CORE]: Adobe Targets Refactor (#562)
* [BUG] Allow args=None for OneNoteBookJobs and update aam targets
* change parameter order (#571)
* Update tests to include no args (#572)
* [BUG] Aam tables write fixes (#573)
* [BUG] AAM Fixes (#575)
* typo (#577)
* [BUG] Remove date lag (#578)
* [BUG] Add GA date check and revert to 2 day lag for targets (#580)
* Revert to old method (#584)
* Fix write bug for passthrough scores (#553)
* [SPIRE-375][CLI] Update CLI Runners (#554)
* Fix/dataset cli (#579)
* [BUGFIX] Fix schedule not accessible after commit (#545)
* Schedule code refactor (#546)
* [SPIRE-367][OPS] Fix default schedule for UTC (#550)
* [SPIRE-384][Ops] Remove Submodules from Code Climate (#555)
* [SPIRE-336][Ops] Automate CD to Staging Deployment (#520)
* Add Slack Notification for deploying to staging (#574)
* [BUG] Remove WorkflowLatestHistory and modified_at History column (#610)
* [BUG] Fix scoring write (#611)

## Version 4.0.5
* @harin, fix spire version always None (#523)
* @harin, Standardize Spark Initialization (#517)
* @maxcan7, [SPIRE-247][CORE] Update scoring and passthrough for  affiliates (#493)
* @maxcan7, [SPIRE-349][Ops][Core] Fix Scoring Dataframe Bug (#534)
* @maxcan7, [SPIRE-335][CORE] Update setup.py to read version as file rather than module (#519)
* @harin, [SPIRE-331][CORE] Implement MLFlow Postprocessor (#516)
* @maxcan7, Fix Date Type for Scoring (#537)

## Version 4.0.3
* @maxcan7, Downgrade pendulum to 1.4.4 (#512)
* @harin, [BUG] Update kalos load arguments (#513)
* @harin, Fix CI and CD (#509)
* @harin, merge dbr_7x requirements (#507)

## Version 4.0.0
* @astham18, change adobe key name (#505)
* @astham18, [hotfix][CORE] Update cluster configuration with docker image and environment variables (#502)
* @maxcan7, [hotfix][CORE] Add partial indexes for workflow enabled and disabled (#501)
* @maxcan7, [hotfix][CORE]Add back in two deleted workflow History methods (#499)
* @harin, [SPIRE-321][CORE] One Notebook to Rule them ALL (#497)
* @astham18, Add job monitoring, backend_config to run_targets.py (#500)
* @maxcan7, Update scoring delta output path (#498)
* @astham18, [SPIRE-299] Add targets task and executable script for DAG (#491)
* @astham18, [SPIRE-300][CORE] Add AAM tests (#495)
* @harin, [SPIRE-309][Operations] remove module level spark load (#481)
* @astham18, [SPIRE-320][CORE] Add date check for GA Norm table (#484)
* @harin, [SPIRE-292][Operations] Remove deprecated methods (#482)
* @astham18, [SPIRE-298][CORE]: Update targets write method and fix refactor bugs (#475)
* @astham18, [SPIRE-295][CORE]: Remove Polk, Acxiom, Jumpshot code from datasets (#480)
* @harin, [SPIRE-273] Fix MLFlowProjectJobConfig bugs (#473)
* @harin, [SPIRE-310][CORE] Add event hooks to Job (#479)
* @harin, remove history init override (#478)
* @harin, disable workflow dataset sync (#476)
* @harin, Rename MLFlowProjects to MLFlowProject (#474)
* @maxcan7, [SPIRE-276][API] Move Workflow query functionality to History API (#470)
* @astham18, [SPIRE-229][CORE] Targets refactor design (#464)
* @maxcan7, [SPIRE-275][API] Update spire APIs (#469)
* @maxcan7, [SPIRE-254][CORE] Mirror scores format in Delta Lake format (#467)
* @harin, [SPIRE-271][CORE] Add MLFlow Github Authentication (#468)
* @maxcan7, Add wraps decorator to wrap_session method on SpireDBConnector (#466)
* @maxcan7, [Spire-232][API] Create Spire API (#458)
* @DurgaSankar-CN, Using spire DB connector instead start_session (#465)
* @sliu4, [SPIRE-159] Add environment to reporter output (#462)
* @astham18, [SPIRE-217][CORE] Targets Refactor: Refactor utils, constants (#456)
* @maxcan7, [SPIRE-60][CORE] Refactor SpireDBConnector (#455)
* @harin, [SPIRE-191][CORE] Implements MLFlow Project Runner (#451)
* @astham18, [SPIRE-193][CORE] Tests for Spire's Targets Processing Layer (#452)
* @harin, [SPIRE-198][CORE] Implement configuration management (#446)

## Version 3.3.0
* @DurgaSankar-CN, @maxcan7, Added traits and tags CLI (#396)
* @harin, Dockerize Spire (#392)
* @maxcan7, Refactor of Features for Spire Global (#374)
* @harin, Fix continuous deployment process and documentation (#402)
* @harin, Standardize Environment Variable Access in Spire (#397) 
* @maxcan7, Remove separate write type for adobe full_
* @DurgaSankar-CN, Added Runner CLI (#419)
* @maxcan7, @paulfryzel, Implement automatic rebalancing of assembly and training schedules (#347)
* @pbhosale87, Feature/get workflow by id (#424)
* @DurgaSankar-CN, Added the rebalancer into CLI (#426) 
* @harin, [SPIRE-104][CORE] Add Slack Notification for new builds (#428) 

## Version 3.2.2
* @maxcan7, Fix postprocessing wheel lookup (#399)

## Version 3.2.1
* @maxcan7, updated SPIRE_DBFS_URI constant (#394)

## Version 3.2.0
* @DurgaSankar-CN, Added clusterstatus and schedule CLI (#383)
* @harin, @maxcan7, Add alembic support (#351)
* @harin, Fix version bug in wheel name (#385)
* @harin, Fix dfp\_orders table lookups (#388)
* @harin, Add delete workflow test (#389)
* @paulfryzel, Fix segments\_array lookup in AAM targets load (#387)
* @paulfryzel, Update default branch to main
* @paulfryzel, Update spire-astronomer include to 3bc0ff2

## Version 3.1.1
* N/A

## Version 3.1.0 (Stable)
* Change from egg- to wheel-based distribution
* Added ExpirationSchedule
* For other changes see v3.0.0...v3.1.0

## Version 3.0.0 (Stable)
* Adds Integration with Skittles
* Outputs scores as Delta files partitioned by workflow id
* Adds Databricks Job Runners

## Version 2.4.1 (Stable)
* Adds Dermstore processing logic
* Adds bugfixes to schedule, datasets, cluster status, and postprocessing

## Version 2.4.0 (Stable)
* N/A

## Version 2.3.0 (Stable)
* Major refactor of core ORM with SQLAlchemy

## Version 2.1.1 (Stable)
* Add updated code for scoring, incorporating design changes to Kalos
* Add support for IQVIA models
* Bug fixes

## Version 2.1.0 (Stable)
* Major refactor of underlying data types and APIs
* Integration of Databricks and execution utils 
