# If a module is run directly, it must be imported in spire.tasks,
# otherwise it won't be discoverable
from spire.tasks.run_training import run_training, run_training_legacy
from spire.tasks.run_assembly import run_assembly, run_assembly_legacy
from spire.tasks.run_scoring import run_scoring, run_scoring_legacy
from spire.tasks.run_postprocessing import run_postprocessing
from spire.tasks.run_stage import run_stage
from spire.tasks.run_targets import run_targets
import spire.tasks.execute_targets as execute_targets
import spire.tasks.execute_postprocessing as execute_postprocessing
from spire.utils import get_logger
from spire.utils.databricks import monitor_runs
from spire.tasks.run_aam_table_updates import (
    run_mcid_segments_update,
    run_active_segments_update,
    run_aam_segments_updates,
)
import spire.tasks.execute_mcid_segments_update as execute_mcid_segments_update
import spire.tasks.execute_active_segments_update as execute_active_segments_update
import spire.tasks.execute_aam_segments_tables_updates as execute_aam_segments_tables_updates  # noqa

__all__ = [
    "run_stage",
    "run_training",
    "run_assembly",
    "run_scoring",
    "run_scoring_legacy",
    "run_targets",
    "run_postprocessing",
    "execute_targets",
    "execute_postprocessing",
    "run_mcid_segments_update",
    "run_active_segments_update",
    "run_aam_segments_updates",
    "execute_mcid_segments_update",
    "execute_active_segments_update",
    "execute_aam_segments_tables_updates",
]


logger = get_logger(__name__)

"""
Spire Tasks is, in effect, a set of Runner interfaces on top of the lower level run
code, not unlike the Spire API for the Workflow ORM. As with the Spire API, these
interfaces are called by the CLI and allow developers to change the lower level runner /
execution code without breaking the user tooling.

Example Usage:

```python
import datetime
from spire.tasks.run_stage import run_stage
from spire.framework.workflows import WorkflowStages

today = datetime.datetime.today()
yesterday = (today - datetime.timedelta(days=1))
run_dates = [today, yesterday]
stage = WorkflowStages.SCORING
dry_run = True (returns only the job definition dictionary without launching a cluster.
Default False)

# Do a dry_run of a Scoring backfill for two dates, for whichever Workflows would have
# been scheduled at those datetimes.
# NOTE that for run_stage, the job_configs for launching the clusters are automated in
# the code via definitions in the database, but this is not the case for run_targets.

jobs = run_stage(stage=stage, run_dates=run_dates, dry_run=True)

# No dry_run, and this time only for two specific workflows (regardless of whether or
# not they were scheduled)

wf_one_id = 'c12dd2d6-4562-41c8-98ad-1985c944dd4a'
wf_two_id = '8e1cb19b-2534-4460-a753-1370a380e6c8'
wf_ids = [wf_one_id, wf_two_id]

jobs = run_stage(stage=stage, run_dates=run_dates, wf_ids=wf_ids)
```
"""


def run_assembly_and_training_legacy(
    run_date, assembly_config, training_config, **context
):
    # initialize runners
    _, successfully_built = run_assembly_legacy(
        run_date, assembly_config, return_successful_workflows_only=True
    )
    if not successfully_built:
        logger.warning(
            "{} workflows successfully assembled"
            " training sets!".format(len(successfully_built))
        )
    else:
        logger.info(
            "sending {} workflows with successfully"
            "built training sets to model training".format(
                len(successfully_built)
            )  # noqa
        )
        run_id, _ = run_training_legacy(
            run_date, training_config, successfully_built, max_per_cluster=300
        )
        monitor_runs(run_id)


def run_assembly_and_training(run_date, **context):
    # initialize runners
    run_assembly(run_date)
    run_training(run_date)
