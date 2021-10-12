import datetime
from collections import defaultdict
from requests.exceptions import RequestException

from spire.framework.workflows import Workflow

from spire.integrations.postgres import connector

from spire.utils import constants, chunk_into_groups
from spire.utils.logger import get_logger
from spire.utils.databricks import get_run_status
from spire.utils.databricks import get_cluster_state, get_cluster_id, submit_job


LOGGER = get_logger()


def get_scheduled_workflows(session, stage, run_date):
    ready_to_run = []
    enabled_workflows = Workflow.load_all_enabled(session)
    for workflow in enabled_workflows:
        ready = workflow.schedule.ready_to_run(stage, run_date)
        if ready:
            ready_to_run.append(workflow)
    LOGGER.info("{} workflows scheduled for stage {}".format(len(ready_to_run), stage))
    return ready_to_run


def execute_stage_task(config, run_date, **kwargs):
    """
    TODO: deprecate
    """
    run_ids = []
    session = connector.make_session()
    workflows = get_scheduled_workflows(session, config["task_type"], run_date)
    grouped_workflows = chunk_into_groups(workflows, kwargs["group_size"])
    for group in grouped_workflows:
        ids_to_launch = [str(wf.id) for wf in group]
        run_id = submit_job(
            config,
            run_date,
            **{"workflows": ids_to_launch, "group_size": kwargs["group_size"]},
        )
        run_ids.append(run_id)
        LOGGER.info("job submitted -- id = {}".format(run_id))
        LOGGER.info(get_run_status(run_id))
    session.close()
    return run_ids


def launch_vendor_partitioned_stage(config, run_date):
    workflows_assembled = []
    run_ids = []
    task_name = config["task_type"]
    cluster_group_size = config["cluster_group_size"]
    vendors = config["data_vendors"]
    session = connector.make_session()
    try:
        scheduled_wfs = get_scheduled_workflows(
            session, constants.ASSEMBLE_TASK_ID, run_date
        )
        LOGGER.info(
            constants.N_TOTAL_WORKFLOWS_SCHEDULED.format(len(scheduled_wfs), task_name)
        )
        for vendor in vendors:
            LOGGER.info(constants.TRAINING_SET_PREP_BY_VENDOR.format(vendor))
            wfs_by_vendor = [wf for wf in scheduled_wfs if vendor in wf.name]
            ready_wfs = get_ready_workflows(task_name, wfs_by_vendor)[
                :cluster_group_size
            ]
            ids_to_launch = [str(wf.id) for wf in ready_wfs]
            if len(ids_to_launch) < constants.WORKFLOW_THRESHOLD_MIN:
                LOGGER.error(
                    constants.INSUFFICIENT_WORKFLOW_COUNT.format(
                        len(ids_to_launch), constants.WORKFLOW_THRESHOLD_MIN
                    )
                )
                continue
            LOGGER.info(
                constants.N_WORKFLOWS_LAUNCHED_BY_VENDOR.format(
                    len(ids_to_launch), vendor
                )
            )
            if not ready_wfs:
                LOGGER.warn(constants.NO_WORKFLOWS_READY)
            else:
                run_id = submit_job(
                    config, run_date, workflows=ids_to_launch, vendor=vendor
                )
                run_ids.append(run_id)
                cluster_id = get_cluster_id(run_id)
                for wf in ready_wfs:
                    wf.update_cluster_status(task_name, cluster_id, run_id)
                    workflows_assembled.append(wf.id)
                session.commit()
        return run_ids, [str(_id) for _id in workflows_assembled]
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def launch_single_cluster_stage(config, run_date, workflow_ids=None):
    task_name = config["task_type"]
    cluster_group_size = config["cluster_group_size"]
    session = connector.make_session()
    LOGGER.info(constants.PREPARING_TASK_LAUNCH.format(task_name))
    if not workflow_ids:
        scheduled_wfs = get_scheduled_workflows(session, task_name, run_date)
    else:
        wfs = session.query(Workflow).filter(Workflow.id.in_(workflow_ids)).all()
        scheduled_wfs = [
            wf for wf in wfs if wf.schedule.ready_to_run(task_name, run_date)
        ]
    if not scheduled_wfs:
        LOGGER.warn(constants.NO_WORKFLOWS_SCHEDULED.format(task_name))
        return scheduled_wfs
    try:
        ready_workflows = get_ready_workflows(task_name, scheduled_wfs)
        ids_to_launch = [str(wf.id) for wf in ready_workflows][:cluster_group_size]
        LOGGER.info(constants.N_WORKFLOWS_LAUNCHED.format(len(ids_to_launch)))
        run_id = submit_job(config, run_date, workflows=ids_to_launch)
        cluster_id = get_cluster_id(run_id)
        for wf in ready_workflows:
            wf.update_cluster_status(task_name, cluster_id, run_id)
        session.commit()
        return run_id
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def filter_failed_workflows(config, run_date, workflow_ids):
    if isinstance(run_date, datetime.datetime):
        run_date = datetime.datetime.combine(
            run_date.date(), datetime.datetime.min.time()
        )
    elif isinstance(run_date, datetime.date):
        run_date = datetime.datetime.combine(run_date, datetime.datetime.min.time())
    else:
        raise Exception("target_date must be date or datetime.")
    session = connector.make_session()
    stage = config["task_type"]
    try:
        wfs = session.query(Workflow).filter(Workflow.id.in_(workflow_ids)).all()
        status_reference = {
            wf.id: wf.last_successful_run_date(session, stage) for wf in wfs
        }
        return [k for k, v in status_reference.items() if v == run_date]
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def get_ready_workflows(stage, workflows):
    workflows_per_cluster = get_workflows_per_cluster(stage, workflows)
    return filter_finished_jobs(workflows_per_cluster)


def get_workflows_per_cluster(stage, workflows):
    workflows_per_cluster = defaultdict(list)
    for wf in workflows:
        for c in range(len(wf.cluster_status)):
            if wf.cluster_status[c].stage == "{}_status".format(stage):
                cluster_id = wf.cluster_status[c].cluster_id
                workflows_per_cluster[cluster_id].append(wf)
    return workflows_per_cluster


def filter_finished_jobs(cluster_groups):
    results = []
    for k, v in cluster_groups.items():
        if not k:
            results += v
            continue
        try:
            cluster_state = get_cluster_state(k)
            if cluster_state == constants.RUN_TERMINATED:
                results += v
        except RequestException:
            results += v
    return results


def execute_target_task(config, run_date, **kwargs):
    """
    TODO: deprecate
    :param config:
    :param run_date:
    :param kwargs:
    :return:
    """
    return submit_job(config, run_date)


def execute_task(config, **context):
    """
    TODO: deprecate
    """
    run_date = context["execution_date"]
    group_size = config.get("cluster_group_size")
    vendors = config.get("data_vendors")
    num_clusters = config.get("num_clusters")
    return task_type_lookup(config, run_date, group_size, vendors, num_clusters)


def task_type_lookup(
    config, run_date, group_size=None, vendors=None, num_clusters=None
):
    """
    TODO: deprecate
    """
    lookup = {
        "training": execute_stage_task,
        "scoring": execute_stage_task,
        "target": execute_target_task,
        "postprocessing": execute_postprocess_task,
    }
    return lookup[config["task_type"]](
        config,
        run_date,
        group_size=group_size,
        vendors=vendors,
        num_clusters=num_clusters,
    )


def execute_task_subset(config, run_date, workflow_ids, vendor=None):
    group_size = config["cluster_group_size"]
    run_ids = []
    grouped_objects = chunk_into_groups(workflow_ids, group_size)
    for group in grouped_objects:
        run_id = submit_job(config, run_date, workflows=group, vendor=vendor)
        run_ids.append(run_id)
        status_message = "job submitted -- id = {}".format(run_id)
        print(status_message)
    return run_ids


def execute_postprocess_task(config, run_date, **kwargs):
    run_ids = []
    session = connector.make_session()
    workflows = session.query(Workflow).filter(Workflow.enabled).all()
    grouped_workflows = chunk_into_groups(workflows, kwargs["group_size"])
    for group in grouped_workflows:
        ids_to_launch = [str(wf.id) for wf in group if len(wf.postprocessor) > 0]
        if len(ids_to_launch) == 0:
            LOGGER.error("No workflows in this group have postprocessors")
            continue
        LOGGER.info(
            f"{len(ids_to_launch)} workflows will be " f"postprocessed in this group"
        )
        run_id = submit_job(
            config,
            run_date,
            **{"workflows": ids_to_launch, "group_size": kwargs["group_size"]},
        )
        run_ids.append(run_id)
        LOGGER.info("job submitted -- id = {}".format(run_id))
        LOGGER.info(get_run_status(run_id))
    session.close()
    return run_ids


def execute_cli_runner(run_date, runner, **kwargs):
    runner.run_name = runner.run_name + "-cli"
    if kwargs["docker_url"]:
        runner.docker_image["url"] = kwargs["docker_url"]
    run_id, workflow_ids = runner.run(
        run_date, kwargs["workflows"], dry_run=kwargs["dry_run"]
    )
    return run_id, workflow_ids
