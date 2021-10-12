from __future__ import annotations
import re
import uuid
import datetime
from collections import defaultdict
from typing import List, Union, Dict
import requests
from sqlalchemy.orm import relationship, Session
from sqlalchemy.schema import Index
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.sql.expression import func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import (
    Boolean,
    String,
    Column,
    DateTime,
)

from spire.framework.workflows.trait import Trait
from spire.framework.workflows.history import History
from spire.framework.workflows.dataset import BinaryClassesDataset, MultiClassesDataset
from spire.framework.workflows.dataset.dataset_makers import make_dataset_definition
from spire.framework.workflows.schedule import Schedule
from spire.framework.workflows.schedule_rebalancer import ScheduleRebalancer
from spire.framework.workflows.connectors import (
    Base,
    workflow_datasets,
    workflow_tags,
    workflow_features,
)
from spire.framework.workflows.job_config import WorkflowJobConfig
from spire.framework.workflows.cluster import (
    AssemblyStatus,
    TrainingStatus,
    ScoringStatus,
)
from spire.framework.workflows.postprocessor import Thresholder
from spire.framework.workflows.workflow_stages import WorkflowStages
from spire.framework.workflows.constants import (
    DEFAULT_ASSEMBLY_CONFIG_NAME,
    DEFAULT_TRAINING_CONFIG_NAME,
    DEFAULT_SCORING_CONFIG_NAME,
)
from spire.framework.workflows.job_config import JobConfig
from spire.integrations import SkittlesMixin
from spire.framework.execution import Job

from spire.integrations.postgres import connector

from spire.utils.constants import EXTRACT_VENDOR_REGEX
from spire.utils.databricks import get_cluster_state
from spire.utils import constants
from spire.utils.logger import get_logger


logger = get_logger(__name__)

"""
For a higher level overview of the Workflow framework, read the docstring in
`spire/framework/workflows/__init__.py`!
"""


class Workflow(Base, SkittlesMixin):
    __tablename__ = "workflows"
    id = Column(UUID(as_uuid=True), primary_key=True, unique=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(100), nullable=False)
    created_at = Column(DateTime(), default=datetime.datetime.now)
    modified_at = Column(DateTime(), default=datetime.datetime.now)
    enabled = Column(Boolean(), nullable=False)
    is_proxy = Column(Boolean(), nullable=False, default=False)

    """
    In production, the majority of workflows are enabled, whereas in staging the
    majority are disabled. This skew can cause a query on enabled to timeout (>300ms).
    These indices make it so that if there is a skew in either direction, the query can
    leverage the partial index binary tree to speed up the query given the condition.
    """
    __table_args__ = (
        Index(
            "enabled_idx",
            "enabled",
            postgresql_where=(enabled),
        ),
        Index(
            "disabled_idx",
            "enabled",
            postgresql_where=(~enabled),
        ),
    )

    dataset = relationship(
        "Dataset",
        secondary=workflow_datasets,
        uselist=False,
        single_parent=True,
        cascade="all, save-update, merge, " "delete, delete-orphan",
        passive_deletes=True,
        backref="workflow_datasets",
        # this to be False because dataset.workflow is viewonly
        sync_backref=False,
    )
    features = relationship(
        "Features", secondary=workflow_features, back_populates="workflows"
    )
    schedule = relationship(
        "Schedule",
        uselist=False,
        cascade="save-update, merge, " "delete, delete-orphan",
        passive_deletes=True,
        back_populates="workflow",
    )
    history = relationship(
        "History",
        back_populates="workflow",
        cascade="save-update, merge, delete",
        passive_deletes=True,
    )

    trait = relationship(
        "Trait",
        uselist=False,
        cascade="save-update, merge, delete, delete-orphan",
        back_populates="workflow",
    )
    postprocessor = relationship(
        "Postprocessor",
        cascade="save-update, merge, delete," "delete-orphan",
        passive_deletes=True,
        back_populates="workflow",
    )
    cluster_status = relationship(
        "ClusterStatus",
        cascade="save-update, merge," "delete, delete-orphan",
        passive_deletes=True,
        back_populates="workflow",
    )
    tags = relationship("Tag", secondary=workflow_tags, back_populates="workflows")

    job_configs = association_proxy(
        "workflow_jobconfigs",
        "job_config",
        creator=lambda k, v: WorkflowJobConfig(stage=k, job_config=v),
    )

    def __init__(self, name, description, is_proxy=False, enabled=False):
        """
        The attributes for this workflow. If initialized (creating a new workflow),
        a UUID will be created for this workflow instance. If loading a pre-existing
        workflow, it is not initialized and will retain its uuid from the database.

        id: UUID for the workflow which is used as join key for other tables.
        name: Workflow name in the format spire_<vendor>_<trait_id> i.e.
        `spire_adobe_30`
        description: Also known as ui_name in Segmentation Framework
        (outside of Spire core). Will look something like i.e.
        `Spire > Beauty > In-Market > Cosmetics > Foundation Make-Up`
        enabled: Boolean, determines whether or not the workflow will be included in
        scheduled tasks
        is_proxy: Boolean, edge-case used only for proxy / passthrough workflows
        """
        self.id = uuid.uuid4()
        self.name = name
        self.description = description
        self.enabled = enabled
        self.is_proxy = is_proxy

    def __repr__(self):
        if self.trait:
            trait_id = self.trait.trait_id
        else:
            trait_id = None
        return (
            "<Workflow(id='{}', name='{}', description='{}', "
            "trait_id={}, enabled={})>".format(
                self.id, self.name, self.description, trait_id, self.enabled
            )
        )

    def to_dict(self):
        """
        Ad-hoc serializer in lieu of a schema manager such as marshmallow
        (https://marshmallow-sqlalchemy.readthedocs.io/en/latest/)
        """
        if hasattr(self.trait, "trait_id"):
            trait_id = str(self.trait.trait_id)
        else:
            trait_id = None
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "trait_id": trait_id,
            "enabled": self.enabled,
        }

    def update_dataset(self, session, definition={}, dataset_obj=None):
        # Deserializes a Dataset instance from a definition, then adds it to the
        # session and to the workflow. Does NOT commit the session automatically.
        if dataset_obj:
            obj = dataset_obj
            session.add(dataset_obj)
        else:
            families = {
                "binary": BinaryClassesDataset,
                "multiclass": MultiClassesDataset,
            }
            obj = families[definition["family"]].from_dict(definition)
            session.add(obj)
        self.dataset = obj

    def update_job_config(self, stage, config_name, session):
        # Queries the database for job config of config_name, adds it to the workflow
        # job_config dict, and automatically commits the session.
        try:
            cfg = session.query(JobConfig).filter(JobConfig.name == config_name).one()
            self.job_config[stage] = cfg
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

    def update_schedule(self, session, definition):
        # Deserializes a Schedule instance from a definition, then adds it to the
        # session and to the workflow. Does NOT commit the session automatically.
        obj = Schedule.from_dict(definition)
        session.add(obj)
        self.schedule = obj

    def update_trait_id(self, session, trait_id):
        # If the trait_id is not already in use, deserializes a Trait instance from a
        # trait_id, then adds it to the session and to the workflow. Does NOT commit
        # the session automatically.
        if not self.is_trait_exist(session, trait_id):
            obj = Trait(workflow_id=self.id, trait_id=trait_id)
            session.add(obj)
            self.trait = obj

    def append_postprocessor(self, session, postprocessor_obj):
        # Appends a postprocessor instance to the workflow postprocessor list and adds
        # it to the session. Does NOT commit the session automatically.
        assert postprocessor_obj.type in [Thresholder.__name__], (
            f"{postprocessor_obj.type} is not a supported " f"Postprocessor type"
        )
        session.add(postprocessor_obj)
        self.postprocessor.append(postprocessor_obj)

    def remove_postprocessor(self, postprocessor_obj):
        # Removes a postprocessor instance from a workflow's postprocessor list. Does
        # NOT commit the session automatically.
        self.postprocessor.remove(postprocessor_obj)

    def commit_status(self, session, response):
        # Wraps and runs update_cluster_status, then automatically commits the change
        # from the session.
        try:
            stage = response["stage"]
            cluster_id = response["cluster_id"]
            run_id = response["run_id"]
            self.update_cluster_status(stage, cluster_id, run_id)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

    def update_cluster_status(self, stage, cluster_id, run_id):
        # Updates the workflow cluster_status corresponding to the stage in the
        # cluster_status list from the response. Does NOT commit the session
        # automatically.
        if stage not in ["assembly", "training", "scoring"]:
            raise ValueError(
                """{} is not a supported spire
                             processing stage""".format(
                    stage
                )
            )
        for status in self.cluster_status:
            if status.stage == "{}_status".format(stage):
                status.update(cluster_id, run_id)
                return
        self._add_new_stage_status(stage, cluster_id, run_id)

    def _add_new_stage_status(self, stage, cluster_id, run_id):
        # Automates finding the cluser_status list entry corresponding to the
        # appropriate stage
        cluster_lookup = {
            "assembly": AssemblyStatus,
            "training": TrainingStatus,
            "scoring": ScoringStatus,
        }
        new_stage_status = cluster_lookup[stage](cluster_id, run_id)
        self.cluster_status.append(new_stage_status)

    def commit_history(self, response_or_history: Union[Dict, History], session):
        # Either adds a history instance to the workflow.history list or first creates
        # that history instance from a dictionary, and automatically commits the new
        # history to the session.
        try:
            if isinstance(response_or_history, History):
                new_history = response_or_history
            else:
                new_history = History(**response_or_history)

            self.history.append(new_history)
            session.add(new_history)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

    def commit(self, session):
        # Handling to commit all changes from the session to the database. Used in
        # create_default_workflow.
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

    def get_feature_definition(self, stage):
        # NOTE(Max): Feature ORM functionality is extremely outdated...
        stages = ["assembly", "training", "scoring"]
        if stage not in stages:
            raise ValueError(
                """Argument passed for 'stage'
                             must be in {} \n Recieved value '{}'
                             """.format(
                    stages, stage
                )
            )
        for definition in self.features:
            if definition.stage == stage:
                return definition
        raise RuntimeError(
            """Workflow {} missing feature definition
                           for stage {}""".format(
                self.id, stage
            )
        )

    def load_features(self, stage, cache=False, **kwargs):
        # NOTE(Max): Feature ORM functionality is extremely outdated...
        feature_definition = self.get_feature_definition(stage)
        features = feature_definition.load(cache=cache)
        transformed_filters = feature_definition._transform_if_filters(
            self.name, **kwargs
        )  # noqa
        if transformed_filters:
            return feature_definition.apply_filters(
                features, transformed_filters, self.name
            )  # noqa
        return features

    def transform_features(self, stage, dataset):
        # NOTE(Max): Feature ORM functionality is extremely outdated...
        feature_definition = self.get_feature_definition(stage)
        return feature_definition.transform(dataset)

    def add_tag(self, tag):
        # NOTE(Max): Tag ORM functionality is extremely outdated...
        self.tags.append(tag)

    def remove_tag(self, tag):
        # NOTE(Max): Tag ORM functionality is extremely outdated...
        self.tags.remove(tag)

    @classmethod
    def load_all_enabled(cls, session):
        # Session query that returns all enabled Workflow instances.
        try:
            return session.query(Workflow).filter(Workflow.enabled).all()
        except Exception as e:
            session.rollback()
            raise e

    @classmethod
    def get_by_trait(cls, session, trait_id):
        # Session query that returns the Workflow instance with the trait_id.
        return (
            session.query(Workflow).join(Trait).filter(Trait.trait_id == trait_id).one()
        )

    @classmethod
    def get_by_name_like(cls, session, name_like):
        # Session query that returns a list of Workflow instances where the name
        # follows the string pattern of name_like.
        response = (
            session.query(Workflow)
            .filter(func.lower(Workflow.name).like("%{}%".format(name_like)))
            .all()
        )
        return response

    @classmethod
    def get_by_name(cls, session, name):
        # Session query that returns a Workflow instance with the name.
        response = (
            session.query(Workflow).filter(func.lower(Workflow.name) == name).one()
        )
        return response

    @classmethod
    def get_by_id(cls, session, id):
        # Session query that returns a Workflow instance with the id (UUID).
        response = session.query(Workflow).filter(Workflow.id == id).one()
        return response

    @classmethod
    def get_by_ids(cls, session, ids):
        # Session query that returns a list of Workflow instances with ids in the list
        # of ids.
        response = session.query(Workflow).filter(Workflow.id.in_(ids)).all()
        if len(response) != len(ids):
            found_ids = set([str(w.id) for w in response])
            missing_ids = set(ids) - found_ids
            logger.warning(
                f"Could not find workflow with the following ids {missing_ids}"
            )
        return response

    @classmethod
    def get_by_description_like(cls, session, desc_like):
        # Session query that returns a list of Workflow instances where the description
        # follows the string pattern desc_like
        response = (
            session.query(Workflow)
            .filter(func.lower(Workflow.description).like("%{}%".format(desc_like)))
            .all()
        )
        return response

    @classmethod
    def get_by_description(cls, session, desc):
        # Session query that returns a Workflow instance with the description desc.
        response = (
            session.query(Workflow)
            .filter(func.lower(Workflow.description) == desc)
            .one()
        )
        return response

    @classmethod
    def is_trait_exist(cls, session, trait_id):
        q = session.query(Trait).filter(Trait.trait_id == trait_id)
        exists = session.query(q.exists()).scalar()
        if exists:
            print("Given trait id exist, please update workflow with another trait id")
        return exists

    @classmethod
    def create_default_workflow(
        cls,
        name,
        description,
        vendor,
        source=None,
        trait_id=None,
        enabled=False,
        start_date=None,
        schedule=None,
        session=None,
        **kwargs,
    ) -> "Workflow":
        """
        Initializes and commits to postgres a Spire workflow with the
        specified target behaviors and default stage configs.
        """
        dataset = make_dataset_definition(vendor, source, **kwargs)
        workflow = cls(name, description, enabled=enabled)
        workflow.dataset = dataset

        # Default JobConfigs possessing the default names must be present
        # in the associated database prior to this function being called.

        assembly_config = (
            session.query(JobConfig)
            .filter(JobConfig.name == DEFAULT_ASSEMBLY_CONFIG_NAME)
            .one()
        )
        workflow.job_configs["assembly"] = assembly_config

        training_config = (
            session.query(JobConfig)
            .filter(JobConfig.name == DEFAULT_TRAINING_CONFIG_NAME)
            .one()
        )
        workflow.job_configs["training"] = training_config

        scoring_config = (
            session.query(JobConfig)
            .filter(JobConfig.name == DEFAULT_SCORING_CONFIG_NAME)
            .one()
        )
        workflow.job_configs["scoring"] = scoring_config

        if not start_date:
            start_date = datetime.date.today() - datetime.timedelta(days=1)
        if not schedule:
            workflow.add_workflow_to_balanced_schedules(
                session, vendor=vendor, start_date=start_date
            )
        else:
            workflow.schedule = schedule
        assembly_status = AssemblyStatus()
        training_status = TrainingStatus()
        scoring_status = ScoringStatus()
        workflow.cluster_status = [assembly_status, training_status, scoring_status]
        if trait_id and not workflow.is_trait_exist(session, trait_id):
            workflow.trait = Trait(workflow_id=workflow.id, trait_id=trait_id)
        workflow.commit(session)
        return workflow

    def add_workflow_to_balanced_schedules(self, session, vendor=None, start_date=None):
        if not start_date:
            start_date = datetime.date.today() - datetime.timedelta(days=1)
        rb = ScheduleRebalancer()
        rb.START_DATE = start_date
        rb.vendor = vendor
        all_workflows = Workflow.load_all_enabled(session)
        rb._get_schedule_counts(all_workflows)
        schedule = rb.create_new_balanced_schedule()
        self.schedule = schedule

    @property
    def vendor(self):
        # Returns the Workflow target vendor from the name as if it were an attribute,
        # assuming the name follows the typical workflow name convention:
        # spire_<vendor>_<trait_id>
        return re.search(EXTRACT_VENDOR_REGEX, self.name).group(0)

    @classmethod
    def get_ready_workflows(cls, stage, run_date):
        """
        Get workflows that is ready to be process, filtering out workflows
        that is already running.
        """
        assert stage in ["scoring", "training", "assembly"]
        session = connector.make_session()
        ready_to_run = []
        enabled_workflows = Workflow.load_all_enabled(session)

        for workflow in enabled_workflows:
            ready = workflow.schedule.ready_to_run(stage, run_date)
            if ready:
                ready_to_run.append(workflow)

        workflows_per_cluster = defaultdict(list)
        for wf in ready_to_run:
            for c in range(len(wf.cluster_status)):
                if wf.cluster_status[c].stage == f"{stage}_status":
                    cluster_id = wf.cluster_status[c].cluster_id
                    workflows_per_cluster[cluster_id].append(wf)

        results = []
        for cluster_id, workflows in workflows_per_cluster.items():
            # for workflows with no status, add them to queue
            if not cluster_id:
                results += workflows
            else:
                # else, check if it is already running first
                try:
                    cluster_state = get_cluster_state(cluster_id)
                    if cluster_state == constants.RUN_TERMINATED:
                        results += workflows
                except requests.exceptions.RequestException:
                    results += workflows

        return results

    @staticmethod
    def get_jobs_for_workflows(workflows, stage, run_date: datetime.date):
        groups = defaultdict(list)
        for workflow in workflows:
            if workflow.job_configs.get(stage.value):
                cfg = workflow.job_configs[stage.value]
                groups[cfg].append(workflow)
            else:
                logger.info(f"{workflow.name} missing job_config for {stage.value}")

        jobs = []
        for job_config, workflows in groups.items():
            group_jobs = job_config.get_jobs(workflows=workflows, run_date=run_date)
            jobs.extend(group_jobs)

        return jobs

    @classmethod
    def get_scheduled_jobs(
        cls,
        run_date: datetime.date,
        stage: WorkflowStages,
    ) -> List[Job]:
        """Prepare Jobs for `run_date` and `stage`."""

        ready_workflows = cls.get_ready_workflows(stage, run_date)
        if len(ready_workflows) == 0:
            return []

        return cls.get_jobs_for_workflows(ready_workflows, stage, run_date)

    @staticmethod
    def get_ran_workflows_by_date(
        date: datetime.datetime, stage: WorkflowStages, workflow_ids: List[str]
    ):
        session = connector.make_session()
        workflows = session.query(Workflow).filter(Workflow.id.in_(workflow_ids)).all()
        return list(
            filter(
                lambda wf: wf.last_successful_run_date(session, stage) == date,
                workflows,
            )
        )

    def last_successful_run_date(self, session: Session, stage: WorkflowStages):
        result = (
            session.query(History.arg_date)
            .filter(History.status == "success")
            .filter(History.workflow_id == self.id)
            .filter(History.stage == stage)
            .order_by(History.arg_date.desc())
            .first()
        )
        if result is not None:
            return result.arg_date

    def get_recent_failures(
        self,
        session: Session,
        stage: str,
        seconds: int,
        as_of: datetime.datetime = None,
    ):
        if as_of is None:
            as_of = datetime.datetime.now()
        return (
            session.query(History)
            .filter(History.status != "success")
            .filter(History.workflow_id == self.id)
            .filter(History.stage == stage)
            .filter(History.arg_date >= as_of - datetime.timedelta(seconds=seconds))
            .all()
        )
