import json
import yaml

from spire.utils.databricks import post_to_databricks
from spire.framework.constants import CLUSTER_TYPES, TASK_TYPES
from spire.config import config
from deprecation import deprecated


class JobRunner:
    """A wrapper class for executing Databricks Spark tasks

    JobRunners are meant to provide additional flexibility for launching
    tasks in varied environments and to expedite development
    """

    # this doesn't actually print a warning, but putting the decorator
    # here so we can find it and remove in the future.
    @deprecated(details="Please use spire.framework.execution instead")
    def __init__(self, task_name, **kwargs):
        """
        Create a new JobRunner. No initial params are required to
        instantiate the class, but no methods will be able to run without
        all specified params.

        :param run_name: kwargs[str]: Databricks Spark job name
               (e.g. 'spire-targets-aam-development').
        :param task_name: kwargs[str]: The informal reference for a certain process
        :param cluster_type: kwargs[str]: Databricks API param
                -- valid options are 'existing_cluster' and 'new_cluster_id'.
        :param libraries: kwargs[list[str]]: A list of dependencies for the job to execute,
                (e.g. ['pandas', 'numpy'])
        :param task_type: kwargs[str]: The type of task to submit to the Databricks API
                -- valid options are notebook_task and spark_python_task
                TODO: support for JAR tasks
        :param runner_script: kwargs[str]: The location of .py file or notebook.
                -- for python scripts, S3 and DBFS URI's are both valid options
        :param timeout_seconds: kwargs[int]: An optional timeout duration for the job,
                -- default from the Databricks API is none
        :param cluster_id: **kwargs[str]: If 'cluster_type' is 'existing_cluster_id',
                this param specifies which existing cluster
                (e.g. '0122-223212-aided963')
        :param wheels: kwargs[List[str]]: A list of dbfs wheel locations
        :param cluster_config: **kwargs[dict[str]]: If 'cluster_type' is 'new_cluster',
                this param contains a config meeting the requirements specified here:
                https://docs.databricks.com/dev-tools/api/latest/jobs.html#jobsclusterspecnewclus   # noqa: E501


        >>> from spire.framework.runners import JobRunner
        >>> r = JobRunner(task_name='howdy')
        """
        self.task_name = task_name
        self.cluster_type = None
        self.run_name = None
        self.task_type = None
        self.runner_script = None
        self.timeout_seconds = None
        self.cluster_id = None
        self.cluster_config = None
        self.libraries = []
        self.wheels = []
        self.docker_image = None

        self.set_attributes(**kwargs)

    def set_attributes(self, **kwargs):
        """Sets attributes on class instance"""
        self.task_name = kwargs.get("task_name", self.task_name)

        attrs = [
            "task_type",
            "task_name",
            "run_name",
            "cluster_type",
            "libraries",
            "runner_script",
            "timeout_seconds",
            "cluster_id",
            "cluster_config",
            "wheels",
            "docker_image",
        ]

        unsupported_kwargs = set(kwargs.keys()) - set(attrs)
        if unsupported_kwargs:
            raise ValueError(f"Unsupported kwargs: {unsupported_kwargs}")

        for attr in attrs:
            new_attr = kwargs.get(attr)
            if new_attr:
                setattr(self, attr, kwargs.get(attr))

    def load_yaml_config(self, path):
        with open(path) as f:
            return yaml.safe_load(f)

    @classmethod
    def parse_python_spark_task_params(cls, task, params):
        """Parse params for spark python task"""
        formatted_params = [v for k, v in params.items()]
        task["spark_python_task"].update(parameters=formatted_params)
        return task

    @classmethod
    def parse_notebook_task_params(cls, task, params):
        """Parse params for notebook task"""
        formatted_params = {k: v for k, v in params.items()}
        task["notebook_task"].update(base_parameters=formatted_params)
        return task

    def parse_task_params(self, task, params):
        """Formats input params according to the Databricks API
        specifications for each type of task

        A notebook task expects to receive params as a dictionary
        e.g. {'date': '2666-01-01', 'workflow_id': 2}

        A spark python task will be fed arguments by sys.argv (a list)
        e.g. ['2666-01-01', 2]
        """
        try:
            if "spark_python_task" in task.keys():
                return JobRunner.parse_python_spark_task_params(task, params)
            return JobRunner.parse_notebook_task_params(task, params)
        except Exception:
            raise TypeError(
                """spark_python_task params
                            must be a dict -- recieved type {}""".format(
                    type(params)
                )
            )

    def _set_cluster_type(self):
        if self.cluster_type == "new_cluster":
            assert self.cluster_config is not None
            new_cluster = self.cluster_config.copy()
            if self.docker_image is not None:
                new_cluster["docker_image"] = self.docker_image
            return {"new_cluster": new_cluster}
        assert self.cluster_id is not None
        return {"existing_cluster_id": self.cluster_id}

    def _configure_cluster(self):
        """Formats cluster setting data for either an existing
        cluster or a new cluster according their respective
        requirements from the Databricks API
        """

        if not self.cluster_type:
            raise ValueError(
                """cluster settings have not been set,
                            valid options are \n
                            ['existing_cluster_id','new_cluster']"""
            )
        if self.cluster_type and self.cluster_type not in CLUSTER_TYPES:
            raise ValueError(
                """Passed cluster type {} does not exist.
                             Valid types are {}""".format(
                    self.cluster_type, "\n".join([x for x in CLUSTER_TYPES])
                )
            )
        return self._set_cluster_type()

    def _configure_libraries(self):
        """Formats passed dependencies in the format expected by
        the Databricks Jobs API
        e.g. [{'pypi': {'package': numpy}}]
        TODO: Add support for wheel and JAR
        """
        if not isinstance(self.libraries, list):
            raise TypeError("Dependency libraries must be passed as a list")
        if not self.libraries:
            return []

        formatted_libraries = [
            {"pypi": {"package": package}} for package in self.libraries
        ]

        for wheel in self.wheels:
            formatted_libraries.append({"whl": wheel})

        return {"libraries": formatted_libraries}

    def _configure_task(self):
        """Prepares JSON for the Databricks API according to
        the expectations for a spark_python_task or notebook_task
        TODO: Add support for JAR task
        """
        if self.task_type not in TASK_TYPES:
            raise ValueError(
                """Task type {} is not a valid Databricks task.
                            Valid task types are: \n {}""".format(
                    self.task_type, "\n".join([x for x in TASK_TYPES])
                )
            )
        if self.task_type == "spark_python_task":
            return self._configure_python_spark_task()
        return self._configure_notebook_task()

    def _configure_notebook_task(self):
        """Prepares JSON for a notebook task"""
        return {"notebook_task": {"notebook_path": self.runner_script}}

    def _configure_python_spark_task(self):
        """Prepares JSON for a spark python task"""
        return {"spark_python_task": {"python_file": self.runner_script}}

    def prepare_json(self):
        """Formats job data as expected by the Databricks Runs Submit API
        Required JSON structure and further details here:
        https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit
        """
        run_json = {}
        run_name_json = {"run_name": self.run_name}
        timeout_seconds = {"timeout_seconds": self.timeout_seconds}
        cluster_json = self._configure_cluster()
        libraries_json = self._configure_libraries()
        task_json = self._configure_task()
        job_data = [
            run_name_json,
            cluster_json,
            libraries_json,
            task_json,
            timeout_seconds,
        ]
        for data in job_data:
            run_json.update(data)
        return run_json

    def launch_task(self, params, dry_run=False):
        """Takes a dictionary of params and launches
        a Databricks Spark task as specified by the attributes.

        :param params: dict: arguments to be passed to the Spark job
                NB: Pass a dictionary for both notebook_tasks
                and python_spark_tasks. Data will be parsed
                according to required formats for both jobs
                via internal helper methods
        """
        if not isinstance(params, dict):
            raise TypeError(
                """job input params must be
                            passed as a dictionary"""
            )
        self.inject_vault_token()
        job_config = self.prepare_json()
        job_to_launch = self.parse_task_params(job_config, params)
        json_request = json.dumps(job_to_launch)
        if dry_run:
            return json_request
        return post_to_databricks(json_request)

    def inject_vault_token(self):
        if self.cluster_type == "new_cluster":
            if "VAULT_TOKEN" not in self.cluster_config["spark_env_vars"]:  # noqa
                self.cluster_config["spark_env_vars"][
                    "VAULT_TOKEN"
                ] = config.VAULT_TOKEN
