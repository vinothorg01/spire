from enum import Enum


class ImplementationTasks(str, Enum):
    """
    An enum class for assigning tasks to implementations. This is currently only
    implemented for DatabricksRunSubmitJob, whereas MLFlowProjectRunJob does not seem
    to be as flexible in the kinds of tasks it supports and doesn't utilize this. It is
    to be determined to what extent these implementation_tasks were created because
    DatabricksRunSubmitJob required them, vs. because the tasks using this
    implementation require them.

    If the latter is the case, then this will need to be utilized by other
    implementations in the future. If this is not the case, it can be folded into
    databricks_run_submit instead.
    """

    # Inherit string so that we can do
    # ImplementationTasks.NOTEBOOK_TASK == "notebook_task"
    NOTEBOOK = "notebook_task"
    SPARK_JAR = "spark_jar_task"
    SPARK_PYTHON = "spark_python_task"
    SPARK_SUBMIT = "spark_submit_task"

    def __str__(self) -> str:
        """
        Use the value as this Enum string representation for easy transition
        from string based value to Enum based.

        e.g.
        str(ImplementationTasks.NOTEBOOK_TASK) == str('notebook_task')

        As with WorkflowStages, this is necessary because the implementation task code
        was written for strings

        """
        return self.value
