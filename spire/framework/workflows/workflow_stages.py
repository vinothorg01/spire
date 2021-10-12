from enum import Enum


class WorkflowStages(str, Enum):
    # Inherit string so that we can do Workflow.Stages.TRAINING == "training"
    TRAINING = "training"
    SCORING = "scoring"
    ASSEMBLY = "assembly"
    POSTPROCESSING = "postprocessing"

    def __str__(self) -> str:
        return self.value
