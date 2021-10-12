from typing import List
from .spire_notebook_job import SpireNotebookJobConfig
from spire.utils.general import group_by_vendor, chunk_into_groups


class SpireVendorNotebookJobConfig(SpireNotebookJobConfig):
    __mapper_args__ = {"polymorphic_identity": "SpireVendorNotebookJobConfig"}
    MAX_WF_PER_CLUSTER = 25

    def group_workflows(self, workflows) -> List["Workflow"]:  # noqa
        vendor_dict = group_by_vendor(workflows)
        groups = []
        for _, wfs in vendor_dict.items():
            vendor_groups = chunk_into_groups(wfs, self.MAX_WF_PER_CLUSTER)
            groups.extend(vendor_groups)

        return groups
