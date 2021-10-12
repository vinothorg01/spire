import pytest
from spire.framework.workflows import Workflow


@pytest.fixture(scope="module")
def dummy_workflows():
    wf1 = Workflow("spire_vendor1_1", description="")
    wf2 = Workflow("spire_vendor1_2", description="")
    wf3 = Workflow("spire_vendor2_3", description="")
    return [wf1, wf2, wf3]
