import pytest
from typing import Optional
from pydantic import Field
from spire.framework.workflows.job_config import (
    SpireNotebookJobContext,
    JobContext,
)


def test_context_has_default_values():
    SpireNotebookJobContext(workflow_ids=["test"])


def test_context_requires_workflow_ids():
    with pytest.raises(Exception):
        SpireNotebookJobContext()


class DummyJobContext(JobContext):
    a: str = Field(alias="a.a")
    b: Optional[str] = Field(alias="a.b")


class TestJobContext:
    def test_inject_to_dotted_path(self):
        context = DummyJobContext(a="test", b="test")
        config = {}
        injected = context.inject(config)
        assert injected["a"]["a"] == "test"
        assert injected["a"]["b"] == "test"

    def test_inject_ignore_none(self):
        context = DummyJobContext(a="test", b=None)
        config = {}
        injected = context.inject(config)
        assert injected["a"]["a"] == "test"
        assert "b" not in injected["a"]
