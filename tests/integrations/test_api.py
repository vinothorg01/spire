import pytest
import spire
from spire.framework.workflows import Workflow
from spire.api import ConnectorException


class TestGet:
    """
    Tests of the functionality of retrieving data about a Workflow or
    Workflow Relationship, as opposed to testing each API function per se
    """

    @staticmethod
    def test_workflow_get(session, wf_def):
        """
        Test the workflow dict output of spire.api.workflow.get
        """
        wf_orig = Workflow.create_default_workflow(**wf_def, session=session)
        session.add(wf_orig)
        wf_orig_dict = wf_orig.to_dict()
        wf_dict = spire.workflow.get(wf_name=wf_def["name"], session=None)
        assert wf_dict == wf_orig_dict

    @staticmethod
    def test_relationship_get(session, wf_def, wf_dataset):
        """
        Test the serialized output returned from querying for a Workflow Relationship

        The Workflow class uses sqlalchemy to create an object relational mapping (ORM)
        between a database and the codebase. The Workflow table joins on various
        relationship tables such as Dataset or Schedule, defined by the Relationship
        mappings, which can be accessed via the ORM by Workflow.dataset. This test uses
        the dataset relationship as an example to test the get_workflow_dataset API
        function. However, this should be effetively the same process for getting any
        other Relationship, so for the time being, only this one test is being written
        as a basis for all Relationships, although we may want to reconsider this later
        """
        Workflow.create_default_workflow(**wf_def, session=session)
        spire.dataset.get(wf_name=wf_def["name"], session=None)
        wf = Workflow.get_by_name(session, wf_def["name"])
        assert wf.dataset.to_dict() == wf_dataset


class TestCreateOrUpdate:
    """
    Tests of the functionality of creating or updating a Workflow or Workflow
    Relationship, as opposed to testing each API function per se
    """

    @staticmethod
    def test_workflow_create(session, wf_def):
        """
        Test the dict of a Workflow created with spire.api.workflow.create
        """
        wf_def["schedule"] = wf_def["schedule"].to_dict()
        wf_orig_dict = spire.workflow.create(**wf_def, session=None)
        wf = Workflow.get_by_name(session, wf_def["name"])
        wf_dict = wf.to_dict()
        assert wf_orig_dict == wf_dict

    @staticmethod
    def test_workflow_update(session, wf_def):
        """
        Test the description of a Workflow updated using spire.api.workflow.update

        This differs from the process of spire.api.<relationship>.update
        functions in that it maps to the Workflow table in the database directly, as
        opposed to joining on one of the relationship tables
        """
        wf_orig = Workflow.create_default_workflow(**wf_def, session=session)
        session.add(wf_orig)
        wf_orig_desc = wf_orig.description
        session.rollback()
        definition = {"description": "test_2 > test_2"}
        spire.workflow.update(
            wf_name=wf_def["name"], definition=definition, session=None
        )
        wf_new = Workflow.get_by_name(session, wf_def["name"])
        new_desc = wf_new.description
        assert definition["description"] == new_desc
        assert wf_orig_desc != new_desc

    @staticmethod
    def test_relationship_update(session, wf_def):
        """
        Test the trait_id of a Workflow.trait updated using spire.api.trait.update

        This differs from the process of spire.workflow.update in that it maps to the
        Trait table in the database which joins in the Workflow table via the ORM
        relationship, as opposed to the Workflow tabe directly. This process should be
        similar for all other relationship update API functions, although we may want
        to add individual tests for all relationships in the future
        """
        Workflow.create_default_workflow(**wf_def, session=session)
        trait_id = 9090
        spire.trait.update(trait_id=trait_id, wf_name=wf_def["name"], session=None)
        wf = Workflow.get_by_name(session, wf_def["name"])
        assert wf.trait.trait_id == trait_id


def test_connector_exception(wf_def):
    """
    Tests whether the ConnectorException was raised

    This custom Exception wraps around a regular Exception, and is only used by the
    session_transaction decorator from the SpireDBConnector in
    spire.integrations.postgres. Therefore, in effect, this test demonstrates that the
    context management works as expected
    """
    wf_def.pop("name", None)
    with pytest.raises(ConnectorException):
        spire.workflow.create(**wf_def)
