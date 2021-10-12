import pytest
import datetime
import getpass

from spire.framework.workflows import Workflow


def reformat(schema):
    return schema.replace(" ", "").replace("\n", "")


def test_trait_schema_setter():
    # setup
    wf = Workflow("test", "test_description")
    expected_schema = "\n            {\n                workflowId\n                isWorkflowEnabled\n                isWorkflowTrained\n                traits {\n                    traitId\n                    name\n                    vendor\n                    type\n                    attribs\n                    configuration\n                    createdBy\n                    dateCreated\n                    dateUpdated\n                    signals\n                    segments {\n                        segment {\n                            segmentId\n                            systemCode\n                            technicalStatus\n                            isActive\n                            segmentSize\n                        }\n                    }\n                }\n            }\n                "

    # exercise
    schema = wf.SCHEMA

    # assert
    assert reformat(schema) == reformat(expected_schema)


def test_get_query_parser():
    # setup
    wf = Workflow("test", "test_description")
    workflow_id = str(wf.id)
    expected_query = f'query {{ workflow(workflowId: "{workflow_id}" ) \n            {{\n                workflowId\n                isWorkflowEnabled\n                isWorkflowTrained\n                traits {{\n                    traitId\n                    name\n                    vendor\n                    type\n                    attribs\n                    configuration\n                    createdBy\n                    dateCreated\n                    dateUpdated\n                    signals\n                    segments {{\n                        segment {{\n                            segmentId\n                            systemCode\n                            technicalStatus\n                            isActive\n                            segmentSize\n                        }}\n                    }}\n                }}\n            }}\n                 }}'

    # exercise
    query = wf._parse_get_query()

    # assert
    assert reformat(query) == reformat(expected_query)


def test_update_request_parser():
    # setup
    trait_id = 666
    user = getpass.getuser()
    wf = Workflow("test", "test_description")

    workflow_id = str(wf.id)
    expected_request_query = f"mutation {{ updateWorkflow(request: {{'workflowId': '\"{workflow_id}\"', 'updatedBy': '\"{user}\"', 'traitIds': '\"{trait_id}\"'}} ) \n            {{\n                workflowId\n                isWorkflowEnabled\n                isWorkflowTrained\n                traits {{\n                    traitId\n                    name\n                    vendor\n                    type\n                    attribs\n                    configuration\n                    createdBy\n                    dateCreated\n                    dateUpdated\n                    signals\n                    segments {{\n                        segment {{\n                            segmentId\n                            systemCode\n                            technicalStatus\n                            isActive\n                            segmentSize\n                        }}\n                    }}\n                }}\n            }}\n                 }}"

    # exercise
    request_query = wf._parse_mutation(wf.UPDATE, trait_id=trait_id)

    # assert
    assert reformat(request_query) == reformat(expected_request_query)


def test_formatting_helpers():
    # setup
    wf = Workflow("test", "test_description")
    workflow_id = str(wf.id)

    # iso formatting
    test_date = datetime.datetime(2020, 4, 27, 22, 44, 10, 869050)
    expected_date = "2020-04-27T22:44:10.869050Z"

    # graphql boolean parsing
    test_data = f"request: {{workflowId: {workflow_id}, isWorkflowEnabled: " "true" "}"
    expected_test_data = (
        f"request: {{workflowId: {workflow_id}, isWorkflowEnabled: true}}"
    )

    # backfill validation
    invalid_backfill_date = datetime.date(2020, 4, 20)
    valid_backfill_date = datetime.datetime(2020, 4, 27, 23, 13, 28, 943031)

    # exercise
    iso_format_date = wf._parse_date_to_iso(test_date)
    graphql_bools = wf._format_bools(test_data)

    # assert
    assert iso_format_date == expected_date
    assert graphql_bools == expected_test_data
    with pytest.raises(TypeError):
        wf._validate_backfill_arg(**{"backfill_datetime": invalid_backfill_date})
    with pytest.raises(TypeError):
        wf.commit_to_skittles(666, invalid_backfill_date)
