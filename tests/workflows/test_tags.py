from spire.framework.workflows import Workflow, Tag


def test_add_multiple_tags_to_workflow():
    # setup
    wf = Workflow("test", "test_description")
    tag1 = Tag(label="test_tag_1")
    tag2 = Tag(label="test_tag_2")
    tag3 = Tag(label="test_tag_3")

    # exercise
    wf.add_tag(tag1)
    wf.add_tag(tag2)
    wf.add_tag(tag3)

    # assert
    assert len(wf.tags) == 3


def test_add_multiple_workflows_to_tag():
    # setup
    n_workflows = 3
    wf1 = Workflow(name="test_wf_1", description="none")
    wf2 = Workflow(name="test_wf_2", description="none")
    wf3 = Workflow(name="test_wf_3", description="none")
    tag = Tag(label="test_tag")

    # exercise
    tag.add_workflow(wf1)
    tag.add_workflow(wf2)
    tag.add_workflow(wf3)

    # assert
    assert len(tag.workflows) == n_workflows


def test_remove_tags_from_workflow():
    # setup
    expected_tag_count = 2
    wf = Workflow("test", "test_description")
    tag1 = Tag(label="test_tag_1")
    tag2 = Tag(label="test_tag_2")
    tag3 = Tag(label="test_tag_3")
    wf.add_tag(tag1)
    wf.add_tag(tag2)
    wf.add_tag(tag3)

    # exercise
    wf.remove_tag(tag1)

    # assert
    assert len(wf.tags) == expected_tag_count
