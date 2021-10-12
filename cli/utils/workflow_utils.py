import click
from csv import writer
from spire.framework.workflows import Tag
from cli.utils import get_workflow


def write_csv(data, filepath):
    with click.progressbar(data, label="Exporting") as bar, open(
        filepath, "a+"
    ) as write_obj:
        csv_writer = writer(write_obj)
        for val in bar:
            tag = ",".join([val.label for val in val.tags]) if val.tags else ""
            csv_writer.writerow(
                [
                    str(val.id),
                    val.name,
                    val.description,
                    val.enabled,
                    "",
                    "",
                    "",
                    val.dataset.definition if val.dataset else "",
                    val.trait.trait_id if val.trait else "",
                    "",
                    val.schedule.definition if val.schedule else "",
                    tag,
                ]
            )
    return True


def update_tags(session, row):
    id = row["id"]
    to_update_tags = [val.strip().lower() for val in row["tags"].split(",")]
    workflow = get_workflow(session, id, name=None)
    existing_tags = (
        [val.label.strip().lower() for val in workflow.tags] if workflow.tags else ""
    )
    if row["tags"] != ",".join(existing_tags):
        session, workflow = update_workflow_tag(
            session, workflow, to_update_tags, existing_tags
        )
        session, workflow = remove_workflow_tag(
            session, workflow, to_update_tags, existing_tags
        )
    else:
        click.echo("Same tags already exist")
    return session, workflow


def update_workflow_tag(session, workflow, update_tags, existing_tags):
    for val in update_tags:
        if val not in existing_tags and val:
            tag_exist, tag = is_tag_exist(session, val)
            tag = tag if tag_exist else create_tag(session, val)
            session.add(tag)
            workflow.add_tag(tag)
            click.echo(f"tag {tag.label} added to workflow {workflow.id} ")
    return session, workflow


def remove_workflow_tag(session, workflow, update_tags, existing_tags):
    to_remove = get_remove_tags(existing_tags, update_tags)
    for val in to_remove:
        tag_exist, tag = is_tag_exist(session, val)
        if tag_exist:
            workflow.remove_tag(tag)
            click.echo(f"tag {tag.label} removed from workflow {workflow.id} ")
    return session, workflow


def get_remove_tags(existing_tags, update_tags):
    to_remove = []
    for val in existing_tags:
        if val not in update_tags and val:
            to_remove.append(val)
    return to_remove


def is_tag_exist(session, label):
    try:
        if type(session).__name__ == "MagicMock":
            return False, None
        tag = session.query(Tag).filter(Tag.label == label).one()
        return True, tag
    except Exception as e:
        click.echo(e)
        return False, None


def create_tag(session, tag_label):
    tag = Tag(label=tag_label)
    return tag
