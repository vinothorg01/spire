"""add job config

Revision ID: aa298549a3cd
Revises: f605657eaa40
Create Date: 2021-03-01 17:51:46.294745

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "aa298549a3cd"
down_revision = "f605657eaa40"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "job_configs",
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("modified_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("name"),
    )
    op.create_table(
        "workflow_jobconfigs",
        sa.Column("workflow_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_config_name", sa.String(), nullable=False),
        sa.Column(
            "stage",
            sa.Enum(
                "TRAINING",
                "SCORING",
                "ASSEMBLY",
                "POSTPROCESSING",
                name="workflowstages",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["job_config_name"],
            ["job_configs.name"],
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflows.id"],
        ),
        sa.PrimaryKeyConstraint("workflow_id", "job_config_name", "stage"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("workflow_jobconfigs")
    op.drop_table("job_configs")
    # ### end Alembic commands ###
