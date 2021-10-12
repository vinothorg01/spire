"""convert arg_date to date type

Revision ID: 9713d3159095
Revises: 9a18fa4a434f
Create Date: 2021-05-19 15:55:16.150688

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '9713d3159095'
down_revision = '9a18fa4a434f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('workflow_latest_history',
    sa.Column('workflow_id', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('arg_date', sa.Date(), nullable=False),
    sa.Column('stage', sa.Enum('TRAINING', 'SCORING', 'ASSEMBLY', 'POSTPROCESSING', name='workflowstages', native_enum=False), nullable=False),
    sa.Column('history_id', postgresql.UUID(as_uuid=True), nullable=True),
    sa.ForeignKeyConstraint(['history_id'], ['history.id'], ),
    sa.ForeignKeyConstraint(['workflow_id'], ['workflows.id'], ),
    sa.PrimaryKeyConstraint('workflow_id', 'arg_date', 'stage'),
    sa.UniqueConstraint('history_id')
    )
    op.add_column('history', sa.Column('cluster_id', sa.String(length=100), nullable=True))
    op.add_column('history', sa.Column('modified_at', sa.DateTime(), nullable=True))
    op.add_column('history', sa.Column('run_id', sa.String(length=100), nullable=True))
    op.alter_column('history', 'arg_date',
               existing_type=postgresql.TIMESTAMP(),
               nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('history', 'arg_date',
               existing_type=postgresql.TIMESTAMP(),
               nullable=True)
    op.drop_column('history', 'run_id')
    op.drop_column('history', 'modified_at')
    op.drop_column('history', 'cluster_id')
    op.drop_table('workflow_latest_history')
    # ### end Alembic commands ###
