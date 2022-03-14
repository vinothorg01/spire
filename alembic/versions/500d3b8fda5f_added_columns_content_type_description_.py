"""added columns - content_type_description - for model outcomes for all schemas in prod mode

Revision ID: 500d3b8fda5f
Revises: 3bc2ec2159da
Create Date: 2020-01-02 15:29:02.867330

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '500d3b8fda5f'
down_revision = '3bc2ec2159da'
branch_labels = None
depends_on = None


def upgrade():
    # Adding the column socialcopy to all model_outcomes table
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='gq')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('model_outcomes', sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='wired')

def downgrade():
    pass
