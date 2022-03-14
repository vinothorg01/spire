"""added columns - content_type_description - for socialflow_queue_posted_data for all schemas in prod mode

Revision ID: 070870075879
Revises: 500d3b8fda5f
Create Date: 2020-01-02 15:42:43.389831

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '070870075879'
down_revision = '500d3b8fda5f'
branch_labels = None
depends_on = None


def upgrade():
    # Adding the column content_type_description to all socialflow_queue_posted_data table
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='allure')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='bon-appetit')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='epicurious')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='glamour')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='gq')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='new-yorker')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='pitchfork')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='self')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='teen-vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='vanity-fair')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('content_type_description', sa.VARCHAR(), autoincrement=False, nullable=True), schema='wired')


def downgrade():
    pass
