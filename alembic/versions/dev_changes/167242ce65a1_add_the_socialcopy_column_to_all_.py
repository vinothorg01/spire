"""add the socialcopy column to all schemas for socialflow_queue_posted_data

Revision ID: 167242ce65a1
Revises: 4783045f0ea6
Create Date: 2019-09-03 16:54:32.811202

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '167242ce65a1'
down_revision = '4783045f0ea6'
branch_labels = None
depends_on = None


def upgrade():
    # Adding the column socialcopy to all socialflow_queue_posted_data table
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='allure')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='architectural-digest')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='conde-nast-traveler')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='bon-appetit')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='epicurious')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='glamour')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='gq')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='new-yorker')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='pitchfork')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='self')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='teen-vogue')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='vanity-fair')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='vogue')
    op.add_column('socialflow_queue_posted_data', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='wired')



def downgrade():
    pass
