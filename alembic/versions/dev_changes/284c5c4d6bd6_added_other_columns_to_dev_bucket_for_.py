"""added other columns to dev bucket for socialflow_queue_posted_data table

Revision ID: 284c5c4d6bd6
Revises: 167242ce65a1
Create Date: 2019-09-03 17:33:03.399179

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '284c5c4d6bd6'
down_revision = '167242ce65a1'
branch_labels = None
depends_on = None


def upgrade():
    # Adding the column socialcopyfb_created_epoch_time to all socialflow_queue_posted_data table
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='gq')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='wired')

    # Adding the column article_recency_when_accessed to all socialflow_queue_posted_data table
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='gq')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='wired')


def downgrade():
    pass
