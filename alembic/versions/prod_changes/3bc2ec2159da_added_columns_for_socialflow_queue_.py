"""added columns for socialflow queue posted data for all schemas in prod mode

Revision ID: 3bc2ec2159da
Revises: 7d40a68befd2
Create Date: 2019-09-04 11:59:08.660015

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '3bc2ec2159da'
down_revision = '7d40a68befd2'
branch_labels = None
depends_on = None


def upgrade():
    # Adding the column socialcopy to all socialflow_queue_posted_data table
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='allure')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='bon-appetit')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='epicurious')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='glamour')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='gq')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='new-yorker')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='pitchfork')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='self')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='teen-vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='vanity-fair')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True), schema='wired')

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

    # Adding the column pubdate_diff_anchor_point to all socialflow_queue_posted_data table
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True), schema='gq')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('socialflow_queue_posted_data',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='wired')


def downgrade():
    pass
