"""added columns for model outcomes for all schemas in prod mode

Revision ID: 7d40a68befd2
Revises: ecccbdbe98d7
Create Date: 2019-09-04 11:25:33.596883

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '7d40a68befd2'
down_revision = 'ecccbdbe98d7'
branch_labels = None
depends_on = None


def upgrade():
    # Adding the column socialcopy to all model_outcomes table
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='gq')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('model_outcomes', sa.Column('socialcopy', sa.VARCHAR(), autoincrement=False, nullable=True),
                  schema='wired')

    # Adding the column socialcopyfb_created_epoch_time to all model_outcomes table
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='gq')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('model_outcomes',
                  sa.Column('socialcopyfb_created_epoch_time', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='wired')

    # Adding the column article_recency_when_accessed to all model_outcomes table
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='gq')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('model_outcomes',
                  sa.Column('article_recency_when_accessed', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='wired')

    # Adding the column pubdate_diff_anchor_point to all model_outcomes table
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='allure')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='architectural-digest')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='conde-nast-traveler')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='bon-appetit')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='epicurious')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='glamour')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True), schema='gq')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='new-yorker')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='pitchfork')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='self')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='teen-vogue')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vanity-fair')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='vogue')
    op.add_column('model_outcomes',
                  sa.Column('pubdate_diff_anchor_point', sa.INTEGER(), autoincrement=False, nullable=True),
                  schema='wired')


def downgrade():
    pass
