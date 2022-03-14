"""added columns for model outcomes for all schemas in dev mode

Revision ID: 9292c28027f9
Revises: 284c5c4d6bd6
Create Date: 2019-09-03 17:36:42.768286

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9292c28027f9'
down_revision = '284c5c4d6bd6'
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


def downgrade():
    pass
