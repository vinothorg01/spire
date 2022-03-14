"""drop primary key constraint from - id - for model_outcomes for all schemas in prod mode

Revision ID: 925ebb884422
Revises: a0bc303ef2b1
Create Date: 2020-01-09 13:25:20.709851

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '925ebb884422'
down_revision = 'a0bc303ef2b1'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        'ALTER TABLE allure.model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE \"architectural-digest\".model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE \"bon-appetit\".model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE \"conde-nast-traveler\".model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE epicurious.model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE glamour.model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE gq.model_outcomes drop constraint model_outcomes_pkey')
    op.execute(
        'ALTER TABLE \"new-yorker\".model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE pitchfork.model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE self.model_outcomes drop constraint model_outcomes_pkey')
    op.execute(
        'ALTER TABLE \"teen-vogue\".model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE \"vanity-fair\".model_outcomes drop constraint model_outcomes_pkey'
    )
    op.execute(
        'ALTER TABLE wired.model_outcomes drop constraint model_outcomes_pkey')


def downgrade():
    pass
