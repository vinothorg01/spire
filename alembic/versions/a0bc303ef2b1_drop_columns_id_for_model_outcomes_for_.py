"""drop columns - id - for model_outcomes for all schemas in prod mode

Revision ID: a0bc303ef2b1
Revises: 070870075879
Create Date: 2020-01-09 12:29:51.492682

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'a0bc303ef2b1'
down_revision = '070870075879'
branch_labels = None
depends_on = None


def upgrade():
    # Altering the column id for all model_outcomes table
    op.execute(
        'ALTER TABLE vogue.model_outcomes drop constraint model_outcomes_pkey')


def downgrade():
    pass
