"""drop column - id - for model_outcomes for all schemas

Revision ID: a072f6143829
Revises: 925ebb884422
Create Date: 2020-01-09 13:55:00.383864

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'a072f6143829'
down_revision = '925ebb884422'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE vogue.model_outcomes DROP COLUMN id')


def downgrade():
    pass
