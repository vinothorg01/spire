"""drop column - id - for model_outcomes for all schemas other than vogue

Revision ID: 8732744e53c7
Revises: a072f6143829
Create Date: 2020-01-09 14:01:53.863688

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '8732744e53c7'
down_revision = 'a072f6143829'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE allure.model_outcomes DROP COLUMN id')
    op.execute(
        'ALTER TABLE "architectural-digest".model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE "bon-appetit".model_outcomes DROP COLUMN id')
    op.execute(
        'ALTER TABLE "conde-nast-traveler".model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE epicurious.model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE glamour.model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE gq.model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE "new-yorker".model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE pitchfork.model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE self.model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE "teen-vogue".model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE "vanity-fair".model_outcomes DROP COLUMN id')
    op.execute('ALTER TABLE wired.model_outcomes DROP COLUMN id')


def downgrade():
    pass
