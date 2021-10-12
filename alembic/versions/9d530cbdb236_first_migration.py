"""first migration

Revision ID: 9d530cbdb236
Revises: 
Create Date: 2020-10-26 16:49:07.937877

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from pathlib import Path

# revision identifiers, used by Alembic.
revision = '9d530cbdb236'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    sql_path = Path(__file__).parent.parent/'spire_v3_schema.sql'
    with open(sql_path, 'r') as f:
        sql = f.read()
        op.execute(sql)


def downgrade():
    pass
