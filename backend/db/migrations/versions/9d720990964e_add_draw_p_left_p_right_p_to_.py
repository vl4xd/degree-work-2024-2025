"""add draw_p left_p right_p to PredictionDrawLeftRight

Revision ID: 9d720990964e
Revises: e427dc538d45
Create Date: 2025-05-12 01:10:59.973037

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9d720990964e'
down_revision: Union[str, None] = 'e427dc538d45'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('prediction_draw_left_right', sa.Column('draw_p', sa.Float(), nullable=False))
    op.add_column('prediction_draw_left_right', sa.Column('left_p', sa.Float(), nullable=False))
    op.add_column('prediction_draw_left_right', sa.Column('right_p', sa.Float(), nullable=False))
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('prediction_draw_left_right', 'right_p')
    op.drop_column('prediction_draw_left_right', 'left_p')
    op.drop_column('prediction_draw_left_right', 'draw_p')
    # ### end Alembic commands ###
