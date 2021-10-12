import pytest
from spire.framework.workflows import DFPOrder
from datetime import date


def test_dfp_order(session):
    # setup
    order = DFPOrder(advertiser="test", order_id=1, start_date=date.today())
    # exercise
    session.add(order)
    session.commit()

    order_2 = session.query(DFPOrder).filter(DFPOrder.advertiser == "test").first()

    # autoincrement
    assert order_2.id == 1
    # default to True
    assert order_2.active == True
    # we get the same order back
    assert order_2 == order
