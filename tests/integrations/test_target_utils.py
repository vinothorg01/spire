from spire.targets.utils import dfp_load_order_ids
from spire.framework.workflows import DFPOrder


def test_load_order_ids(session):
    order_1 = DFPOrder(advertiser="a", order_id=1)
    order_2 = DFPOrder(advertiser="b", order_id=2)
    order_3 = DFPOrder(advertiser="c", order_id=3, active=False)
    session.add(order_1)
    session.add(order_2)
    session.add(order_3)
    session.commit()

    order_ids = list(sorted(dfp_load_order_ids()))
    assert len(order_ids) == 2
    assert order_ids[0] == 1
    assert order_ids[1] == 2
