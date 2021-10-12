from spire.integrations import connector


def test_session_scoping():
    scoped_session = connector.make_scoped_session()
    second_scoped_session = connector.make_scoped_session()
    unscoped_session = connector.make_session()
    second_unscoped_session = connector.make_session()
    assert scoped_session == second_scoped_session
    assert not scoped_session == unscoped_session
    assert not unscoped_session == second_unscoped_session
