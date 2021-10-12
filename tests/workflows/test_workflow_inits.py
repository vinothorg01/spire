import os

ENVIRONMENT = os.environ.get("DEPLOYMENT_ENV", "test")


def test_cluster_init():
    # setup
    from spire.framework.workflows.cluster import ClusterStatus

    # assert
    assert locals().get(ClusterStatus.__name__) is not None
