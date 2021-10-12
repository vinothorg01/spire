from spire.utils.general import chunk_into_groups


def test_chunk_into_groups():
    tasks = [1]
    result = chunk_into_groups(tasks, 20)
    assert len(result) == 1
    import sys

    tasks = list(range(21))
    result = chunk_into_groups(tasks, 20)
    assert len(result) == 2
    assert len(result[0]) == 20
    assert len(result[1]) == 1
