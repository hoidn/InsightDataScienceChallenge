import process_log

def test_get_subgraph():
    graph = {1: set([2, 3]), 2: set([1, 3]), 3: set([1, 2, 4]), 4: set([3]), 4: set([5]), 5: set([4])}
    subgraph = {1: set([2, 3]), 2: set([1, 3]), 3: set([1, 2, 4]), 4: set([3])}
    result = process_log.get_subgraph(graph, 1, 2)
    assert result == subgraph
