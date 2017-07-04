from collections import namedtuple
import json
import numpy as np
import time
import datetime
from collections import OrderedDict
from heapq import merge

"""
Solution to Insight Data Engineering challenge:
https://github.com/InsightDataScience/anomaly_detection
"""


Purchase = namedtuple('Purchase', ['id', 'time', 'amount'])
def purchases_mean(purchases):
    return np.mean([p.amount for p in purchases])
def purchases_std(purchases):
    return np.std([p.amount for p in purchases])

class Network:
    def __init__(self, T, D):
        self.T = int(T)
        self.D = int(D)
        self.graph = {}
        self.purchases = {} # map user id to chronologically-ordered list of purchases

    def process_event(self, event, output_stream = True):
        """
        event : dict (str -> str)
            Event to handle
        output_stream : bool
            If True, generate json output for anomalous events
        
        This method is the public interface to Network instances. 
        """
        evt_type = event['event_type']
        if evt_type == 'purchase':
            self._process_purchase(event['id'], event['timestamp'], float(event['amount']), output_stream = output_stream)
        elif evt_type == 'befriend':
            self._process_befriend(event['id1'], event['id2'])
        elif evt_type == 'unfriend':
            self._process_unfriend(event['id1'], event['id2'])
        else:
            raise ValueError('Invalid event type: %s' % evt_type)

    def _get_purchases(self, ids):
        result = []
        purchase_lists = [self.purchases.setdefault(id, []) for id in ids]
        merged = ordered_purchases(purchase_lists)
        for i, elt in zip(range(self.T), merged):
            result.append(elt)
        return result

    def _handle_streaming_purchase(self, purchase):
        """
        Helper function for handling a streaming purchase event. If the
        given purchase is identified as anomalous, an output event is
        generated.
        """
        in_neighborhood = self._purchases_in_neighborhood(purchase.id)
        mean = purchases_mean(in_neighborhood)
        std = purchases_std(in_neighborhood)
        if purchase.amount > mean + 3 * std:
            time_str = datetime.datetime.fromtimestamp(time.mktime(purchase.time)).strftime("%Y-%m-%d %H:%M:%S")

            output_str = json.dumps(OrderedDict([("event_type", "purchase"), ("timestamp", time_str), ("id", purchase.id),
                    ("amount", str(purchase.amount)), ("mean", format(mean, '.2f')), ("sd", format(std, '.2f'))]))

            output.write(output_str + '\n')


    def _process_purchase(self, id, timestamp, amount, output_stream = True):
        purchases = self.purchases
        purchase = Purchase(id, time.strptime(timestamp, "%Y-%m-%d %H:%M:%S"), amount)
        if output_stream:
            self._handle_streaming_purchase(purchase)
        id_purchases = purchases.setdefault(id, [])
        id_purchases.append(
               purchase 
            )

    def _process_befriend(self, source, target):
        """
        source : int
            id of the user initiating the friend addition
        target : int
            id of the user being friended
        
        This function adds the new friend relationship to graph.
        """
        source_friends = self.graph.setdefault(source, set())
        source_friends.add(target)
        target_friends = self.graph.setdefault(target, set())
        target_friends.add(source)

    def _process_unfriend(self, source, target):
        """
        source : int
            id of the user initiating the friend deletion
        target : int
            id of the user being unfriended
        
        This function removes a friend relationship from graph.
        """
        source_friend = self.graph[source]
        source_friend.remove(target)
        target_friend = self.graph[target]
        target_friend.remove(source)


    def _purchases_in_neighborhood(self, id):
        """
        Return the last self.T purchases by users within distance self.D
        of the given user id.
        """
        subgraph = get_subgraph(self.graph, id, self.D)
        friend_ids = set(subgraph.keys()) - set([id])
        return self._get_purchases(friend_ids)

def ordered_purchases(purchase_lists):
    """
    Given a list of iterables, each of which is sorted in descending
    order, return a generator that yields the merged values in correct
    order.
    """
    return merge(*purchase_lists, reverse = True)

def get_subgraph(graph, id, degree, partial_graph = None):
    """
    Given an undirected graph, represented as a dict (int -> set of
    ints), return a subgraph including nodes within distance degree of
    the given id (inclusive). This function is used to pull a user's
    entourage out of the global social network graph.
    """
    if partial_graph is None:
        partial_graph = {}
    if degree == 0:
        return partial_graph
    vertex = partial_graph.setdefault(id, set())
    for neighbor in graph[id]:
        vertex.add(neighbor)
        neighbor_set = partial_graph.setdefault(neighbor, set())
        neighbor_set.add(id)
        # recurse on this neighboring node. Note that we risk visiting a
        # given node many times (optimization opportunity!)
        get_subgraph(graph, neighbor, degree - 1, partial_graph = partial_graph)
    return partial_graph

output = open('log_output/flagged_purchases.json', 'w')

def main():
    with open('log_input/batch_log.json', 'r') as f:
        config = json.loads(f.readline())
        network = Network(config['T'], config['D'])
        log_events = f.read().split('\n')
    for event in log_events:
        if event:# handle empty input lines
            network.process_event(json.loads(event), output_stream = False)

    with open('log_input/stream_log.json', 'r') as f:
        stream_events = f.read().split('\n')
    for event in stream_events:
        if event:# handle empty input lines
            network.process_event(json.loads(event), output_stream = True)
    output.close()

if __name__ == '__main__':
    main()
