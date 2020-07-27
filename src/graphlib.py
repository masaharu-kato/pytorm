from typing import List, Dict, Set, Hashable, Optional
from itertools import chain

Node = Hashable


class Graph:
    def __init__(self, edges:Dict[Node, Set[Node]]):
        self._edges = edges
        self.roots = set(edges.keys()) - set(chain(*edges.values()))


    def edges(self, node:Node) -> Set[Node]:
        return self._edges[node] if node in self._edges else set()


    def subgraph(self, /, targets:Optional[Set[Node]] = None, start:Optional[Node] = None) -> 'Graph':
        res_graph = {}
        if targets is not None:
            checked = set()
            self._out_subgraph_with_targets(targets, start, res_graph, checked)
        else:
            self._out_subgraph(start, res_graph)
        return res_graph


    def sorted(self) -> 'Graph':
        return self.subgraph()


    def _out_subgraph(self, node:Optional[Node], out:'Graph') -> None:
        if node in out: return
        out_lnodes = self.edges(node) if node is not None else self.roots
        for lnode in out_lnodes:
            self._out_subgraph(lnode, out)
        out[node] = out_lnodes


    def _out_subgraph_with_targets(self, target_nodes:Set[Node], cnode:Optional[Node], out:Dict[Node, Set[Node]], checked:Set[Node]) -> None:
        if cnode in checked: return
        lnodes = self.edges(cnode) if cnode is not None else self.roots
        for lnode in lnodes:
            self._out_subgraph_with_targets(target_nodes, lnode, out, checked)
        out_lnodes = [lnode for lnode in lnodes if lnode in out]
        # print(cnode, out_lnodes)
        if cnode in target_nodes or out_lnodes:
            out[cnode] = set(out_lnodes)
        checked.add(cnode)
        

    def groups(self) -> List['Graph']:
        return [self.subgraph(start=root_node) for root_node in self.roots]





