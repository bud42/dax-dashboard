#https://networkx.org/documentation/stable/tutorial.html

import networkx as nx
import matplotlib.pyplot as plt

# Build the graph
graph = nx.DiGraph()
graph.add_nodes_from(['T1', 'FLAIR', 'fMRI_REST', 'fMRI_MSIT'])
graph.add_node('FS7_v1')
graph.add_edge('T1', 'FS7_v1')
graph.add_edge('T1', 'SAMSEG_v1')
graph.add_edge('FLAIR', 'SAMSEG_v1')
graph.add_edge('T1', 'struct_preproc_v1')
graph.add_edge('FLAIR', 'struct_preproc_v1')
graph.add_edge('fMRI_REST', 'fmri_rest_v1')
graph.add_edge('struct_preproc_v1', 'fmri_rest_v1')

# Draw it
fig = plt.figure(figsize=(8, 5))
nx.draw(graph, with_labels=True, font_weight='bold')
#pos = nx.spring_layout(graph)
#nx.draw_networkx_nodes(graph, pos, alpha=0)
#nx.draw_networkx_labels(graph, pos, font_size=12)

# Save to file
plt.savefig('networkx.png')
