import pydot

# Build the graph
graph = pydot.Dot(graph_type='digraph')

graph.set_node_defaults(
    color='lightblue',
    style='filled',
    shape='box',
    fontname='Courier',
    fontsize='12')

scantypes = ['T1', 'FLAIR', 'fMRI_REST', 'fMRI_MSIT', 'FieldMaps', 'DTI']

for scan in scantypes:
    graph.add_node(pydot.Node(scan, color='orange'))

graph.add_node(pydot.Node('EDAT', color='violet'))

graph.add_edge(pydot.Edge('EDAT', 'fmri_msit_v2'))
graph.add_edge(pydot.Edge('T1', 'FS7_v1'))
graph.add_edge(pydot.Edge('FS7_v1', 'SAMSEG_v1'))
graph.add_edge(pydot.Edge('FS7_v1', 'FS7-HPCAMG_v1'))
graph.add_edge(pydot.Edge('FLAIR', 'SAMSEG_v1'))
graph.add_edge(pydot.Edge('T1', 'struct_preproc_v1'))
graph.add_edge(pydot.Edge('FLAIR', 'struct_preproc_v1'))
graph.add_edge(pydot.Edge('fMRI_REST', 'fmri_rest_v2'))
graph.add_edge(pydot.Edge('struct_preproc_v1', 'fmri_rest_v2'))
graph.add_edge(pydot.Edge('FieldMaps', 'fmri_rest_v2'))
graph.add_edge(pydot.Edge('fMRI_MSIT', 'fmri_msit_v2'))

graph.write_png('graph.png')
