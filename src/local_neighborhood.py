from src.prm import PRM
from pathlib import Path
from src.util import prepare_volume, run_container
from src.dataset import contains_node_columns, request_node_columns

import pandas as pd

__all__ = ['LocalNeighborhood']

class LocalNeighborhood(PRM):
    required_inputs = ['network', 'nodes']

    @staticmethod
    def generate_inputs(data, filename_map):
        """
        Access fields from the dataset and write the required input files
        @param data: dataset
        @param filename_map: a dict mapping file types in the required_inputs to the filename for that type
        @return:
        """
        for input_type in LocalNeighborhood.required_inputs:
            if input_type not in filename_map:
                raise ValueError(f"{input_type} filename is missing"

        if data.contains_node_columns(['prize']):
            prizes_df = data.request_node_columns(['prize'])
            node_df_pr = prizes_df.loc[complete.cases(prizes_df), 'NODEID']
            if data.contains_node_columns(['sources','targets']):
                sources_targets = data.request_node_columns(['sources', 'targets']
                node_df_st = sources_targets.loc[(sources_targets['sources'] == True) | (sources_targets['targets'] == True), 'NODEID']
                node_df = (pd.concat([node_df_pr, node_df_st])).drop_duplicates()
            else:
                node_df = node_df_pr
        elif data.contains_node_columns(['sources','targets']):
            sources_targets = data.request_node_columns(['sources', 'targets'])
            node_df = sources_targets.loc[(sources_targets['sources'] == True) | (sources_targets['targets'] == True), 'NODEID']
        else:
            raise ValueError("LocalNeighborhood requires node prizes or sources and targets")
        node_df.to_csv(filename_map['nodes'],sep='\t',index=False,columns=['NODEID'],header=['node'])

        # Only need <vertex1> | <vertex2>
        edges_df = data.get_interactome()
        edges_df.to_csv(filename_map['network'],sep='|',index=False,columns=['Interactor1','Interactor2'],header=['protein1','protein2'])

    @staticmethod
    def run(network=None, nodes=None, output_file=None, singularity=False):
        """
        Run LocalNeighborhoods with Docker
        @param network:  input network file (required)
        @param nodes:  input node types with prizes or sources and targets (required)
        @param output_file: path to the output pathway file (required)
        @param singularity: if True, run using the Singularity container instead of the Docker container
        """
        if not network or not nodes or not output_file:
            raise ValueError('Required LocalNeighborhood arguments are missing')

        work_dir = '/spras'

        # Each volume is a tuple (src, dest)
        volumes = list()

        bind_path, network_file = prepare_volume(network, work_dir)
        volumes.append(bind_path)

        bind_path, node_file = prepare_volume(nodes, work_dir)
        volumes.append(bind_path)

        bind_path, mapped_output_file = prepare_volume(str(output_file), work_dir)
        volumes.append(bind_path)

        command = ['python',
                   '/LocalNeighborhood/local_neighborhood.py',
                   '--network_file', network_file,
                   '--node_file', node_file,
                   '--output', mapped_output_file]

        print('Running LocalNeighborhood with arguments: {}'.format(' '.join(command)), flush=True)

        container_framework = 'singularity' if singularity else 'docker'
        out = run_container(container_framework,
                            'otjohnson/local-neighborhood',
                            command,
                            volumes,
                            work_dir)
        print(out)

    @staticmethod
    def parse_output(raw_pathway_file, standardized_pathway_file):
        """
        Convert a predicted pathway into the universal format
        @param raw_pathway_file: pathway file produced by an algorithm's run function
        @param standardized_pathway_file: the same pathway written in the universal format
        """

        df = pd.read_csv(raw_pathway_file, sep='|', header=None)
        df['rank'] = 1 # adds in a rank column of 1s because the edges are not ranked
        df.to_csv(standardized_pathway_file, header=False, index=False, sep='\t')
