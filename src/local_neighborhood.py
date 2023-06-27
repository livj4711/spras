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

        if data.contains_node_columns('prize') or data.containse_node_columns(['sources','targets']):
            #NODEID is always included in the node table
            node_df = data.loc[:,'NODEID']
        else:
            raise ValueError("LocalNeighborhood requires node prizes or sources and targets")

        node_df.to_csv(filename_map['nodes'],sep='\t',index=False,columns=['NODEID'],header=['node'])

        #Only need <vertex1> | <vertex2>
        edges_df = data.get_interactome()
        edges_df.to_csv(filename_map['network'],sep='|',index=False,columns=['Interactor1','Interactor2'],header=['protein1','protein2'])
