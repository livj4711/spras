import pytest
import sys
from pathlib import Path
from src.util import compare_files
from src.local_neighborhood import LocalNeighborhood

# TODO consider refactoring to simplify the import
# Modify the path because of the - in the directory
SPRAS_ROOT = Path(__file__).parent.parent.parent.absolute()
sys.path.append(str(Path(SPRAS_ROOT, 'docker-wrappers', 'LocalNeighborhood')))
from local_neighborhood import local_neighborhood

TEST_DIR = Path('test', 'LocalNeighborhood/')
OUT_FILE = Path(TEST_DIR, 'output', 'ln-output.txt')


class TestLocalNeighborhood:
    """
    Run the local neighborhood algorithm on the example input files and check the output matches the expected output
    """
    def test_ln(self):
        OUT_FILE.unlink(missing_ok=True)
        local_neighborhood(network_file=Path(TEST_DIR, 'input', 'ln-network.txt'),
                           nodes_file=Path(TEST_DIR, 'input', 'ln-nodes.txt'),
                           output_file=OUT_FILE)
        assert OUT_FILE.exists(), 'Output file was not written'
        expected_file = Path(TEST_DIR, 'expected_output', 'ln-output.txt')
        assert compare_files(OUT_FILE, expected_file), 'Output file does not match expected output file'

    """
    Run the local neighborhood algorithm with a missing input file
    """
    def test_missing_file(self):
        with pytest.raises(OSError):
            local_neighborhood(network_file=Path(TEST_DIR, 'input', 'missing.txt'),
                               nodes_file=Path(TEST_DIR, 'input', 'ln-nodes.txt'),
                               output_file=OUT_FILE)

    """
    Run the local neighborhood algorithm with an improperly formatted network file
    """
    def test_format_error(self):
        with pytest.raises(ValueError):
            local_neighborhood(network_file=Path(TEST_DIR, 'input', 'ln-bad-network.txt'),
                               nodes_file=Path(TEST_DIR, 'input', 'ln-nodes.txt'),
                               output_file=OUT_FILE)

    # Write tests for the Local Neighborhood run function here
    """
    Run test for the Local Neighborhood run function
    """
    def test_localneighborhood_required(self):
        out_path = Path(OUT_FILE)
        out_path.unlink(missing_ok=True)
        # Only include required arguments
        LocalNeighborhood.run(
            network_file=TEST_DIR+'input/ln-network.txt',
            nodes_file=TEST_DIR+'input/ln-nodes.txt',
            output_file=OUT_FILE
        )
        assert out_path.exists()

    """
    Test the expected error is raised when nodes argument is missing
    """
    def test_localneighborhood_missing(self):
        with pytest.raises(ValueError):
            # No nodes
            LocalNeighborhood.run(
                network_file=TEST_DIR + 'input/ln-network.txt',
                output_file=OUT_FILE)

    """
    Test the expected error is raised when network argument is missing
    """
    def test_localneighborhood_missing(self):
        with pytest.raises(ValueError):
            # No network
            LocalNeighborhood.run(
                nodes_file=TEST_DIR+'input/ln-nodes.txt',
                output_file=OUT_FILE)

    # Only run Singularity test if the binary is available on the system
    # spython is only available on Unix, but do not explicitly skip non-Unix platforms
    @pytest.mark.skipif(not shutil.which('singularity'), reason='Singularity not found on system')
    def test_localneighborhood_singularity(self):
        out_path = Path(OUT_FILE)
        out_path.unlink(missing_ok=True)
        # Only include required arguments and run with Singularity
        LocalNeighborhood.run(
            network_file=TEST_DIR+'input/ln-network.txt',
            nodes_file=TEST_DIR+'input/ln-nodes.txt',
            output_file=OUT_FILE
            singularity=True
        )
        assert out_path.exists()

