# DOMINO Docker image

A Docker image for [DOMINO](https://github.com/Shamir-Lab/DOMINO)

To create the Docker image run:
```
docker build -t otjohnson/domino -f Dockerfile .
```
from this directory.

To inspect the installed Python packages:
```
winpty docker run otjohnson/domino pip list
```
The `winpty` prefix is only needed on Windows.
