# Searchkit

Python library providing tools to perform searches on files in parallel.

## Search Library

The following classes are provided:

### FileSearcher

The main search engine. Register one or more [SearchDef](#SearchDef) objects against one or more path then execute in parallel.

### SearchDef

A simple search definition. Can be tagged to easily retrieve results.

### SequenceSearchDef

A multi-line search definition that takes into account sequences by matching start, body and end.

### SearchResultsCollection

A collection of search results that can be queried in a number of ways for easy retrieval.

## Installation

searchkit is packaged in PyPI and can be installed as follows:

```console
sudo apt install python3-pip
pip install searchkit
```

## Example Usage

```
from searchkit import FileSearcher, SearchDef

with open('foo', 'w') as fd:
    fd.write('the quick brown fox')

fs = FileSearcher()
fs.add(SearchDef(r'.+ \S+ (\S+) .+'), fd.name)
results = fs.run()
for r in results.find_by_path(fd.name):
    print(r.get(1))
```
