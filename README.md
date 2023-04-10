# Searchkit

Python library providing tools to perform searches on files in parallel.

## Search Types

Differest types of search are supported. Add one or more search definition to a `FileSearcher` object, registering them against a file, directory or glob path. Results are collected and returned as a `SearchResultsCollection` which provides different ways to retrieve results.

### Simple Search

Uses the `SearchDef` class and supports matching one or more patterns against each line in a file. Patterns are executed until the first match is found.

### Sequence Search

Uses the `SequenceSearchDef` class and supports matching strings over multiple lines by matching a start, end and optional body in between.

## Installation

searchkit is packaged in [pypi](https://pypi.org/project/searchkit) and can be installed as follows:

```console
sudo apt install python3-pip
pip install searchkit
```

## Example Usage

An example simple search is as follows:

```
from searchkit import FileSearcher, SearchDef

fname = 'foo.txt'
open(fname, 'w').write('the quick brown fox')
fs = FileSearcher()
fs.add(SearchDef(r'.+ \S+ (\S+) .+'), fname)
results = fs.run()
for r in results.find_by_path(fname):
    print(r.get(1))
```

An example sequence search is as follows:

```
from searchkit import FileSearcher, SequenceSearchDef, SearchDef

content = """
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ValueError: invalid literal for int() with base 10: 'foo'"""

fname = 'my.log'
open(fname, 'w').write(content)

start = SearchDef(r'Traceback')
body = SearchDef(r'.+')
# terminate sequence with start of next or EOF so no end def needed.

fs = FileSearcher()
fs.add(SequenceSearchDef(start, tag='myseq', body=body), fname)
results = fs.run()
for seq, results in results.find_sequence_by_tag('myseq').items():
    for r in results:
        if 'body' in r.tag:
            print(r.get(0))
```
