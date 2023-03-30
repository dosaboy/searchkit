# Searchkit

Python library providing tools to search files in parallel.

## Search Types

Different types of search are supported. Add one or more search definition to a `FileSearcher` object, registering them against a file, directory or glob path. Results are collected and returned as a `SearchResultsCollection` which provides different ways to retrieve results.

### Simple Search

The `SearchDef` class supports matching one or more patterns against each line in a file. Patterns are executed until the first match is found.

When defining a search, you can optionally specify field names so that result values can be retrieved by name rather than index e.g. for the following content:

```
    PID TTY          TIME CMD
 111024 pts/4    00:00:00 bash
 111031 pts/4    00:00:00 ps
```

You can define as search as follows:

```python
SearchDef(r'.*(\S+)\s+(\S+)\s+(\S+)\s+(\S+)')
```

and retrieve results with:

```python
for r in results:
    pid = r.get(1)
    tty = r.get(2)
    time = r.get(3)
    cmd = r.get(4)
```

or alternatively:

```python
for r in results:
    pid, tty, time, cmd = r
```

or you can provide field names and types:

```python
fields = ResultFieldInfo({'PID': int, 'TTY': str, 'TIME': str, 'CMD': str})
SearchDef(r'.*(\S+)\s+(\S+)\s+(\S+)\s+(\S+)', field_info=fields)
```

and retrieve results with:

```python
for r in results:
    pid = r.PID
    tty = r.TTY
    time = r.TIME
    cmd = r.CMD
```

### Sequence Search

The `SequenceSearchDef` class supports matching string sequences ("sections") over multiple lines by matching a start, end and optional body in between. These section components are each defined with their own `SearchDef` object.

## Installation

searchkit is packaged in [pypi](https://pypi.org/project/searchkit) and can be installed as follows:

```console
sudo apt install python3-pip
pip install searchkit
```

## Example Usage

An example simple search is as follows:

```python
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

```python
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
