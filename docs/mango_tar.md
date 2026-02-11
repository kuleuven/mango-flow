# Tarring and archiving with (or without) ManGO Flow

The [mango_tar](../src/mango_flow/lib/mango_tar.py) module defines classes and functions to create a tar archive of files either from the file system or iRODS data objects. The destination tar file can also be either on a (mounted) file system or an iRODS data object. It also comes with some additional features:

- keeping track of the progress of the tarring at the chunk level via a pure python streaming re-implementation of writing the tar file
- adding low level hooks/callbacks at any stage during the process, e.g. to abort the process cleanly upon an external signal when wrapping the functionality in a Celery task
- optionally registering callbacks that can perform any additional task at the individual file level (when a new item has been written to the destination tar file), for example the checksum calculation or a sidecar file with metadata of the item added to it

The code in this module can be tested by running the script locally,
which calls the `test_the_thing()` function. 

<!-- TODO: move the test function into its own file inside a tests folder -->
Do change [the glob pattern](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L391)
to something that can be found in your local system.

<!-- TODO: anticipate the sections -->

## The tarring function

If we focus on the tarring functionality, the core user-facing function
is [`stream_fp_to_tar()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L320-L345).
This function takes as input:

- An open file-like object `read_fp`, e.g. the result of [`rich.progress.open(input_file, "rb")`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L442) in the example function;
- A [`TarInputItem`](#the-tarinputitem-class-and-its-children) instance `input_item`, which normalizes information such as the name, size, modification time and checksum across different types of objects;
- An open file-like object to write the tar into, `dest_tar`;
- The size of the buffer for reading, `read_buffer_size`;
- An optional [`orchestratorTar`](#the-tarorchestrator-class) instance `orchestrator`.

The arguments `read_fp`, `read_buffer_size` and `orchestrator`, along with the size of `input_item`,
are sent to [`read_from _fp_in_chunks()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L303-L317),
which returns a generator of file chunks and,
_if the orchestrator is provided_, runs `orchestrator.at_chunk_progress()`
to call any eventual [guards](#guards), e.g. checking whether to abort.
At each iteration of this generator, a chunk of the file is read,
then written into the tar,
and if relevant the checksum of the chunk is computed and the checksum
of the file is [updated](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L345) with that information.
At the end of this function, if checksum was requested, `input_item` is
[updated](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L353)
with the checksum value. 

Note that this module writes to the tar directly,
only using the `tarfile` module to generate a valid header.
First, [`write_file_header()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L162-L167),
called at the beginning of `stream_fp_to_tar()`,
[writes the header](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L149-L159) and provides the position to keep writing from.
Then, when looping over the chunks, each of them is written into the tar.
A tar needs to be written in blocks of [512-bytes](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L137),
so if the number of bytes in the file is not a multiple of 512,
[it is padded at the end](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L348-L350).
The smaller [`add_bytes_item_to_tar()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L356-L358) function
only writes the header and then the file, padded to blocks of 512 bytes, skipping the split in chunks and the orchestrator callbacks.

## The `TarInputItem` class and its children

As mention above, the [tarring function](#the-tarring-function)
expects a `TarInputItem` `input_item` - more precisely,
it expects an instance of one of its child classes.
The idea is that different kinds of inputs (file-like objects)
have different native methods to extract the information we need for tarring.
This information includes
the path to the file in the tar (and eventually in the manifest),
the size,
the last modification time
and the checksum (when it might have already been computed).

By wrapping these file-like objects into these classes,
the [`TarOrchestrator`](#the-tarorchestrator-class) instances
can query this information using the same methods in all cases.

There are currently three child classes to represent three kinds of file-like objects:

- [`iRODSInputItem`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L65-L95) to represent iRODS Data Objects.
- [`FileInputItem`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L98-L122) to represent local files.
- [`BytesInputItem`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L125-L132) to represent `io.BytesIO` objects, e.g. bytes representations of objects in memory. This is useful in order to add [the bagit file](https://gitea.icts.kuleuven.be/foz/cold-storage/src/commit/9a8f208fcdf4d4a60727e010b9e96f67a79a7fe6/src/cold_storage/tasks/frigo.py#L16-L24)
or
[metadata from an iRODS data object or collection](https://gitea.icts.kuleuven.be/foz/cold-storage/src/commit/9a8f208fcdf4d4a60727e010b9e96f67a79a7fe6/src/cold_storage/tasks/frigo.py#L27-L38) without having to save them to a local file first.

### The paths

The original path of, say, an `iRODSDataObject`, maybe something like "/zone/home/projectName/collectionName/subcollection/filename.csv".

If the collection to tar is "collectionName", then we need to truncate the path to turn it into a path relative to the root of the tar, i.e. "subcollection/filename.csv". This is done when instantiating an `iRODSInputItem` by providing the `rel_path` "/zone/home/projectName/collectionName/subcollection/filename.csv".

However, we are not _just_ creating a tar, but a
[tar with a BagIt in an RO-Crate](https://gitea.icts.kuleuven.be/foz/RDM/src/branch/main/meeting-notes/cold-storage/bagit-rocrate.md#proposal).
Therefore, we also need to add a `prefix` to the path, namely "bag/data/", to generate the appropriate path inside the tar. This then becomes the name of the `input_item`.

In addition, the paths in the _manifest_ are relative to the payload of the BagIt,
not to the root of the tar: they should start with "data/", not "bag/data/". This can be provided as `alt_prefix`.

## The `TarOrchestrator` class

This class is used for monitoring, i.e. to keep track of the status of the tarring (various stats, and most importantly,
[the last file to be properly appended to the tar](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L291)) and collect callbacks. There are two types of callbacks: [guards](#guards) and [file-end](#file-end-callbacks) callbacks.

### Guards

**Guards** (so called because their main goal is to be able to abort the progress)
are called every time a chunk of a file is successfully added to the tar
(called by the
[`at_chunk_progress()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L264-L280)
method)
AND when the file has been completely added to the file
(called by the
[`at_current_file_end()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L282-L299)
method).
They are functions (often partials) that only take the instance of the `TarOrchestrator` as argument.
An example is [`fake_abort()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L414-L416):

```python
 def fake_abort(orchestrator: orchestratorTar):
    if orchestrator.num_files_processed > 14:
        raise ManGOFlowTaskAbortException()
```

This is a function that only takes as input a `checkpoint` instance of the `TarOrchestrator` class
and raises an exception to abort the task.
Because this is a dummy example, it just aborts whenever 14 files have been added
to the tar, but in [the more realistic `check_abort()`](https://gitea.icts.kuleuven.be/foz/cold-storage/src/commit/9a8f208fcdf4d4a60727e010b9e96f67a79a7fe6/src/cold_storage/tasks/frigo.py#L87-L89) we check for an attribute of the orchestrator
to trigger the task abortion (this attribute is updated by regular checks to the database).

The utility in this case is that each time that a chunk is written,
the guard is run, thus checking whether the function should raise this abortion error.

### File-end callbacks

The **file-end callbacks** are only called when a file has been completely,
successfully added to the tar, by the
[`at_current_file_end()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L282-L299)
method.
These are functions (often partials) that only take the item
(instance of [`TarInputItem`](#the-tarinputitem-class-and-its-children))
as argument.
An example is [`add_metadata_to_tar_test()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L399-L412):

```python
def add_metadata_to_tar_test(
    item: TarInputItem, dest_tar: io.BufferedWriter
):  # will be used in partial
    metadata = json.dumps(
        {
            "name": item.name,
            "secret": item.checksum,
            "birth_date": "yesterday",
            "id": str(uuid.uuid4()),
        }
    ).encode()
    add_bytes_item_to_tar(
        dest_tar, BytesInputItem(f"{item.name}.metadata.json", metadata)
    )
```

This function takes two arguments:
a [`TarInputItem`](#the-tarinputitem-class-and-its-children)
and a file open for writing (the destination tar).
The idea is that when a file (the item) has been fully written into the tar
a (dummy) metadata file is generated with information about the item
and saved to the tar with
[`add_bytes_item_to_tar()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L356-L358).
This latter function appends a file without going through chunks, guards, etc.

In the [iRODS-specific implementation](https://gitea.icts.kuleuven.be/foz/cold-storage/src/commit/9a8f208fcdf4d4a60727e010b9e96f67a79a7fe6/src/cold_storage/tasks/frigo.py#L27-L38) the metadata is collected from the data object
and the checksum of the metadata "file" is also registered, for the eventual manifest.

As mentioned before, the file-end callback can only take one argument, i.e. the item.
In order to accomodate that, and because for all the files to be added to a given
tar the `dest_tar` argument will be the same,
we don't provide the raw function as a callback but a [partial](https://mathspp.com/blog/functools-partial) in which `dest_tar` is fixed.
The result of [`functools.partial(add_metadata_to_tar_test, dest_tar=dest_tar)`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L437-L440)
is a function that only takes a `TarInputItem` as input, and always writes to
the same `dest_tar`.

### Checksums

Another use of the file-end callback is to record the checksums.
[`record_checksum()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L362-L370)
also takes two arguments, a `TarInputItem` and a `TarOrchestrator`,
and is [provided as a partial fixing the `orchestrator` argument](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L423-L425).

For each file successfully added to the tar,
it appends a dictionary with file-specific information (including its checksum)
to an attribute of the `TarOrchestrator` instance itself.
Then, [every now and then](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L457-L459)
the [`flush_checksums()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/16aee32ee8ef84bf140233d27c6a631bea810db9/src/mango_flow/lib/mango_tar.py#L373-L382) function
is called to store these checksums in a (manifest) file and clean up the orchestrator.
This is not done once per file, but in regular intervals, to increase performance.

