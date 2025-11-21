import base64
import datetime
import functools
import hashlib
import json
import pathlib
import tarfile
import time

from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject

# from ..mango_flow_app import MFException
from .report import TaskProgressPart

"""
@todo: explain the overall ideas of this tarring, like that it is generic and can accept regular files as well as irods objects
"""


class ManGOFlowTaskAbortException(Exception):
    pass


class TarInputItem:
    """
    A utility/abstract class that represents the input to read from in a normalized way.
    It should provide system metadata such as the name, but also size, modified time and checksum
    We need two incarnations for now: a file on a regular file system, and an irods data object

    """

    # item:
    # needs_checksum = False

    def __init__(self, item, needs_checksum=False):
        self.item = item
        self.needs_checksum = needs_checksum
        self._name = ""
        self._size = 0
        self._modified = 0.0
        self._checksum = None
        self._alt_name = ""

    @property
    def name(self):
        return self._name

    @property
    def modified(self):
        return self._modified

    @property
    def size(self):
        return self._size

    @property
    def checksum(self):
        return self._checksum

    @property
    def alt_name(self):
        return self._alt_name

    @checksum.setter
    def checksum(self, value):
        self._checksum = value

    def open_for_read(self):
        return None


class iRODSInputItem(TarInputItem):
    def __init__(
        self,
        item: iRODSDataObject | iRODSCollection,
        needs_checksum=False,
        rel_path="",
        prefix="",
        alt_prefix="",
    ):
        super().__init__(item, needs_checksum)
        self._name = (
            item.path
            if not rel_path
            else str(
                pathlib.PosixPath(prefix)
                / pathlib.PosixPath(item.path).relative_to(rel_path)
            )
        )
        self._alt_name = (
            self._name
            if not alt_prefix
            else str(
                pathlib.PosixPath(alt_prefix)
                / pathlib.PosixPath(item.path).relative_to(rel_path)
            )
        )
        self._size = item.size if isinstance(item, iRODSDataObject) else 0  # type: ignore
        self._modified = item.modify_time.timestamp()  # type: ignore
        if isinstance(item, iRODSDataObject) and item.checksum:  # type: ignore # maybe there is no need to do the expensive calculation?
            self._checksum = base64.b64decode(item.checksum.replace("sha2:", "")).hex()  # type: ignore
            self.needs_checksum = False

    def open_for_read(self):
        return self.item.open("r")


class FileInputItem(TarInputItem):
    item: pathlib.Path  # | None = None

    def __init__(
        self,
        item: pathlib.Path,
        needs_checksum=False,
        rel_path="",
        prefix="",
        alt_prefix="",
    ):
        super().__init__(item, needs_checksum)
        self._name = (
            str(item)
            if not rel_path
            else str(pathlib.PosixPath(prefix) / item.relative_to(rel_path))
        )
        self._alt_name = (
            self._name
            if not alt_prefix
            else str(pathlib.PosixPath(alt_prefix) / item.relative_to(rel_path))
        )
        self._size = item.stat().st_size
        self._modified = item.stat().st_mtime
        self._checksum = None

    def open_for_read(self):
        return self.item.open("rb")


class BytesInputItem(TarInputItem):
    def __init__(
        self, name, item, needs_checksum=False, rel_path="", prefix="", alt_prefix=""
    ):
        super().__init__(item, needs_checksum)
        if rel_path:
            name = str(pathlib.PosixPath(name).relative_to(rel_path))
        self._name = name if not prefix else f"{prefix}/{name}"
        self._alt_name = self._name if not alt_prefix else f"{alt_prefix}/{name}"
        self._size = len(item)
        self._modified = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        if needs_checksum:
            self._checksum = hashlib.sha256(item).hexdigest()


# ## Modified code that came originally from ChatGPT via Peter Verraedt

BLOCK_SIZE = 512  # tar file fundamental uit
EMPTY_BLOCK = b"\0" * BLOCK_SIZE


def pad_to_block(data):
    """Pad data to the nearest 512-byte block."""
    remainder = len(data) % BLOCK_SIZE
    if remainder == 0:
        return data
    return data + (b"\0" * (BLOCK_SIZE - remainder))


def create_pax_header(input_item: TarInputItem):
    """Create a PAX tar header block for a file."""
    tarinfo = tarfile.TarInfo(input_item.name)
    print(f"writing tarinfo file name: {input_item.name}")
    tarinfo.size = input_item.size
    tarinfo.mtime = input_item.modified
    tarinfo.type = tarfile.REGTYPE
    tarinfo.mode = 0o644
    tarinfo.pax_headers = {}
    tarinfo_bytes = tarinfo.tobuf(format=tarfile.GNU_FORMAT)
    return tarinfo_bytes


def write_file_header(tarfile_fp, input_item: TarInputItem):
    """Write a file header and return a position where to write data."""
    header = create_pax_header(input_item=input_item)
    tarfile_fp.write(header)
    position = tarfile_fp.tell()
    return position


def write_tar_end(tarfile_fp):
    """Write two empty 512-byte blocks to end the tar."""
    tarfile_fp.write(EMPTY_BLOCK + EMPTY_BLOCK)


class TarOrchestrator:
    """
    Actionable monitoring base class. This is to be passed at the various levels in the tarring process
    Callbacks can be inserted and will be called depending on the level. They are supposed to be
    self suffient in their operations. The orchestrator class does not assume any other responsibility
    than to call the callbacks with a single positional argument which depends on the level.
    """

    # GUARD_CALLBACK_SCOPE = "guard"

    def __init__(
        self,
        chunk_threshold=None,
    ) -> None:
        self.start_time = time.time()
        self.num_bytes_processed = 0
        self.total_bytes = 0
        self.total_files = 0
        self.num_files_processed = 0
        self.total_collections = 0
        self.num_collections_processed = 0
        self.chunk_threshold = chunk_threshold or 2**22  # 4MiB
        self.local_dir = "."
        self.save_interval_time = 10.0
        # callbacks
        self.callbacks = {
            "start": {},
            "guard": {},
            "file_end": {},
            "delay_point": {},
            "collection_item": {},
            "end": {},
            "abort": {},
            "pause": {},
            "exception": {},
        }

        self.last_good_write = {}
        self.checksums = []  # may not be used, secondary storage for manifest output
        # progress
        self.current_file_progress = TaskProgressPart(
            title="File progress", unit="bytes", start_time=self.start_time
        )
        self.overall_file_progress = TaskProgressPart(
            title="Overall # files progress", start_time=self.start_time
        )
        self.overall_size_progress = TaskProgressPart(
            title="Overall bytes progress", unit="bytes", start_time=self.start_time
        )

        self.save_cycle_reference_time = time.time()
        self.report_reference_time = time.time()

    def add_callback(self, scope, name, callback: functools.partial):
        if scope in self.callbacks:
            self.callbacks[scope][name] = callback
        else:
            raise ValueError(f"Unknow scope {scope} in trying to add callback")

    def at_main_start(self):
        """
        This method needs to be called before the main tarring loop.
        It will also call any configured callback partials
        and pass the tar_file as the first argument

        """
        # @Todo: reset reference times to current time
        for name in self.callbacks["start"]:
            print(f"Calling start callback {name=}")
            self.callbacks["start"][name](self)

    def at_current_file_start(self, item: TarInputItem):
        """
        This method should be called before a new file is added in a loop
        It initializes statistics parameters at this level only
        No callbacks are called
        """

        self.current_file = {
            "name": item.name,
            "bytes_read": 0,
            "total_size": item.size,
            "read_started": time.time(),
        }

        self.current_file_progress.value = 0
        self.current_file_progress.start_time = time.time()
        self.current_file_progress.name = item.name
        self.current_file_progress.total = item.size

    def at_chunk_progress(self, bytes_read):
        """
        This method should be called in the streaming of chuncks loop/iterator.
        It updates low level statistics and calls the guard callbacks.
        """
        self.current_file_progress.value = bytes_read
        self.current_file_progress.current_time = time.time()
        if self.current_file["total_size"] > self.chunk_threshold:
            for name, guard in self.callbacks["guard"].items():
                # print(f"{name}")
                guard(self)

    def at_current_file_end(
        self, item: TarInputItem, dest_tar, callbacks: str | list | None = "all"
    ):
        """
        This method should be called after a new file is added in a loop.
        It launches also all guard and file_end callbacks which are supposed to be
        self sufficient to perform any desired operation, including raising exceptions
        """
        self.overall_file_progress.value += 1
        self.overall_size_progress.value += item.size
        self.overall_size_progress.current_time = (
            self.overall_file_progress.current_time
        ) = time.time()

        path = item.item.path if item.item is iRODSDataObject else f"{item.item}"  # type: ignore
        self.last_good_write = {"path": path, "tar_file_end": dest_tar.tell()}
        # now call the tar_me_too_partials
        # guards
        for name, guard in self.callbacks["guard"].items():
            print(f"Calling guard {name=} at {item.name=}")
            guard(self)

        if callbacks == "all" and callbacks is not None:
            # then use all callbacks
            callbacks = list(self.callbacks["file_end"].keys())
        elif callbacks is None:
            callbacks = []

        for name in callbacks:
            print(f"Calling file end callback {name=} getting {item.name=}")
            self.callbacks["file_end"][name](self, item)

    def at_delay_point(self):
        """
        This method should be called at regular intervals that transcend
        the fast pace of buffered writes of large objects or looping
        over many smaller objects. The timing is typically at the scale of seconds
        It will call any configured callback partials.
        The main use cases are expensive calls like soft abort requests in a
        celery context, status updates to some persistent storage, flushing manifest data to disk,...

        """
        for name in self.callbacks["delay_point"]:
            print(f"Calling start callback {name=}")
            self.callbacks["delay_point"][name](self)

    def at_collection_item(self, item):
        for name in self.callbacks["collection_item"]:
            print(f"Calling collection callback {name=}")
            self.callbacks["colection_item"][name](self, item)

    def at_main_end(self):
        """
        This method needs to be called after the main tarring loop.
        It will also call any configured callback partials

        """
        for name in self.callbacks["end"]:
            print(f"Calling end callback {name=}")
            self.callbacks["end"][name](self)

    def at_abort(self):
        for name in self.callbacks["abort"]:
            print(f"Calling abort callback {name=}")
            self.callbacks["abort"][name](self)

    def at_pause(self):
        for name in self.callbacks["pause"]:
            print(f"Calling pause callback {name=}")
            self.callbacks["pause"][name](self)

    def at_exception(self):
        for name in self.callbacks["exception"]:
            print(f"Calling exception callback {name=}")
            self.callbacks["exception"][name](self)


# adapted from basic operations, irods object streaming between zones
def read_from_fp_in_chunks(
    read_fp, read_buffer_size, file_size, orchestrator: TarOrchestrator | None = None
):
    position = 0
    while position < file_size:
        read_bytes = min(
            read_buffer_size,
            file_size - position,
        )
        file_chunk = read_fp.read(read_bytes)
        bytes_read = len(file_chunk)
        position += bytes_read
        if orchestrator:
            orchestrator.at_chunk_progress(position)
        yield file_chunk


def stream_fp_to_tar(
    read_fp,
    input_item: TarInputItem,
    tar_dest,
    read_buffer_size,
    orchestrator=None,
):
    # first write the header and obtain the position for tha actual file data
    tar_file_position_file_data = write_file_header(
        tarfile_fp=tar_dest, input_item=input_item
    )
    tar_dest.seek(tar_file_position_file_data)
    # now actual file data
    bytes_written = 0
    if input_item.needs_checksum:
        h = hashlib.sha256()
    for chunk in read_from_fp_in_chunks(
        read_fp=read_fp,
        read_buffer_size=read_buffer_size,
        file_size=input_item.size,
        orchestrator=orchestrator,
    ):
        tar_dest.write(chunk)
        bytes_written += len(chunk)
        if input_item.needs_checksum:
            h.update(chunk)

    # bytes_written should now be equal to the file size. Should we report also the header overhead?
    remainder = input_item.size % BLOCK_SIZE
    if remainder > 0:
        tar_dest.write(b"\0" * (BLOCK_SIZE - remainder))

    if input_item.needs_checksum:
        input_item.checksum = h.hexdigest()


def add_bytes_item_to_tar(tar_fp, item: BytesInputItem):
    _ = write_file_header(tarfile_fp=tar_fp, input_item=item)
    tar_fp.write(pad_to_block(item.item))


# possible use as callback: register current checksum if any in the orchestrator structure
def record_checksum(item: TarInputItem, *, orchestrator: TarOrchestrator):
    orchestrator.checksums.append(
        {
            "name": item.name,
            "size": item.size,
            "checksum": item.checksum,
            "modified": item.modified,
            "alt_name": item.alt_name,
        }
    )


def flush_checksums(orchestrator: TarOrchestrator, fp, format="bagit", name_field="name"):
    match format:
        case "bagit":
            for item in orchestrator.checksums:
                fp.write(f"{item['checksum']} {item[name_field]}\n")
        case "jsonline":
            for item in orchestrator.checksums:
                fp.write(f"{json.dumps(item)}\n")

    orchestrator.checksums.clear()


def create_tar_from_iterators_and_orchestrator(
    object_iterator,
    collection_iterator,
    dest_tar,
    orchestrator: TarOrchestrator,
):
    # just in case: callbacks can obain the tar_fp from the orchestrator
    setattr(orchestrator, "dest_tar", dest_tar)

    try:
        orchestrator.at_main_start()
        # MAIN LOOP
        for tar_item in object_iterator:
            with tar_item.open_for_read() as read_fp:
                # print(f"Treating file : {irods_item.name}")

                orchestrator.at_current_file_start(item=tar_item)
                stream_fp_to_tar(
                    read_fp=read_fp,
                    input_item=tar_item,
                    tar_dest=dest_tar,
                    read_buffer_size=BLOCK_SIZE * 2048,
                    orchestrator=orchestrator,
                )
            orchestrator.at_current_file_end(item=tar_item, dest_tar=dest_tar)

            #  use cases intermediate saves if max save cycle interval is crossed
            if (
                (current_time := time.time()) - orchestrator.save_cycle_reference_time
            ) > orchestrator.save_interval_time:
                orchestrator.at_delay_point()
                orchestrator.save_cycle_reference_time = current_time
        # finished files/ data objects, now collections or folders:

        for tar_item in collection_iterator:
            orchestrator.at_collection_item(tar_item)
            orchestrator.at_current_file_end(
                item=tar_item, dest_tar=dest_tar, callbacks=None
            )

        # normal succesful execution: finishing up

        # WRITE MANIFEST FILE
        orchestrator.at_main_end()
        write_tar_end(dest_tar)

    except (
        ManGOFlowTaskAbortException
    ) as e:  # any other exception will not be catched here
        orchestrator.at_abort()
        raise (e)  # now we can really exit the task

    except:  # noqa: E722
        # before we really exit, save some vital info like manifest and last good write
        orchestrator.at_exception()
        raise ValueError("Abnormal termination")

