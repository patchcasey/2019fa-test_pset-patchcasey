from contextlib import contextmanager
import tempfile
import os
import shutil
import time


def suffix_parser(input):
    parts = []
    parts = input.split(".")
    return "." + parts[1]


@contextmanager
def atomic_write(file, mode="w", as_file=True, *args, **kwargs):
    """Write a file atomically

    :param file: str or :class:`os.PathLike` target to write

    :param bool as_file:  if True, the yielded object is a :class:File.
        (eg, what you get with `open(...)`).  Otherwise, it will be the
        temporary file path string

    :param kwargs: anything else needed to open the file

    :raises: FileExistsError if target exists

    Example::

        with atomic_write("hello.txt") as f:
            f.write("world!")

    """

    tf = tempfile.NamedTemporaryFile(
        delete=False, dir=os.path.dirname(file), suffix=suffix_parser(file)
    )
    tf.close()

    # Open temporary file, write to it, atomically close, handling errors in writing process
    if as_file == True:
        with open(tf.name, "w") as temporaryfile:
            try:
                yield temporaryfile
            except IOError as e:
                print(temporaryfile.name)
                temporaryfile.close()
                os.remove(temporaryfile.name)
                raise e
            else:
                temporaryfile.seek(0)
                temporaryfile.flush()
                os.fsync(temporaryfile.fileno())
                temporaryfile.close()
                try:
                    os.link(temporaryfile.name, file)
                except FileExistsError as e:
                    os.remove(temporaryfile.name)
                    raise FileExistsError
                os.remove(temporaryfile.name)
    # returns name of tempfile as requested
    else:
        yield tf
        tempfile_name = tf.name
        tf.close()
        os.remove(tf.name)
        return tempfile_name


@contextmanager
def test():
    cwd = os.getcwd()
    fp = os.path.join(cwd, "zzzz.txt")
    with atomic_write(fp, "w") as f:
        f.write("world!")


if __name__ == "__main__":
    test()
