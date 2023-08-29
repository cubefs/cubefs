import os
import os.path


def copyfileobj(fsrc, fdst, length=128 * 1024):
    while 1:
        buf = fsrc.read(length)
        if not buf:
            break
        fdst.write(buf)


def do_copy(message):
    src_file, dst_file = message
    try:
        if os.path.exists(dst_file):
            return
        dst_dir = os.path.dirname(dst_file)
        src = src_file
        dst = dst_file + ".tmp"
        os.makedirs(dst_dir, exist_ok=True)
        with open(src, 'rb') as fsrc:
            with open(dst, 'wb') as fdst:
                copyfileobj(fsrc, fdst)
        os.rename(dst, dst_file)
    except KeyboardInterrupt:
        return
    except Exception as e:
        pass
