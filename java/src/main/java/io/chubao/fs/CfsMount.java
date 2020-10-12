package io.chubao.fs;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import io.chubao.fs.CfsLibrary.Dirent;
import io.chubao.fs.CfsLibrary.DirentArray;

public class CfsMount {
    // Open flags
    public static final int O_RDONLY = 0;
    public static final int O_WRONLY = 1;
    public static final int O_RDWR = 2;
    public static final int O_ACCMODE = 3;
    public static final int O_CREAT = 0100;
    public static final int O_TRUNC = 01000;
    public static final int O_APPEND = 02000;
    public static final int O_DIRECT = 040000;

    // Mode
    public static final int S_IFDIR = 0040000;
    public static final int S_IFREG = 0100000;
    public static final int S_IFLNK = 0120000;

    // dType used in Dirent
    public static final int DT_UNKNOWN = 0x0;
    public static final int DT_DIR = 0x4;
    public static final int DT_REG = 0x8;
    public static final int DT_LNK = 0xa;

    // Valid flags for setattr
    // Must be compatible with proto.Attr values
    public static final int SETATTR_MODE = 1;
    public static final int SETATTR_UID = 2;
    public static final int SETATTR_GID = 4;
    public static final int SETATTR_MTIME = 8;
    public static final int SETATTR_ATIME = 16;

    private CfsLibrary libcfs;
    private String libpath;
    private long cid; // client id allocated by libcfs library

    public CfsMount(String path) {
        libpath = path;
        libcfs = (CfsLibrary) Native.load(libpath, CfsLibrary.class);
        cid = libcfs.cfs_new_client();
    }

    public int setClient(String key, String val) {
        return libcfs.cfs_set_client(this.cid, key, val);
    }

    public int startClient() {
        return libcfs.cfs_start_client(this.cid);
    }

    public void closeClient() {
        libcfs.cfs_close_client(this.cid);
    }

    public int chdir(String path) {
        return libcfs.cfs_chdir(this.cid, path);
    }

    public String getcwd() {
        return libcfs.cfs_getcwd(this.cid);
    }

    public int getAttr(String path, CfsLibrary.StatInfo stat) {
        return libcfs.cfs_getattr(this.cid, path, stat);
    }

    public int setAttr(String path, CfsLibrary.StatInfo stat, int mask) {
        return libcfs.cfs_setattr(this.cid, path, stat, mask);
    }

    public int open(String path, int flags, int mode) {
        return libcfs.cfs_open(this.cid, path, flags, mode);
    }

    public void close(int fd) {
        libcfs.cfs_close(this.cid, fd);
    }

    public long write(int fd, byte[] buf, long size, long offset) {
        return libcfs.cfs_write(this.cid, fd, buf, size, offset);
    }

    public long read(int fd, byte[] buf, long size, long offset) {
        return libcfs.cfs_read(this.cid, fd, buf, size, offset);
    }

    /*
     * Note that the memory allocated for Dirent[] must be countinuous. For example,
     * (new Dirent()).toArray(count).
     */
    public int readdir(int fd, Dirent[] dents, int count) {
        Pointer arr = dents[0].getPointer();
        DirentArray.ByValue slice = new DirentArray.ByValue();
        slice.data = arr;
        slice.len = (long) count;
        slice.cap = (long) count;

        long arrSize = libcfs.cfs_readdir(this.cid, fd, slice, count);

        if (arrSize > 0) {
            for (int i = 0; i < (int) arrSize; i++) {
                dents[i].read();
            }
        }

        return (int) arrSize;
    }

    public int mkdirs(String path, int mode) {
        return libcfs.cfs_mkdirs(this.cid, path, mode);
    }

    public int rmdir(String path) {
        return libcfs.cfs_rmdir(this.cid, path);
    }

    public int unlink(String path) {
        return libcfs.cfs_unlink(this.cid, path);
    }

    public int rename(String from, String to) {
        return libcfs.cfs_rename(this.cid, from, to);
    }

    public int fchmod(int fd, int mode) {
        return libcfs.cfs_fchmod(this.cid, fd, mode);
    }
}
