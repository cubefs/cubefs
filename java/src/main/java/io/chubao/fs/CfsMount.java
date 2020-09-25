package io.chubao.fs;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import io.chubao.fs.CfsLibrary.Dirent;
import io.chubao.fs.CfsLibrary.DirentArray;

public class CfsMount {
    // Open flags
    public final int O_RDONLY = 0;
    public final int O_WRONLY = 1;
    public final int O_RDWR = 2;
    public final int O_ACCMODE = 3;
    public final int O_CREAT = 0100;
    public final int O_TRUNC = 01000;
    public final int O_APPEND = 02000;
    public final int O_DIRECT = 040000;

    // Mode
    public final int S_IFDIR = 0040000;
    public final int S_IFREG = 0100000;
    public final int S_IFLNK = 0120000;

    // dType used in Dirent
    public final int DT_UNKNOWN = 0x0;
    public final int DT_DIR = 0x4;
    public final int DT_REG = 0x8;
    public final int DT_LNK = 0xa;

    // Valid flags for setattr
    // Must be compatible with proto.Attr values
    public final int SETATTR_MODE = 1;
    public final int SETATTR_UID = 2;
    public final int SETATTR_GID = 4;
    public final int SETATTR_MTIME = 8;
    public final int SETATTR_ATIME = 16;

    private CfsLibrary libcfs;
    private String libpath;
    private long cid; // client id allocated by libcfs library

    public CfsMount(String path) {
        libpath = path;
        libcfs = (CfsLibrary) Native.load(libpath, CfsLibrary.class);
        cid = libcfs.cfs_new_client();
    }

    public int SetClient(String key, String val) {
        return libcfs.cfs_set_client(this.cid, key, val);
    }

    public int StartClient() {
        return libcfs.cfs_start_client(this.cid);
    }

    public void CloseClient() {
        libcfs.cfs_close_client(this.cid);
    }

    public int Chdir(String path) {
        return libcfs.cfs_chdir(this.cid, path);
    }

    public String Getcwd() {
        return libcfs.cfs_getcwd(this.cid);
    }

    public int GetAttr(String path, CfsLibrary.StatInfo stat) {
        return libcfs.cfs_getattr(this.cid, path, stat);
    }

    public int SetAttr(String path, CfsLibrary.StatInfo stat, int mask) {
        return libcfs.cfs_setattr(this.cid, path, stat, mask);
    }

    public int Open(String path, int flags, int mode) {
        return libcfs.cfs_open(this.cid, path, flags, mode, 0, 0);
    }

    public void Close(int fd) {
        libcfs.cfs_close(this.cid, fd);
    }

    public long Write(int fd, byte[] buf, long size, long offset) {
        return libcfs.cfs_write(this.cid, fd, buf, size, offset);
    }

    public long Read(int fd, byte[] buf, long size, long offset) {
        return libcfs.cfs_read(this.cid, fd, buf, size, offset);
    }

    /*
     * Note that the memory allocated for Dirent[] must be countinuous. For example,
     * (new Dirent()).toArray(count).
     */
    public int Readdir(int fd, Dirent[] dents, int count) {
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

    public int Fchmod(int fd, int mode) {
        return libcfs.cfs_fchmod(this.cid, fd, mode);
    }
}
