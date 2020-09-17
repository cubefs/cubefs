package io.chubao.fs;

import com.sun.jna.Library;
import com.sun.jna.Structure;
import com.sun.jna.Pointer;
import java.util.List;
import java.util.Arrays;

public interface CfsDriver extends Library {
    public class StatInfo extends Structure implements Structure.ByReference {
        // note that the field layout should be aligned with cfs_stat_info
        public long ino;
        public long size;
        public long blocks;
        public long atime;
        public long mtime;
        public long ctime;
        public int atime_nsec;
        public int mtime_nsec;
        public int ctime_nsec;
        public int mode;
        public int nlink;
        public int blkSize;
        public int uid;
        public int gid;

        public StatInfo() {
            super();
        };

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "ino", "size", "blocks", "atime", "mtime", "ctime", "atime_nsec",
                    "mtime_nsec", "ctime_nsec", "mode", "nlink", "blkSize", "uid", "gid" });
        }
    }

    public class Dirent extends Structure {
        // note that the field layout should be aligned with cfs_dirent
        public long ino;
        public byte[] name = new byte[256];
        public byte dType;

        public Dirent() {
            super();
        };

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "ino", "name", "dType" });
        }
    }

    public class DirentArray extends Structure {
        public static class ByValue extends DirentArray implements Structure.ByValue {
        }

        // note that the field layout should be aligned with GoSlice
        public Pointer data;
        public long len;
        public long cap;

        public DirentArray() {
            super();
        }

        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "data", "len", "cap" });
        }
    }

    // exports from shared library
    long cfs_new_client();

    int cfs_set_client(long id, String key, String val);

    int cfs_start_client(long id);

    void cfs_close_client(long id);

    int cfs_chdir(long id, String path);

    String cfs_getcwd(long id);

    int cfs_getattr(long id, String path, StatInfo stat);

    int cfs_open(long id, String path, int flags, int mode);

    int cfs_flush(long id, int fd);

    void cfs_close(long id, int fd);

    long cfs_write(long id, int fd, byte[] buf, long size, long offset);

    long cfs_read(long id, int fd, byte[] buf, long size, long offset);

    int cfs_readdir(long id, int fd, DirentArray.ByValue dents, long count);
}
