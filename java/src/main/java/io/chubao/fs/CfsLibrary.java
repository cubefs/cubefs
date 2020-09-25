package io.chubao.fs;

import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

public interface CfsLibrary extends Library {
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
        public static class ByReference extends StatInfo implements Structure.ByReference {}
        public static class ByValue extends StatInfo implements Structure.ByValue {}
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("inodeid:");
            sb.append(ino);
            sb.append(" szie:");
            sb.append(size);
            sb.append(" uid:");
            sb.append(uid);
            sb.append(" gid:");
            sb.append(gid);
            sb.append(" mode:");
            sb.append(mode);
            sb.append(" atime:");
            sb.append(atime);
            sb.append(" ctime:");
            sb.append(ctime);
            sb.append(" mtime:");
            sb.append(mtime);
            sb.append(" atime:");
            sb.append(atime);
            sb.append(" atime:");
            sb.append(atime);
            return sb.toString();
        }

    }

    public class Dirent extends Structure {
        // note that the field layout should be aligned with cfs_dirent
        public long ino;
        public byte dType;
        public int nameLen;
        public byte[] name = new byte[256];

        public Dirent() {
            super();
        };

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "ino", "dType", "nameLen", "name" });
        }
        public static class ByReference extends Dirent implements Structure.ByReference {}
        public static class ByValue extends Dirent implements Structure.ByValue {}

    }

    public class DirentArray extends Structure {
        public static class ByValue extends DirentArray implements Structure.ByValue {
        }

        public static class ByReference extends DirentArray implements Structure.ByReference {}

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

    int cfs_setattr(long id, String path, StatInfo stat, int mask);

    int cfs_open(long id, String path, int flags, int mode, int uid, int gid);

    int cfs_flush(long id, int fd);

    void cfs_close(long id, int fd);

    long cfs_write(long id, int fd, byte[] buf, long size, long offset);

    long cfs_read(long id, int fd, byte[] buf, long size, long offset);
    int cfs_mkdirs(long cid, String path, int mode, int uid, int gid);
    int cfs_unlink(long cid, String path);
    int cfs_rename(long cid, String from, String to);

    int cfs_readdir(long id, int fd, DirentArray.ByValue dents, long count);

    int cfs_fchmod(long id, int fd, int mode);
    int cfs_rmdir(long cid, String path, boolean recursive);
    int cfs_batch_get_inodes(long cid, int fd, long[] iids, DirentArray.ByValue stats, int count);
    int cfs_setattr_by_path(long cid, String path, StatInfo info, int valid);
    long cfs_file_size(long cid, int fd);

}
