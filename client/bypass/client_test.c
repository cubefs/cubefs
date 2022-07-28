#define _GNU_SOURCE
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define clean_errno() (errno == 0 ? "None" : strerror(errno))
#define log_error(M, ...) fprintf(stderr, "[ERROR] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, clean_errno(), ##__VA_ARGS__)
#define assertf(A, M, ...) if(!(A)) {log_error(M, ##__VA_ARGS__); assert(A); }

#define FD_MASK (1 << (sizeof(int)*8 - 2))

void testOp(bool is_cfs, bool ignore);
void testReload();
void testDup(bool is_cfs);
int main(int argc, char **argv) {
    bool is_cfs = true;
    bool ignore = false;
    int num = 1;
    int c;
    const char* mount;
    while((c = getopt(argc, argv, "lin:h")) != -1)
    switch(c) {
        case 'l':
        is_cfs = false;
        break;
        case 'i':
        ignore = true;
        break;
        case 'n':
        num = atoi(optarg);
        break;
        case 'h':
        printf("There are three test modes: local(-l), CFS, CFS but not hook(-i).\n-l\n  local (default CFS)\n-i\n  ignore hook (default false)\n-n num\n  execute num times (default 1)\n");
        return 0;
    }
    if(is_cfs) {
        const char *ld = getenv("LD_PRELOAD");
        const char *config = getenv("CFS_CONFIG_PATH");
        mount = getenv("CFS_MOUNT_POINT");
        if(ld == NULL || config == NULL || mount == NULL) {
            printf("execute with LD_PRELOAD=libcfsclient.so CFS_CONFIG_PATH= CFS_MOUNT_POINT=\n");
            return -1;
        }
    }

    time_t raw_time;
    struct tm *ptm;
    char buf[20];
    const int count = is_cfs && !ignore ? 100 : 100000;
    for(int i = 0; i < num; i++) {
        testOp(is_cfs, ignore);
        if(i >= count && i % count == 0) {
            raw_time = time(NULL);
            ptm = localtime(&raw_time);
            strftime(buf, 20, "%F %H:%M:%S", ptm);
            printf("%s testOp for %d times\n", buf, i);
        }
    }
    printf("Finish testOp for %d times.\n", num);
    testReload();
    setenv("CFS_MOUNT_POINT", mount, 1);
    testDup(is_cfs);
    printf("Finish testDup\n");
    printf("Finish test, press ctrl+c to quit...\n");
    getchar();
}

void testReload() {
    printf("Test update libcfssdk.so. Please waiting finish...\n");
    setenv("RELOAD_CLIENT", "test", 1);
    sleep(20);
}

void testOp(bool is_cfs, bool ignore) {
    #define PATH_LEN 100
    char cwd[PATH_LEN];      // root for this test
    char dir[PATH_LEN];      // temp dir
    char path[PATH_LEN];     // reame source file
    char new_path[PATH_LEN]; // rename to file
    memset(cwd, '\0', PATH_LEN);
    memset(dir, '\0', PATH_LEN);
    memset(path, '\0', PATH_LEN);
    memset(new_path, '\0', PATH_LEN);
    const char *tdir = "t";
    const char *file = "tmp123";
    const char *new_file = "tmp1234";
    if(is_cfs) {
        const char *mount = getenv("CFS_MOUNT_POINT");
        assertf(mount, "env CFS_MOUNT_POINT not exists");
        strcat(cwd, mount);
    } else {
        assertf(getcwd(cwd, PATH_LEN), "getcwd returning NULL");
    }
    strcat(dir, cwd);
    strcat(dir, "/");
    strcat(dir, tdir);
    strcat(path, dir);
    strcat(path, "/");
    strcat(path, file);
    strcat(new_path, dir);
    strcat(new_path, "/");
    strcat(new_path, new_file);

    #define LEN 2
    char wbuf[LEN] = "a";
    char rbuf[LEN];
    memset(rbuf, '\0', LEN);
    int fd;
    int dir_fd;
    int tmp_fd;
    int re;
    ssize_t size;
    off_t off;

    unlink(path);
    rmdir(dir);

    // chdir operations
    char tmp_buf[100];
    memset(tmp_buf, '\0', 100);
    //buf is not enough for the cwd
    char *tmp_dir = getcwd(tmp_buf, 1);
    assertf(tmp_dir == NULL, "getcwd returing %s", tmp_dir);
    re = mkdir(dir, 0775);
    assertf(re == 0, "mkdir %s returning %d", dir, re);
    dir_fd = open(dir, O_RDWR | O_PATH | O_DIRECTORY);
    assertf(!ignore && is_cfs ? dir_fd & FD_MASK : dir_fd, "open dir %s returning %d", dir, tmp_fd);
    re = chdir(cwd);
    assertf(re == 0, "chdir %s returning %d", cwd, re);
    re = chdir(tdir);
    assertf(re == 0, "chdir %s returning %d", tmp_dir, re);
    tmp_dir = getcwd(NULL, 50);
    assertf(tmp_dir != NULL && !strcmp(tmp_dir, dir), "getcwd returning %s, len: %d, expect: %s", tmp_dir, strlen(tmp_dir), dir);
    free(tmp_dir);
    re = fchdir(dir_fd);
    assertf(re == 0, "fchdir %d returning %d", dirfd, re);
    tmp_dir = getcwd(NULL, 50);
    assertf(tmp_dir != NULL && !strcmp(tmp_dir, dir), "getcwd returning %s, len: %d", tmp_dir, strlen(tmp_dir));
    free(tmp_dir);

    // readdir operations
    fd = openat(dir_fd, file, O_RDWR | O_CREAT, 0664);
    assertf(!ignore && is_cfs ? fd & FD_MASK : fd, "openat %s returning %d", path, fd);
    close(fd);
    fd = openat(dir_fd, file, O_RDWR | O_CREAT | O_EXCL);
    assertf(fd == -1 && errno == EEXIST, "openat %s returning %d", path, fd);
    DIR *dirp = opendir(dir);
    assertf(dirp != NULL, "opendir %s returning NULL", dir);
    struct dirent *dp;
    while((dp = readdir(dirp)) && (!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, "..")));
    assertf(dp != NULL && !strcmp(dp->d_name, file), "readdir returning %s", dp == NULL ? "" : dp->d_name);
    dp = readdir(dirp);
    assertf(dp == NULL, "readdir errno %d", errno);
    re = closedir(dirp);
    assertf(re == 0, "closedir returning %d", re);

    // file operations
    fd = open(file, O_RDWR);
    assertf(!ignore && is_cfs ? fd & FD_MASK : fd, "open %s returning %d", path, fd);
    re = renameat(dir_fd, file, dir_fd, new_file);
    assertf(re == 0, "renameat firfd %d %s to %s returning %d", dirfd, file, new_file, re);
    tmp_fd = open(path, O_RDONLY);
    assertf(tmp_fd < 0, "open %s after rename with O_RDONLY returning %d", path, tmp_fd);
    re = rename(new_path, path);
    assertf(re == 0, "rename %s to %s returning %d", path, new_path, re);

    // read & write
    size = write(fd, wbuf, LEN-1);
    assertf(size == LEN-1, "write %s to %s returning %d", wbuf, path, size);
    size = read(fd, rbuf, LEN-1);
    assertf(size == 0, "read %s from %s after write returning %d", rbuf, path, size);
    off = lseek(fd, 0, SEEK_SET);
    assertf(off == 0, "lseek returning %d", off);
    size = read(fd, rbuf, LEN-1);
    assertf(size == LEN-1 && strncmp(wbuf, rbuf, LEN-1) == 0,
            "read %s from %s after write returning %d", rbuf, path, size);

    // file attributes
    // CFS time precision is second, tv_nsec should be 0
    struct timespec ts[2] = {{1605668000, 0}, {1605668001, 0}};
    re = utimensat(dir_fd, file, ts, 0);
    assertf(re == 0, "utimensat %s at dir fd %d returning %d", file, dir_fd, re);
    re = chmod(path, 0611);
    assertf(re == 0, "chmod %s returning %d", path, re);
    struct stat statbuf;
    re = stat(path, &statbuf);
    // access time is updated in metanode when accessing inode, inconsistent with client inode cache
    bool atim_valid = !ignore && is_cfs ?
        ts[0].tv_sec < statbuf.st_atime:
        !memcmp((void*)&ts[0].tv_sec, (void*)&statbuf.st_atime, sizeof(time_t));
    assertf(re == 0 && statbuf.st_size == LEN-1
    //      && atim_valid
            && !memcmp((void*)&ts[1].tv_sec, (void*)&statbuf.st_mtime, sizeof(time_t))
            && statbuf.st_mode == S_IFREG | 0611,
            "stat %s returning %d, size: %d, mode: %o", path, re, statbuf.st_size, statbuf.st_mode);

    // chdir to original cwd, in case of calling test() for many times
    re = chdir(cwd);
    assertf(re == 0, "chdir %s returning %d", cwd, re);
    tmp_dir = getcwd(NULL, 50);
    assertf(tmp_dir != NULL && !strcmp(tmp_dir, cwd), "getcwd returning %s, len: %d", tmp_dir, strlen(tmp_dir));
    free(tmp_dir);

    // cleaning
    re = close(dir_fd);
    assertf(re == 0, "close dir fd %d returning %d", fd, re);
    re = close(fd);
    assertf(re == 0, "close fd %d returning %d", fd, re);
    re = lseek(fd, 0, SEEK_SET);
    assertf(re < 0, "lseek closed fd %d returning %d", fd, re);
    re = unlink(path);
    assertf(re == 0, "unlink %s returning %d", path, re);
    tmp_fd = open(path, O_RDONLY);
    assertf(tmp_fd < 0, "open unlinked %s wirt O_RDONLY returning %d", path, tmp_fd);
    re = rmdir(dir);
    assertf(re == 0, "rmdir %s returning %d", dir, re);
    dir_fd = open(dir, O_RDONLY | O_PATH | O_DIRECTORY);
    assertf(dir_fd < 0, "open removed dir %s returning %d", dir, dir_fd);
}

void testDup(bool is_cfs) {
    #define PATH_LEN 100
    char *mount = getenv("CFS_MOUNT_POINT");
    char *dir = "dir";
    char *file = "file1";
    off_t off;
    int dirfd, fd, newfd1, newfd2;
    ssize_t size;
    int res;

    dirfd = open(dir, O_RDWR | O_PATH | O_DIRECTORY);
    assertf(dirfd > 0, "open dir %s returning %d", dir, dirfd);
    fd = openat(dirfd, file, O_RDWR | O_CREAT, 0664);
    assertf(fd > 0, "open %s/dir/file1 returning %d", mount, fd);
    size = write(fd, "test", 4);
    assertf(size == 4, "write test to fd returning %d, expect 4", size);
    newfd2 = dup2(fd, 100);
    assertf(newfd2 == 100, "dup2 fd %d returning %d, expect 100", fd, newfd2);
    off = lseek(newfd2, 0, SEEK_CUR);
    assertf(off == 4, "lseek returning %d, expect 4", off);

    res = close(fd);
    assertf(res == 0, "close fd %d returning %d, expect 0", fd, res);

    newfd1 = fcntl(newfd2, F_DUPFD, 200);
    assertf(newfd1 >= 200, "fcntl dup fd %d returning %d, expect 200", fd, newfd1);
    size = write(fd, "test", 4);
    assertf(size == 4, "write test to fd returning %d, expect 4", size);
    size = write(newfd2, "test", 4);
    assertf(size == 4, "write test to fd returning %d, expect 4", size);

    off = lseek(newfd1, 0, SEEK_CUR);
    assertf(off == 12, "lseek returning %d, expect 4", off);

    res = close(newfd1);
    assertf(res == 0, "close fd %d returning %d, expect 0", newfd1, res);

    size = write(newfd2, "test", 4);
    assertf(size == 4, "write test to fd returning %d, expect 4", size);

    res = close(newfd2);
    assertf(res == 0, "close fd %d returning %d, expect 0", newfd1, res);

    size = write(fd, "test", 4);
    assertf(size == -1, "write test to close fd returning %d, expect -1", size);
}
