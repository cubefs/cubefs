package io.chubao.fs;

import io.chubao.fs.CfsDriver.Dirent;

public class TestCfsClient {
    public static void main(String[] args) {
        CfsMount mnt = new CfsMount("/usr/lib/libcfs.so");
        long cid = mnt.NewClient();

        mnt.SetClient(cid, "volName", "ltptest");
        mnt.SetClient(cid, "masterAddr", "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010");
        mnt.SetClient(cid, "logDir", "/home/liushuoran/log");
        mnt.SetClient(cid, "logLeval", "info");
        int ret = mnt.StartClient(cid);
        if (ret < 0) {
            System.out.println("start client failed!!!");
            return;
        }

        if (args.length < 2) {
            System.out.println("Invalid args: lack of path");
            return;
        }

        String targetPath = args[1];

        if (args[0].equals("read")) {
            CfsDriver.StatInfo stat = new CfsDriver.StatInfo();
            ret = mnt.GetAttr(cid, targetPath, stat);
            if (ret < 0) {
                System.out.println("GetAttr failed: " + ret);
                return;
            }

            System.out.println("ino: " + stat.ino);
            System.out.println("size: " + stat.size);
            System.out.println("blocks: " + stat.blocks);
            System.out.println("nlink: " + stat.nlink);
            System.out.println("mode: " + stat.mode);
            System.out.println("uid: " + stat.uid);
            System.out.println("gid: " + stat.gid);

            int fd = mnt.Open(cid, targetPath, mnt.O_RDONLY, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            byte[] buf = new byte[4096];

            long readByte = mnt.Read(cid, fd, buf, buf.length, 0);

            System.out.println("fd: " + fd);
            System.out.println("read bytes: " + readByte);
            System.out.println(new String(buf));

            // verify md5sum
            byte[] toVerify = new byte[(int) readByte];
            System.arraycopy(buf, 0, toVerify, 0, (int) readByte);

            StringBuilder sb = new StringBuilder();
            java.security.MessageDigest md5 = null;
            try {
                md5 = java.security.MessageDigest.getInstance("MD5");
                md5.update(toVerify);
            } catch (java.security.NoSuchAlgorithmException e) {
            }
            if (md5 != null) {
                for (byte b : md5.digest()) {
                    sb.append(String.format("%02x", b));
                }
            }

            System.out.println("md5: " + sb.toString());

            mnt.Close(cid, fd);
        } else if (args[0].equals("write")) {
            int fd = mnt.Open(cid, targetPath, mnt.O_RDWR | mnt.O_CREAT, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            String toWrite = "abcdefg";
            byte[] buf = toWrite.getBytes();

            long writeBytes = mnt.Write(cid, fd, buf, buf.length, 0);

            System.out.println("write bytes: " + writeBytes);

            mnt.Close(cid, fd);
        } else if (args[0].equals("ls")) {
            int fd = mnt.Open(cid, targetPath, mnt.O_RDWR, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            Dirent dent = new Dirent();
            Dirent[] dents = (Dirent[]) dent.toArray(16);
            int n = mnt.Readdir(cid, fd, dents, 16);
            System.out.println("num of entries: " + n);
            for (int i = 0; i < n; i++) {
                System.out.println("ino: " + dents[i].ino + " | d_type: " + dents[i].dType);
            }

            mnt.Close(cid, fd);
        }

        mnt.CloseClient(cid);
    }
}
