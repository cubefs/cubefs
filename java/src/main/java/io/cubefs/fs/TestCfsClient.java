package io.cubefs.fs;

import java.io.FileNotFoundException;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;

public class TestCfsClient {
    public static void main(String[] args) throws FileNotFoundException {
        CfsMount mnt = new CfsMount();

        mnt.setClient("volName", "ltptest");
        mnt.setClient("masterAddr", "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010");
        mnt.setClient("logDir", "/home/liushuoran/log");
        mnt.setClient("logLeval", "info");
        int ret = mnt.startClient();
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
            CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
            ret = mnt.getAttr(targetPath, stat);
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

            int fd = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            byte[] buf = new byte[4096];

            long readByte = mnt.read(fd, buf, buf.length, 0);

            System.out.println("fd: " + fd);
            System.out.println("read bytes: " + readByte);
            System.out.println(new String(buf));

            // verify md5sum
            byte[] toVerify = new byte[(int) readByte];
            System.arraycopy(buf, 0, toVerify, 0, (int) readByte);
            String key = "MySecretKey";
            String strToVerify  = new String(toVerify, StandardCharsets.UTF_8);
            //StringBuilder sb = new StringBuilder();
            //java.security.MessageDigest md5 = null;
            try {
                 byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                 MessageDigest sha = MessageDigest.getInstance("SHA-256");
                 keyBytes = sha.digest(keyBytes);
                 keyBytes = Arrays.copyOf(keyBytes, 16);

                 SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, "AES");
                 Cipher cipher = Cipher.getInstance("AES");
                 cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);

                  byte[] encryptedBytes = cipher.doFinal(strToVerify.getBytes(StandardCharsets.UTF_8));
                  String encryptedString = Base64.getEncoder().encodeToString(encryptedBytes);
                  System.out.println("md5: " + encryptedString);
            } catch (Exception e) {
                e.printStackTrace();
            }


            mnt.close(fd);
        } else if (args[0].equals("write")) {
            int fd = mnt.open(targetPath, CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            String toWrite = "abcdefg";
            byte[] buf = toWrite.getBytes();

            long writeBytes = mnt.write(fd, buf, buf.length, 0);

            System.out.println("write bytes: " + writeBytes);

            mnt.close(fd);
        } else if (args[0].equals("ls")) {
            mnt.chdir(targetPath);
            int fd = mnt.open(".", CfsMount.O_RDWR, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            int total_entry = 0;
            CfsLibrary.Dirent dent = new CfsLibrary.Dirent();
            CfsLibrary.Dirent[] dents = (CfsLibrary.Dirent[]) dent.toArray(2);
            for (;;) {
                int n = mnt.readdir(fd, dents, 2);
                if (n <= 0) {
                    break;
                }
                total_entry += n;
                for (int i = 0; i < n; i++) {
                    System.out.println("ino: " + dents[i].ino + " | d_type: " + dents[i].dType);
                }
            }
            System.out.println("num of entries: " + total_entry);
            mnt.close(fd);
        } else if (args[0].equals("chmod")) {
            int fd = mnt.open(targetPath, CfsMount.O_RDWR, 0644);
            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            ret = mnt.fchmod(fd, 0666);
            if (ret < 0) {
                System.out.println("Fchmod failed: " + ret);
                return;
            }
            CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
            ret = mnt.getAttr(targetPath, stat);
            System.out.println("mode: " + stat.mode);
            mnt.close(fd);
        }

        mnt.closeClient();
    }
}
