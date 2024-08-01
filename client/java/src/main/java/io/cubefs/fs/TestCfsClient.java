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
        mnt.setClient("logDir", "/data1/cfsJavaClient/log");
        mnt.setClient("logLevel", "debug");
        mnt.setClient("accessKey", "jy624wtKbHUER6ZV");
        mnt.setClient("secretKey", "rwzHcpIqsxnHjK510NxjPvPtCisl78sG");
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
            testRead(mnt, targetPath);
        } else if (args[0].equals("write")) {
            testWrite(mnt, targetPath);
        } else if (args[0].equals("ls")) {
            testReadDir(mnt, targetPath);
        } else if (args[0].equals("chmod")) {
            testChmod(mnt, targetPath);
        } else if (args[0].equals("unlink")) {
            testUnlink(mnt, targetPath);
        } else if (args[0].equals("rmdir")) {
            testRmdir(mnt, targetPath);
        } else if (args[0].equals("mkdir")) {
             testMkdirs(mnt, targetPath, 0751);
        } else if (args[0].equals("rename")) {
            if (args.length < 2) {
                System.out.println("Invalid args: lack of path");
                return;
            }

            testRename(mnt, targetPath, args[2]);
        }

        mnt.closeClient();
    }

    private static void testRead(CfsMount mnt, String targetPath) throws FileNotFoundException {
        System.out.println("to test read file: " + targetPath + "\n");

           CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
            int ret = mnt.getAttr(targetPath, stat);
            if (ret < 0) {
                System.out.println("GetAttr failed: " + ret);
                return;
            }

            System.out.println("file attr:");
            System.out.println("  ino: " + stat.ino);
            System.out.println("  size: " + stat.size);
            System.out.println("  blocks: " + stat.blocks);
            System.out.println("  nlink: " + stat.nlink);
            System.out.println("  mode: " + stat.mode);
            System.out.println("  uid: " + stat.uid);
            System.out.println("  gid: " + stat.gid);
            System.out.println("\n");

            int fd = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);

            if (fd < 0) {
                System.out.println("Open failed: " + fd);
                return;
            }

            byte[] buf = new byte[4096];

            long readByte = mnt.read(fd, buf, buf.length, 0);

            System.out.println("fd: " + fd);
            System.out.println("read bytes: " + readByte);
            System.out.println("read content: " + new String(buf));

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
                  System.out.println("file MD5: " + encryptedString);
            } catch (Exception e) {
                e.printStackTrace();
            }

            mnt.close(fd);
        }

    private static void testWrite(CfsMount mnt, String targetPath) throws FileNotFoundException {
       System.out.println("to test write to file: " + targetPath + "\n");

       int fd = mnt.open(targetPath, CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);
       if (fd < 0) {
           System.out.println("Open failed: " + fd);
           return;
       }

       String toWrite = "abcdefg";
       byte[] buf = toWrite.getBytes();
       long writeBytes = mnt.write(fd, buf, buf.length, 0);

       System.out.println("writed content: " + toWrite);
       System.out.println("writed bytes: " + writeBytes);

       mnt.close(fd);
   }

   private static void testReadDir(CfsMount mnt, String targetPath) throws FileNotFoundException {
       System.out.println("to test ls path: " + targetPath + "\n");

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
               System.out.println("name: " + new String(dents[i].name) + " | Ino: " + dents[i].ino + " | d_type: " + dents[i].dType);
           }
       }
       System.out.println("\nnum of entries: " + total_entry);
       mnt.close(fd);
   }

    private static void testChmod(CfsMount mnt, String targetPath) throws FileNotFoundException {
        System.out.println("to test chmod, path: " + targetPath + "\n");

        int fd = mnt.open(targetPath, CfsMount.O_RDWR, 0644);
        if (fd < 0) {
            System.out.println("Open failed: " + fd);
            return;
        }

        CfsLibrary.StatInfo orgStat = new CfsLibrary.StatInfo();
        int ret = mnt.getAttr(targetPath, orgStat);
        if (ret < 0) {
            System.out.println("before chmod, getAttr failed: " + ret);
            return;
        }
        System.out.printf("original mode:%o\n\n ", orgStat.mode);

        ret = mnt.fchmod(fd, 0666);
        if (ret < 0) {
            System.out.println("Fchmod failed: " + ret);
            return;
        }

        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        ret = mnt.getAttr(targetPath, stat);
        if (ret < 0) {
            System.out.println("after chmod, getAttr failed: " + ret);
            return;
        }
        System.out.printf("after chmod, mode: %o\n", stat.mode);

        mnt.close(fd);
    }

    private static void testUnlink(CfsMount mnt, String targetPath) throws FileNotFoundException {
            if (targetPath.equals("/")) {
                System.out.println("invalid argument: can not unlink root dir");
                return;
            }

        System.out.println("to test unlink, path: " + targetPath + "\n");

        int ret = mnt.unlink(targetPath);
        if (ret < 0) {
            System.out.println("unlink failed: " + ret);
            return;
        }

        System.out.println("unlink done");
    }

    private static void testRmdir(CfsMount mnt, String targetPath) throws FileNotFoundException {
        if (targetPath.equals("/")) {
            System.out.println("invalid argument: can not rm root dir");
            return;
        }

        System.out.println("to test rmdir, path: " + targetPath + "\n");

        int ret = mnt.rmdir(targetPath);
        if (ret < 0) {
            System.out.println("rmdir failed: " + ret);
            return;
        }

        System.out.println("rmdir done");
    }

    private static void testMkdirs(CfsMount mnt, String targetPath, int mode) throws FileNotFoundException {
        if (targetPath.equals("/")) {
            System.out.println("invalid argument: can make root dir");
            return;
        }

        System.out.println("to test mkdir, path: " + targetPath + "\n");

        try {
            int ret = mnt.mkdirs(targetPath, mode);
        } catch (Exception e)  {
            System.out.println("mkdir failed: " + e);
            return;
        }

        System.out.println("mkdir done");
    }

    private static void testRename(CfsMount mnt, String from, String to) throws FileNotFoundException {
        System.out.println("to test rename, from [ " + from + " ] to [ " + to + " ]\n");

        int ret = mnt.rename(from, to);
        if (ret < 0) {
            System.out.println("rename failed: " + ret);
            return;
        }

        System.out.println("rename done");
    }

}
