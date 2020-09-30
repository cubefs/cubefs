import io.chubao.fs.CfsLibrary;
import io.chubao.fs.CfsMount;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.*;

public class UnitTest {
    static CfsMount mnt;

    @BeforeClass
    public static void beforeClass() {
        mnt = new CfsMount("/usr/lib/libcfs.so");
        mnt.setClient("volName", "ltptest");
        mnt.setClient("masterAddr", "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010");
        mnt.setClient("logDir", "/tmp/log");
        mnt.setClient("logLeval", "info");
        int ret = mnt.startClient();
        assertEquals(ret, 0);
    }


    @Test
    //This test is about test unlink
    public void testUnlink01() throws FileNotFoundException {
        int fd = mnt.open("/testUnlink.txt", CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);
        //judge
        int jg = mnt.unlink("testUnlink.txt");
        assertEquals(jg, 0);
    }


    @Test
    //This test is about creat a non-existent file in mnt.O_WRONLY|mnt.O_CREAT
    //judge Whether it has been created
    public void testOpen03() throws FileNotFoundException {
        String targetPath = "/test02.txt";
        //creat ana empty file test02.txt
        int fd1 = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd1 > 0);
        mnt.close(fd1);

        //open in mnt.O_RDONLY
        int fd2 = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);

        //judge
        assertTrue(fd2 > 0);
        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);

        mnt.close(fd2);
    }

    @Test
    //This test is about open a existent file in O_WRONLY
    //judge Whether it can be opened
    public void testOpen04() throws FileNotFoundException {
        String targetPath = "/test03.txt";
        //creat an empty file test03.txt
        int fd = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //open this file
        fd = mnt.open(targetPath, CfsMount.O_WRONLY, 0644);

        //judge
        assertTrue(fd > 0);
        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);
        mnt.close(fd);
    }


    @Test
    //This test is about open a existent file in RDWR|CREAT
    //judge whether it can be open
    public void testOpen06() throws FileNotFoundException {
        String targetPath = "/test05.txt";
        //creat an empty file
        int fd = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //open this file in mnt.O_RDWR|mnt.O_CREAT
        fd = mnt.open(targetPath, CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);

        //judge
        assertTrue(fd > 0);
        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);
        mnt.close(fd);
    }

    @Test
    //This test is about open a non-existent file in RDWR|CREAT
    public void testOpen07() throws FileNotFoundException {
        String targetPath = "/test06.txt";
        //creat a empty file test06.txt
        int fd = mnt.open(targetPath, CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //open this file
        fd = mnt.open(targetPath, CfsMount.O_RDWR, 0644);

        //judge
        assertTrue(fd > 0);
        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);
        mnt.close(fd);
    }

    @Test
    //This test is about open a 0 bytes file and read it content
    public void testRead08() throws FileNotFoundException {
        String targetPath = "test07.txt";
        //creat an empty file test07.txt
        int fd = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //open this empty file
        fd = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);
        assertTrue(fd > 0);

        //read its content
        byte[] buf = new byte[4096];
        long readBytes = mnt.read(fd, buf, buf.length, 0);
        int rB = (int) readBytes;
        String readContent = (new String(buf)).substring(0, rB);

        //judge
        assertEquals(0, readBytes);
        assertEquals(readContent, "");


        // verify md5sum
        byte[] toVerify = new byte[(int) readBytes];
        System.arraycopy(buf, 0, toVerify, 0, (int) readBytes);

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

        //System.out.println("md5: " + sb.toString());

        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);
        mnt.close(fd);

    }

    @Test
    //This test is about write 5 bytes content to exists file in WRONLY
    public void testWrite09() throws FileNotFoundException {
        String targetPath = "/test08.txt";
        //creat an empty file test08.txt
        int fd = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);


        //write "write" to this empty file
        String toWrite = "write";
        byte[] buf = toWrite.getBytes();
        long writeBytes = mnt.write(fd, buf, buf.length, 0);
        //System.out.println("writeBytes:"+writeBytes);
        mnt.close(fd);

        //read content of this file
        int fd1 = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);
        assertTrue(fd1 > 0);
        byte[] buf1 = new byte[4096];
        long readBytes = mnt.read(fd, buf1, buf1.length, 0);
        int rB = (int) readBytes;
        String readContent = (new String(buf1)).substring(0, rB);
        //System.out.println("readContent:"+readContent);

        //Judge
        assertEquals(readContent, toWrite);
        assertEquals(writeBytes, readBytes);

        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);
        mnt.close(fd);

    }

    @Test
    //This test is about open a 10 bytes file and read it offset=5 content
    public void testRead10() throws FileNotFoundException {
        //creat an empty file test09.txt
        String targetPath = "/test09.txt";
        int fd = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //write "0123456789" to this empty file
        String toWrite = "0123456789";
        byte[] buf1 = toWrite.getBytes();
        long writeBytes = mnt.write(fd, buf1, buf1.length, 0);
        mnt.close(fd);


        //Read test09.txt in offset=5
        int fd1 = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);
        byte[] buf2 = new byte[4096];
        long readBytes = mnt.read(fd1, buf2, buf2.length, 5);
        int rB = (int) readBytes;
        String readContent = (new String(buf2)).substring(0, rB);

        //judge
        assertEquals(readBytes, (writeBytes - 5));
        assertEquals(readContent, "56789");
        mnt.close(fd1);
        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);

    }


    @Test
    //This test is about open a  10 bytes file  in RDWR|APPEND
    //Judge Whether write  "append" will write over the  10 bytes content
    public void testWrite11() throws FileNotFoundException {
        String targetPath = "/test10.txt";
        //creat an empty file
        int fd = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //write 10 bites "0123456789" to this file
        String toWrite1 = "0123456789";
        byte[] buf1 = toWrite1.getBytes();
        long writeBytes1 = mnt.write(fd, buf1, buf1.length, 0);
        mnt.close(fd);

        //open this file in RDWR|APPEND,and write "append"
        int fd1 = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_APPEND, 0644);
        assertTrue(fd1 > 0);
        String toWrite2 = "append";
        byte[] buf2 = toWrite2.getBytes();
        long writeBytes2 = mnt.write(fd1, buf2, buf2.length, 0);
        mnt.close(fd1);

        //read this files
        int fd2 = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);
        byte[] buf3 = new byte[4096];
        long readBytes3 = mnt.read(fd2, buf3, buf3.length, writeBytes1);
        int rB = (int) readBytes3;
        String readContent = (new String(buf3)).substring(0, rB);
        // System.out.println(readContent);
        mnt.close(fd2);

        //judge
        assertEquals(readBytes3, writeBytes2);
        assertEquals(readContent, toWrite2);

        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);

    }


    @Test
    //This test is about open a 10 bytes file in WRONLY while offset=5
    //write 5 bytes and judge whether it write correct
    public void testWrite12() throws FileNotFoundException {
        String targetPath = "/test11.txt";

        //creat an empty file
        int fd = mnt.open(targetPath, CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //write 10 bites "0123456789" to this file
        String toWrite1 = "0123456789";
        byte[] buf1 = toWrite1.getBytes();
        long writeBytes1 = mnt.write(fd, buf1, buf1.length, 0);
        assertEquals(writeBytes1, 10);
        mnt.close(fd);

        //Open this file and write "write" in offset =5
        int fd1 = mnt.open(targetPath, CfsMount.O_WRONLY, 0644);
        assertTrue(fd1 > 0);
        String toWrite2 = "write";
        byte[] buf2 = toWrite2.getBytes();
        long writeBytes2 = mnt.write(fd1, buf2, buf2.length, 5);
        mnt.close(fd1);

        //Read this file offset=5
        int fd2 = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);
        byte[] buf3 = new byte[4096];
        long readBytes = mnt.read(fd2, buf3, buf3.length, 5);
        int rB = (int) readBytes;
        String readContent = (new String(buf3)).substring(0, rB);
        mnt.close(fd2);

        //judge
        assertEquals(readContent, toWrite2);
        assertEquals(readBytes, writeBytes2);

        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);

    }


    @Test
    //This test is about write 10 bytes content to exists file in RDWR|CREAT
    public void testWrite13() throws FileNotFoundException {
        String targetPath = "/test12.txt";
        //creat an empty file
        int fd = mnt.open(targetPath, CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);

        //write 10 bites "0123456789" to this file
        String toWrite1 = "0123456789";
        byte[] buf1 = toWrite1.getBytes();
        long writeBytes1 = mnt.write(fd, buf1, buf1.length, 0);
        assertEquals(writeBytes1, 10);
        mnt.close(fd);

        //Open this file and write "write"
        int fd1 = mnt.open(targetPath, CfsMount.O_WRONLY, 0644);
        assertTrue(fd1 > 0);
        String toWrite2 = "write";
        byte[] buf2 = toWrite2.getBytes();
        long writeBytes2 = mnt.write(fd, buf2, buf2.length, 0);
        assertEquals(writeBytes2, 5);
        mnt.close(fd1);

        //Read this file offset=0
        int fd2 = mnt.open(targetPath, CfsMount.O_RDONLY, 0644);
        byte[] buf3 = new byte[4096];
        long readBytes = mnt.read(fd2, buf3, buf3.length, 0);
        int rB = (int) readBytes;
        String readContent = (new String(buf3)).substring(0, rB);
        mnt.close(fd2);

        //judge
        assertEquals(readContent, toWrite2 + "56789");


        int ret = mnt.unlink(targetPath);
        assertEquals(ret, 0);

    }


    @Test
    //This test is about getCwd
    public void testGetCwd14() {
        String cwd = mnt.getcwd();
        assertEquals(cwd, "/");
    }

    @Test
    //This test is about mkdirs
    public void testMkdirs15() throws IOException {
        //make a dir testMkdir under "/"
        int jg = mnt.mkdirs("/testMkdirs", 0644);
        assertEquals(jg, 0);

        int jg2 = mnt.rmdir("/testMkdirs");
        assertEquals(jg2, 0);

    }

    @Test
    //This test is about rm an existent dir
    public void testRmdir16() throws IOException {
        //make a dir testMkdir under "/"
        int jg = mnt.mkdirs("/testRmdir", 0644);
        assertEquals(jg, 0);
        int jg2 = mnt.rmdir("/testRmdir");
        assertEquals(jg2, 0);

    }


    @Test
    //unlink
    //This test is about Chdir
    public void testChdir17() throws IOException {
        //current work dir is "/"
        String cwd1 = mnt.getcwd();
        assertEquals(cwd1, "/");

        //change cwd to "/testChdir"
        int jg1 = mnt.mkdirs("/testChdir", 0644);
        assertEquals(jg1, 0);
        int jg2 = mnt.chdir("/testChdir");
        assertEquals(jg2, 0);
        String cwd2 = mnt.getcwd();
        assertEquals(cwd2, "/testChdir");

        //restore the cwd
        int jg3 = mnt.chdir("/");
        assertEquals(jg3, 0);
        String cwd3 = mnt.getcwd();
        assertEquals(cwd3, "/");
        //mnt.unlink("/testChdir");

        int jg4 = mnt.rmdir("/testChdir");
        assertEquals(jg4, 0);

    }


    @Test
    //This test is about ls the directory entries in under /
    public void testLs18() throws IOException {
        //first make a dir testLs
        int fd1 = mnt.mkdirs("/testLs", 0644);
        //System.out.println(fd1);
        assertEquals(fd1, 0);
        mnt.close(fd1);
        //creat testLs01.txt,testLs02,testLs03 under /testLs
        int fd2 = mnt.open("/testLs/test01.txt", CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd2 > 0);
        mnt.close(fd2);
        int fd3 = mnt.open("/testLs/test02.txt", CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd3 > 0);
        mnt.close(fd3);
        int fd4 = mnt.open("/testLs/test03.txt", CfsMount.O_WRONLY | CfsMount.O_CREAT, 0644);
        assertTrue(fd4 > 0);
        mnt.close(fd4);

        //ls /testLs

        int fd5 = mnt.open("/testLs", CfsMount.O_RDWR, 0644);
        int totalEntry = 0;
        CfsLibrary.Dirent dent = new CfsLibrary.Dirent();
        CfsLibrary.Dirent[] dents = (CfsLibrary.Dirent[]) dent.toArray(2);
        for (; ; ) {
            int n = mnt.readdir(fd5, dents, 2);
            if (n <= 0) {
                break;
            }
            totalEntry += n;
        }

        //System.out.println(totalEntry);

        //judge
        assertEquals(totalEntry, 3);

        int ret1 = mnt.unlink("/testLs/test03.txt");
        assertEquals(ret1, 0);
        int ret2 = mnt.unlink("/testLs/test02.txt");
        assertEquals(ret2, 0);
        int ret3 = mnt.unlink("/testLs/test01.txt");
        assertEquals(ret3, 0);
        int ret4 = mnt.rmdir("/testLs");
        assertEquals(ret4, 0);
    }


    @Test
    //This test is about  test the SetAttr and getAttr
    public void testSetAttr19() throws FileNotFoundException {
        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        int ret = mnt.getAttr("/", stat);
        assertEquals(ret, 0);
        //System.out.println(stat.nlink);

        //make a stat as a sample
        stat.mtime = 1600000000;
        int judge = mnt.setAttr("/", stat, 8);
        assertEquals(judge, 0);

        CfsLibrary.StatInfo stat2 = new CfsLibrary.StatInfo();
        ret = mnt.getAttr("/", stat2);
        assertEquals(ret, 0);

        //judge
        assertEquals(stat.mtime, stat2.mtime);

    }

    @Test
    //This test is about test rename an existent dir
    public void testRename20() throws IOException {
        //make a dir /testRename
        int jg1 = mnt.mkdirs("/testRename", 0644);
        assertEquals(jg1, 0);

        //rename this dir
        int jg2 = mnt.rename("/testRename", "/testRename1");
        //System.out.println(jg2);
        assertEquals(jg2, 0);

        //judge
        int fd2 = mnt.open("/testRename1", CfsMount.O_RDONLY, 0644);
        assertTrue(fd2 > 0);
        int jg3 = mnt.rmdir("/testRename1");
        assertEquals(jg3, 0);
        mnt.close(fd2);
    }


    @Test
    //This test is about rename a file
    public void testRename21() throws FileNotFoundException {
        //creat a file
        int fd = mnt.open("/testRename.txt", CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);
        //rename this file
        int ret = mnt.rename("/testRename.txt", "/hello.txt");
        assertEquals(ret, 0);
        int fd2 = mnt.open("/hello.txt", CfsMount.O_RDONLY, 0644);
        assertTrue(fd2 > 0);

        int jg = mnt.unlink("/hello.txt");
        assertEquals(jg, 0);
    }

    @Test
    //This test is about creat a directory and all parents
    public void testMkdirs22() throws IOException {
        int jg1 = mnt.mkdirs("/testMkdirs/test", 0644);
        assertEquals(jg1, 0);
        int jg2 = mnt.rmdir("/testMkdirs/test");
        assertEquals(jg2, 0);
        int jg3 = mnt.rmdir("/testMkdirs");
        assertEquals(jg3, 0);
    }

    @Test
    //This test is about fchmod
    public void testFchmodChmod23() throws FileNotFoundException {
        //creat a file in mode 0644
        int fd = mnt.open("/testFchmod.txt", CfsMount.O_RDWR | CfsMount.O_CREAT, 0644);
        assertTrue(fd > 0);
        //change the mode to 0777
        int jg = mnt.fchmod(fd, 0777);
        assertEquals(jg, 0);
        mnt.close(fd);

        int jg1 = mnt.unlink("/testFchmod.txt");
        assertEquals(jg1, 0);
    }

    @AfterClass
    //close the Client
    public static void closeClient() {
        mnt.closeClient();
    }

}