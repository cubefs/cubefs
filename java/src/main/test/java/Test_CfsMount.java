import io.chubao.fs.CfsLibrary;
import io.chubao.fs.CfsMount;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Test_CfsMount {
   static  String mountPoint ="/cfs/mnt";
   static  CfsMount mnt;

    @BeforeClass
    public static void beforeClass() {
       mnt = new CfsMount("/usr/lib/libcfs.so");
        mnt.SetClient("volName", "ltptest");
        mnt.SetClient("masterAddr", "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010");
        mnt.SetClient("logDir", "/home/zhangxiangping/log");
        mnt.SetClient("logLeval", "info");
        int ret = mnt.StartClient();
        IniMountPoint(mountPoint);
        }
    //Initialization test conditions, mainly about file initialization
    private static void IniMountPoint(String mountPoint) {
        //Initial mount point
        delDir(mountPoint);
        mkdirs(mountPoint);

        //Initial open_test and files for OpenTest
        mkdir(mountPoint+"/open_test");
        writeTxt(mountPoint+"/open_test","test01","0123456789");
        writeTxt(mountPoint+"/open_test","test04","0123456789");
        writeTxt(mountPoint+"/open_test","test06","0123456789");
        writeTxt(mountPoint+"/open_test","test07","0123456789");

        //Initial read_test and files for ReadTest
        mkdir(mountPoint+"/read_test");
        writeTxt(mountPoint+"/read_test","test01","0123456789");
        writeTxt(mountPoint+"/read_test","test02","0123456789");

        //Initial write_test and files for WriteTest
        mkdir(mountPoint+"/write_test");
        writeTxt(mountPoint+"/write_test","test01","01234567890123456789");
        writeTxt(mountPoint+"/write_test","test02","01234567890123456789");
        writeTxt(mountPoint+"/write_test","test03","01234567890123456789");

        //Initial ls_test and files for LsTest
        mkdir(mountPoint+"/ls_test");
        writeTxt(mountPoint+"/ls_test","test011","01234567890123456789");
        writeTxt(mountPoint+"/ls_test","test012","01234567890123456789");

        //Initial setAttr_test for SetAttrTest
        mkdir(mountPoint+"/setAttr_test");

    }
    //creat txt file
    private static void writeTxt(String targetPath, String title, String content) {
        try {
            /* make a txt*/
            File file = new File(targetPath);// 相对路径，如果没有则要建立一个新的output.txt文件
            if (!file.exists()) {
                file.mkdirs();
            }
            file = new File(targetPath + "/" + title + ".txt");// 相对路径，如果没有则要建立一个新的output。txt文件
            file.createNewFile(); // make a new file
            BufferedWriter out = new BufferedWriter(new FileWriter(file));
            out.write(content); // \r\n
            out.flush(); //write to file
            out.close(); // close the file

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    //creat directory
    private static void mkdir(String targetPath) {
        File file=new File(targetPath);
        try{
            if(!file.exists()){
                file.mkdir();
            }
        }catch(Exception e){
            System.out.println("creat directory failed");
            e.printStackTrace();
        }

    }
    //Recursively create directory
    private static void mkdirs(String targetPath) {
        File file=new File(targetPath);
        try{
            if(!file.exists()){
                file.mkdir();
            }
        }catch(Exception e){
            System.out.println("creat directory failed");
            e.printStackTrace();
        }
    }
    //delete  all the file it contains
    private static void delDir(String targetPath) {
        File file = new File(targetPath);
        delAllFile(file);

    }
    // delete all the file bellow this folder
    private static void delAllFile(File directory) {
        if (!directory.isDirectory()){
        directory.delete();
    } else{
        File [] files = directory.listFiles();

        // empty file
        if (files.length == 0){
            directory.delete();
            //System.out.println("delete" + directory.getAbsolutePath());
            return;
        }

        // delete subfolder and all files
        for (File file : files){
            if (file.isDirectory()){
                delAllFile(file);
            } else {
                file.delete();
                // System.out.println("delete" + file.getAbsolutePath());
            }
        }

        // delete folder itself
        directory.delete();
        //System.out.println("delete" + directory.getAbsolutePath());
    }
    }
    //get filesize
    private static long getFileSize(String targetPath) {
        File file = new File(targetPath);
        long filesize =file.length();

        return filesize;
    }

    @Test
    public void testOpen01(){

        String targetPath="/open_test/test01.txt";
        int fd =mnt.Open(targetPath,mnt.O_WRONLY,0644);
        assertTrue(fd>0);
        mnt.Close(fd);
    }

  @Test
    //This test is about open a non-existent file in O_WRONLY
    //judge Whether it can be opened
    public void testOpen02(){
        String targetPath="/open_test/test02.txt";
        int fd =mnt.Open(targetPath,mnt.O_WRONLY,0644);
        assertTrue(fd<0);
        mnt.Close(fd);
    }

    @Test
    //This test is about open a non-existent file in RDWR|APPEND
    public void testOpen03(){
        //This test is about open a non-existent file in RDWR|APPEND
        String targetPath="/open_test/test03.txt";
        int fd =mnt.Open(targetPath,mnt.O_RDWR|mnt.O_APPEND,0644);
        assertTrue(fd<0);
        mnt.Close(fd);
    }

    @Test
    //This test is about open a existent file in RDWR|CREAT
    public void testOpen04(){
        //This test is about open a file in RDWR|CREAT
        String targetPath="/open_test/test04.txt";
        int fd =mnt.Open(targetPath,mnt.O_RDWR|mnt.O_CREAT,0644);
        assertTrue(fd>0);
        mnt.Close(fd);
    }

    @Test
    //This test is about open a non-existent file in RDWR|CREAT
    public void testOpen05(){
        //This test is about open a file in RDWR|CREAT
        String targetPath="/open_test/test05.txt";
        int fd =mnt.Open(targetPath,mnt.O_RDWR|mnt.O_CREAT,0644);
        assertTrue(fd>0);
        mnt.Close(fd);
    }

    @Test
    //This test is about open a  10 Bytes  in RDWR|APPEND
    //Judge Whether write  "append" will write over the  10 bytes content
    //still has bug
    public void testOpen06(){
        //This test is about open a  100 Bytes  in RDWR|APPEND
        //JUDGE Whether write "append" will write over the  100 bytes content
        String targetPath="/open_test/test06.txt";
        int fd =mnt.Open(targetPath,mnt.O_WRONLY|mnt.O_APPEND,0644);
        assertTrue(!(fd<0));
        String toWrite="append";
        byte[] buf =toWrite.getBytes();
        //size1 present the filesize before write
        long size1 =getFileSize("/cfs/mnt/open_test/test06.txt");
        //System.out.println(size1);
        long writeBytes=mnt.Write(fd,buf,buf.length,0);

        //I want to compare (write_buf + original size = = size after writing), but writing is always after getting the file size.
        // System.out.println(writeBytes);
        //try {
        //   Thread.sleep(30000);
        //}catch(Exception e)
        //{System.exit(0);}
        //size2 present the filesize after write
        //long size2 =getFileSize("/cfs/mnt/open_test/test06.txt");
        //System.out.println(size2);
        mnt.Close(fd);
        assertTrue((buf.length+size1)==(size1+writeBytes));
    }

    @Test
    //This test is about open a 10 bytes file in WRONLY|TRUNC
    //judge Whether  the file content will clear
    public void testOpen07(){
        //This test is about open a 10 bytes file in WRONLY|TRUNC
        //judge Whether  the file content will clear
        String targetPath="/open_test/test07.txt";
        int fd =mnt.Open(targetPath,mnt.O_WRONLY|mnt.O_TRUNC,0644);
        assertTrue(!(fd<0));
        //try {
        //  Thread.sleep(5000);
        //}catch(Exception e)
        // {System.exit(0);}
        //size present the filesize after open
        long size =getFileSize("/cfs/mnt/open_test/test07.txt");
        //System.out.println(size);
        assertTrue(size==0);
        mnt.Close(fd);

    }

    @Test
    //This test is about open a 10 bytes file and read it content
    public  void testRead01(){
        String targetPath="/read_test/test01.txt";

        long size=getFileSize(mountPoint+targetPath);
        //System.out.println(size);
        int fd = mnt.Open(targetPath, mnt.O_RDONLY, 0644);
        assertTrue (!(fd < 0));
        byte[] buf = new byte[4096];
        long readByte = mnt.Read(fd, buf, buf.length, 0);
        //System.out.println(readByte);
        assertTrue(size==readByte);


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

        //System.out.println("md5: " + sb.toString());

        mnt.Close(fd);

    }

    @Test
    //This test is about open a 10 bytes file and read it offset content
    public  void testRead02(){
        String targetPath="/read_test/test02.txt";

        long size=getFileSize(mountPoint+targetPath);
        //System.out.println(size);
        int fd = mnt.Open(targetPath, mnt.O_RDONLY, 0644);
        assertTrue(!(fd < 0));
        byte[] buf = new byte[4096];
        long readByte = mnt.Read(fd, buf, buf.length, 5);
        //System.out.println(readByte);
        assertTrue(size==(readByte+5));
       // System.out.println(new String(buf));

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

        //System.out.println("md5: " + sb.toString());

        mnt.Close(fd);

    }

    @Test
    //This test is about write content to exists file in WRONLY
    public void testWrite01(){
        String targetPath="/write_test/test01.txt";
        long size=getFileSize(mountPoint+targetPath);
        //System.out.println(size);
        int fd=mnt.Open(targetPath, mnt.O_WRONLY, 0644);
        assertTrue(!(fd<0));
        String toWrite = "write";
        byte[] buf = toWrite.getBytes();
        long writeBytes = mnt.Write(fd, buf, buf.length, 0);
        //System.out.println(writeBytes);
        mnt.Close(fd);
        long size1=getFileSize(mountPoint+targetPath);
        //System.out.println(size1);
        assertTrue(size1==size);

    }

    @Test
    //This test is about write content to exists file in WRONLY while offset=5
    public void testWrite02(){
        String targetPath="/write_test/test02.txt";
        long size=getFileSize(mountPoint+targetPath);
        //System.out.println(size);
        int fd=mnt.Open(targetPath, mnt.O_WRONLY, 0644);
        assertTrue(!(fd<0));
        String toWrite = "write";
        byte[] buf = toWrite.getBytes();
        long writeBytes = mnt.Write(fd, buf, buf.length, 5);
        //System.out.println(writeBytes);
        mnt.Close(fd);
        long size1=getFileSize(mountPoint+targetPath);
        //System.out.println(size1);
        assertTrue(size1==size);
    }

    @Test
    //This test is about write content to exists file in WRONLY|APPEND
    public void testWrite03(){
        String targetPath="/write_test/test03.txt";
        long size=getFileSize(mountPoint+targetPath);
        //System.out.println(size);
        int fd=mnt.Open(targetPath, mnt.O_WRONLY|mnt.O_APPEND, 0644);
        assertTrue(!(fd<0));
        String toWrite = "write";
        byte[] buf = toWrite.getBytes();
        long writeBytes = mnt.Write(fd, buf, buf.length, 0);
        //System.out.println(writeBytes);

        //long size1=getFileSize(mountPoint+targetPath);
        //System.out.println(size1);
        mnt.Close(fd);
        assertTrue(25==(size+writeBytes));
    }

    @Test
    //This test is about ls the directory entries in the current directory
    public void testLs01(){
        int judge=mnt.Chdir("/ls_test");
        assertTrue(!(judge<0));
        int fd = mnt.Open(".", mnt.O_RDWR, 0644);

        assertTrue(!(fd < 0));

        int total_entry = 0;
        CfsLibrary.Dirent dent = new CfsLibrary.Dirent();
        CfsLibrary.Dirent[] dents = (CfsLibrary.Dirent[]) dent.toArray(2);
        for (;;) {
            int n = mnt.Readdir(fd, dents, 2);
            if (n <= 0) {
                break;
            }
            total_entry += n;
            //for (int i = 0; i < n; i++) {
            //   System.out.println("ino: " + dents[i].ino + " | d_type: " + dents[i].dType);
            // }
        }
        assertTrue(total_entry==2);

        mnt.Close(fd);
        judge=mnt.Chdir("/");
        if(judge<0){
            System.out.println("Chdir failed:"+judge);
            return;
        }
    }

    @Test
    //This test is about ls a non-existent directory
    public void testLs02(){
        int judge=mnt.Chdir("/ls_test1");
        assertTrue(judge<0);
    }

    @Test
    //This test is about  test the SetAttr
    public void testSetAttr(){
        CfsLibrary.StatInfo stat = new CfsLibrary.StatInfo();
        int ret = mnt.GetAttr("/setAttr_test", stat);
        //stat is a sample
        stat.mtime=1600000000;

        int judge =mnt.SetAttr("/setAttr_test",stat,8);//let targetpath.stat=stat
        assertTrue(!(judge<0));


        CfsLibrary.StatInfo stat2 = new CfsLibrary.StatInfo();
        ret = mnt.GetAttr("/setAttr_test", stat2);
        //System.out.println("stat2.mtime:"+stat2.mtime);

        assertTrue(stat.mtime==stat2.mtime);

    }

    @AfterClass
    //close the Client
    public static void closeClient()
    {
        mnt.CloseClient();
    }
}



