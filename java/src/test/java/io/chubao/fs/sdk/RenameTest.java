package io.chubao.fs.sdk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class RenameTest extends StorageTest {
  private final static Log log = LogFactory.getLog(RenameTest.class);

  @Test
  public void testRenameDir() {
    String path1 = unlinkTestDir + "/d0";
    String path2 = unlinkTestDir + "/d1";
    Assert.assertTrue(mkdirs(path1));
    Assert.assertTrue(rename(path1, path2));
    Assert.assertNull(stat(path1));
    Assert.assertNotNull(stat(path2));

    String path3 = unlinkTestDir + "/d2" + "/dd2";
    Assert.assertTrue(mkdirs(unlinkTestDir + "/d2"));
    Assert.assertTrue(rename(path2, path3));
    Assert.assertNull(stat(path2));
    Assert.assertNotNull(stat(path3));

    Assert.assertTrue(rmdir(unlinkTestDir, true));
  }

  @Test
  public void testRenameFile() {
    String path1 = unlinkTestDir + "/d0";
    String path2 = unlinkTestDir + "/d1";
    Assert.assertTrue(mkdirs(unlinkTestDir));
    Assert.assertTrue(createFile(path1, 0));
    Assert.assertTrue(rename(path1, path2));
    Assert.assertNull(stat(path1));
    CFSStatInfo stat = stat(path2);
    Assert.assertNotNull(stat);
    Assert.assertEquals(stat.getType(), CFSStatInfo.Type.REG);

    String path3 = unlinkTestDir + "/d2" + "/dd2";
    Assert.assertTrue(mkdirs(unlinkTestDir + "/d2"));
    Assert.assertTrue(rename(path2, path3));
    Assert.assertNull(stat(path2));
    stat = stat(path3);
    Assert.assertNotNull(stat);
    Assert.assertEquals(stat.getType(), CFSStatInfo.Type.REG);

    Assert.assertTrue(rmdir(unlinkTestDir, true));
  }

  @Test
  public void testRenameExce() {
    // Source is not exist.
    String path1 = renameTestDir + "/d0";
    String path2 = renameTestDir + "/d1";
    Assert.assertNull(stat(path1));
    Assert.assertNull(stat(path2));
    Assert.assertFalse(rename(path1, path2));

    // target is exist, as dir.
    Assert.assertTrue(mkdirs(path1));
    Assert.assertTrue(mkdirs(path2));
    Assert.assertTrue(rename(path1, path2));
    Assert.assertNull(stat(path1));
    Assert.assertTrue(rmdir(renameTestDir, true));

    // soruce is file, target is dir
    Assert.assertTrue(mkdirs(renameTestDir));
    Assert.assertTrue(createFile(path1, 0));
    Assert.assertTrue(mkdirs(path2));
    Assert.assertTrue(rename(path1, path2));
    Assert.assertTrue(rmdir(renameTestDir, true));

    // soruce is dir, target is file
    Assert.assertTrue(mkdirs(path1));
    Assert.assertTrue(createFile(path2, 0));
    Assert.assertFalse(rename(path1, path2));
    Assert.assertTrue(rmdir(renameTestDir, true));
  }

  @Test
  public void testRenameSrouceIsInvalid() {
    String to = renameTestDir + "/d1";
    String dir1 = "../";
    Assert.assertFalse(rename(dir1, to));

    String dir2 = "/../";
    Assert.assertFalse(rename(dir2, to));

    String dir3 = null;
    Assert.assertFalse(rename(dir3, to));

    String dir4 = " ";
    Assert.assertFalse(rename(dir4, to));
  }

  @Test
  public void testRenameTargetIsInvalid() {
    String from = renameTestDir + "/d1";
    Assert.assertTrue(mkdirs(from));

    String dir1 = "../";
    Assert.assertFalse(rename(from, dir1));

    String dir2 = "/../";
    Assert.assertFalse(rename(from, dir2));

    String dir3 = null;
    Assert.assertFalse(rename(from, dir3));

    String dir4 = " ";
    Assert.assertFalse(rename(from, dir4));
  }


  @Test
  public void testRenameDir2() {
    String path1 = unlinkTestDir + "/d0";
    String path2 = path1 + "/dd0";
    String path3 = unlinkTestDir + "/d1/";
    Assert.assertTrue(mkdirs(path2));
    Assert.assertTrue(mkdirs(path3));
    Assert.assertTrue(rename(path1, path3));
    Assert.assertNull(stat(path1));
    Assert.assertNotNull(stat(path3));
    Assert.assertTrue(rmdir(unlinkTestDir, true));
  }


  @Test
  public void testRenameToChileDir() {
    String path1 = unlinkTestDir + "/d0";
    String path2 = path1 + "/dd0/dd1";
    Assert.assertTrue(mkdirs(path2));
    Assert.assertFalse(rename(path1, path2));
    Assert.assertNotNull(stat(path1));
    Assert.assertTrue(rmdir(unlinkTestDir, true));
  }
}