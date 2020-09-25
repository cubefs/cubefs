package io.chubao.fs.sdk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

public class ListTest extends StorageTest {
  private final static Log log = LogFactory.getLog(ListTest.class);
  @Test
  public void testList() {
    CFSStatInfo[] stats = listStats(listTestDir);
    Assert.assertNull(stats);

    boolean res = mkdirs(listTestDir);
    Assert.assertTrue(res);

    String[] subdirs = new String[3];
    subdirs[0] = "d1";
    subdirs[1] = "d2";
    subdirs[2] = "f1";
    String path1 = listTestDir + "/" + subdirs[0];
    Assert.assertTrue(mkdirs(path1));
    String path2 = listTestDir + "/" + subdirs[1];
    Assert.assertTrue(mkdirs(path2));
    String path3 = listTestDir + "/" + subdirs[2];
    Assert.assertTrue(createFile(path3, 0));

    stats = listStats(listTestDir);
    for (int i=0; i<stats.length; i++) {
      if (stats[i].getName().equals("d1") || stats[i].getName().equals("d2")) {
        Assert.assertEquals(stats[i].getType(), CFSStatInfo.Type.DIR);
      } else if (stats[i].getName().equals("f1")) {
        Assert.assertEquals(stats[i].getType(), CFSStatInfo.Type.REG);
      } else {
        Assert.assertTrue(false);
      }
//      //Assert.assertEquals(stats[i].getName().length(), subdirs[i].length());
//      //Assert.assertEquals(stats[i].getName(), subdirs[i]);
//      Assert.assertEquals(stats[i].getUid(), DEFAULT_UID);
//      Assert.assertEquals(stats[i].getGid(), DEFAULT_GID);
//     //Assert.assertEquals((short)stats[i].getMode(), DEFAULT_MODE);
    }
  }

  @Test
  public void testList2() {
    CFSStatInfo[] stats = listStats(listTestDir);
    Assert.assertNull(stats);

    boolean res = mkdirs(listTestDir);
    Assert.assertTrue(res);

    for (int i=0; i<1000; i++) {
      String path = listTestDir + "/" + i;
      Assert.assertTrue(createFile(path, 0));
    }

    stats = listStats(listTestDir);
    Assert.assertEquals(stats.length, 1000);
    for (int i=0; i<stats.length; i++) {
      Assert.assertEquals(stats[i].getType(), CFSStatInfo.Type.REG);
    }
  }

  @Test
  public void testListInvalidPath() {
    String dir1 = "../";
    Assert.assertNull(listStats(dir1));

    String dir2 = "/../";
    Assert.assertNotNull(listStats(dir2));

    String dir3 = null;
    Assert.assertNull(listStats(dir3));

    String dir4 = " ";
    Assert.assertNull(listStats(dir4));
  }
}