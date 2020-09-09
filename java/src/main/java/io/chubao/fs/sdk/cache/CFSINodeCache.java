package io.chubao.fs.sdk.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class CFSINodeCache {
  private Map<String, CFSINode> inodes;
  private int capacity;
  private ReentrantLock lock;
  public CFSINodeCache(int capacity) {
    inodes =  new LinkedHashMap<String, CFSINode>(capacity, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > capacity;
      }
    };
    lock = new ReentrantLock();
  }

  public CFSINode get(String key) {
    lock.lock();
    CFSINode inode = inodes.get(key);
    lock.unlock();
    return inode;
  }

  public void put(String key, CFSINode inode) {
    lock.lock();
    inodes.put(key, inode);
    lock.unlock();
  }

  public static void  main(String[] args) {
    CFSINodeCache cache = new CFSINodeCache(10);
    CFSINode inode = cache.get("1");
    if (inode != null) {
      System.out.println("IID:" + inode.getID());
    } else {
      System.out.println("Not found.");
    }
  }

}
