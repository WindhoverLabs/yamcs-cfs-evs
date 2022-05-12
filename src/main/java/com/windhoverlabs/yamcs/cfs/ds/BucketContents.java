package com.windhoverlabs.yamcs.cfs.ds;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.yamcs.YamcsServer;
import org.yamcs.yarch.FileSystemBucket;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.rocksdb.protobuf.Tablespace.ObjectProperties;

public class BucketContents {
  FileSystemBucket bucket;
  List<ObjectProperties> prevBucketObjects = new LinkedList<ObjectProperties>();

  public BucketContents(String bucketName) throws IOException {
    YarchDatabaseInstance yarch = YarchDatabase.getInstance(YamcsServer.GLOBAL_INSTANCE);

    bucket = (FileSystemBucket) yarch.getBucket(bucketName);
  }

  public List<ObjectProperties> getNewObjects() {
    // List<ObjectProperties> newObjects = new LinkedList<ObjectProperties>();
    List<ObjectProperties> newObjects = new LinkedList<ObjectProperties>();

    try {
      List<ObjectProperties> currentObjects = bucket.listObjects();

      for (ObjectProperties currentBucketObject : currentObjects) {
        boolean found = false;

        for (ObjectProperties prevBucketObject : prevBucketObjects) {
          if (currentBucketObject.getName().equals(prevBucketObject.getName())) {
            if (currentBucketObject.getCreated() == prevBucketObject.getCreated()) {
              found = true;
            }
          }
        }

        if (!found) {
          newObjects.add(currentBucketObject);
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    prevBucketObjects = newObjects;

    return newObjects;
    // return newObjects;
  }

  public String getRootPath() {
    return bucket.getBucketRoot().toString();
  }
}
