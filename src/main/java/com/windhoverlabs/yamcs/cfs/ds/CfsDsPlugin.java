package com.windhoverlabs.yamcs.cfs.ds;

import com.google.common.io.BaseEncoding;
import com.windhoverlabs.yamcs.tctm.CcsdsPacketInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.yamcs.ConfigurationException;
import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.tctm.AbstractTmDataLink;
import org.yamcs.tctm.Link.Status;
import org.yamcs.tctm.PacketInputStream;
import org.yamcs.tctm.PacketTooLongException;
import org.yamcs.time.TimeService;
import org.yamcs.utils.YObjectLoader;
import org.yamcs.yarch.rocksdb.protobuf.Tablespace.ObjectProperties;

public class CfsDsPlugin extends AbstractTmDataLink implements Runnable {
  List<BucketContents> buckets;
  TimeService timeService;
  ScheduledFuture<?> collectionFuture;
  static long frequencyMillisec = 5000;
  static Map<String, CfsDsPlugin> instances = new HashMap<>();
  String packetInputStreamClassName;
  YConfiguration packetInputStreamArgs;
  PacketInputStream packetInputStream;

  int DS_FILE_HDR_SUBTYPE;
  int DS_TOTAL_FNAME_BUFSIZE;

  static final byte[] CFE_FS_FILE_CONTENT_ID_BYTE =
      BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase());

  @Override
  public void init(String yamcsInstance, String serviceName, YConfiguration config) {
    super.init(yamcsInstance, serviceName, config);

    buckets = new LinkedList<BucketContents>();

    List<String> bucketNames = config.getList("buckets");
    DS_FILE_HDR_SUBTYPE = config.getInt("DS_FILE_HDR_SUBTYPE", 12345);
    DS_TOTAL_FNAME_BUFSIZE = config.getInt("DS_TOTAL_FNAME_BUFSIZE", 64);

    for (String bucketName : bucketNames) {
      try {
        buckets.add(new BucketContents(bucketName));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    if (config.containsKey("packetInputStreamClassName")) {
      this.packetInputStreamClassName = config.getString("packetInputStreamClassName");
      this.packetInputStreamArgs = config.getConfig("packetInputStreamArgs");
    } else {
      this.packetInputStreamClassName = CcsdsPacketInputStream.class.getName();
      this.packetInputStreamArgs = YConfiguration.emptyConfig();
    }

    try {
      packetInputStream = YObjectLoader.loadObject(packetInputStreamClassName);
    } catch (ConfigurationException e) {
      log.error("Cannot instantiate the packetInput stream", e);
      throw e;
    }

    //    	System.out.println("***********************************");
    //    	System.out.println("***********************************");
    //    	System.out.println("1 *********************************");
    //        for(BucketContents bucket : buckets) {
    //        	System.out.println(bucket.getName());
    //
    //			List<ObjectProperties> bucketObjects = bucket.getNewObjects();
    //	        for(ObjectProperties bucketObject : bucketObjects) {
    //	        	System.out.println("  " + bucketObject.getName());
    //	        }
    //        }
    //    	System.out.println("2 *********************************");
    //        for(BucketContents bucket : buckets) {
    //        	System.out.println(bucket.getName());
    //
    //			List<ObjectProperties> bucketObjects = bucket.getNewObjects();
    //	        for(ObjectProperties bucketObject : bucketObjects) {
    //	        	System.out.println("  " + bucketObject.getName());
    //	        }
    //        }
    //    	System.out.println("***********************************");
    //    	System.out.println("***********************************");
    //    	System.out.println("***********************************");
  }

  @Override
  protected void doStart() {
    YamcsServer server = YamcsServer.getServer();
    timeService = server.getInstance(yamcsInstance).getTimeService();
    ScheduledThreadPoolExecutor timer = server.getThreadPoolExecutor();
    collectionFuture =
        timer.scheduleAtFixedRate(this, 1000L, frequencyMillisec, TimeUnit.MILLISECONDS);
    notifyStarted();
  }

  @Override
  protected void doStop() {
    collectionFuture.cancel(true);
    synchronized (instances) {
      instances.remove(yamcsInstance);
    }
    try {
      collectionFuture.get();
      notifyStopped();
    } catch (CancellationException e) {
      notifyStopped();
    } catch (Exception e) {
      notifyFailed(e);
    }
  }

  @Override
  public void run() {
    for (BucketContents bucket : buckets) {
      List<ObjectProperties> bucketObjects = bucket.getNewObjects();
      for (ObjectProperties bucketObject : bucketObjects) {
        ParseFile(bucket.getRootPath() + "/" + bucketObject.getName());
      }
    }
  }

  private void ParseFile(String fileName) {
    Path fullPath = Paths.get(fileName).toAbsolutePath();
    log.debug("Interrogating file " + fullPath);

    //    	System.out.println(fullPath.toString());
    //        File processCheck = new File(fullPath.toString());
    //
    //        if(processCheck.canWrite()) {
    //        	System.out.println("CAN WRITE to " + fileName);
    //        } else {
    //        	System.out.println("CANNOT WRITE to " + fileName);
    //        }
    //
    //        if(processCheck.canRead()) {
    //        	System.out.println("CAN READ to " + fileName);
    //        } else {
    //        	System.out.println("CANNOT READ to " + fileName);
    //        }

    try {
      InputStream inputStream = new FileInputStream(fullPath.toString());
      DataInputStream dataInputStream = new DataInputStream(inputStream);

      byte[] nextWord = new byte[4];
      dataInputStream.read(nextWord, 0, 4);
      /* Check for the "cFE1" tattoo */
      if (java.util.Arrays.equals(nextWord, CFE_FS_FILE_CONTENT_ID_BYTE)) {
        /* It does have the "cFE1" tattoo.  Check to see if this is a DS log */
        int subType;
        int length;
        int spacecraftID;
        int processorID;
        int applicationID;
        int timeSeconds;
        int timeSubSeconds;
        byte[] descriptionBytes = new byte[32];
        String description;

        dataInputStream.read(nextWord, 0, 4);
        subType = (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

        dataInputStream.read(nextWord, 0, 4);
        length = (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

        dataInputStream.read(nextWord, 0, 4);
        spacecraftID =
            (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

        dataInputStream.read(nextWord, 0, 4);
        processorID =
            (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

        dataInputStream.read(nextWord, 0, 4);
        applicationID =
            (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

        dataInputStream.read(nextWord, 0, 4);
        timeSeconds =
            (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

        dataInputStream.read(nextWord, 0, 4);
        timeSubSeconds =
            (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

        dataInputStream.read(descriptionBytes, 0, 32);
        description = new String(descriptionBytes, StandardCharsets.UTF_8);

        if (subType == DS_FILE_HDR_SUBTYPE) {
          int closeSeconds;
          int closeSubsecs;
          int fileTableIndex;
          int fileNameType;
          byte[] logFileNameBytes = new byte[DS_TOTAL_FNAME_BUFSIZE];
          String logFileName;

          dataInputStream.read(nextWord, 0, 4);
          closeSeconds =
              (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

          dataInputStream.read(nextWord, 0, 4);
          closeSubsecs =
              (nextWord[0] << 24) + (nextWord[1] << 16) + (nextWord[2] << 8) + (nextWord[3]);

          dataInputStream.read(nextWord, 0, 2);
          fileTableIndex = (nextWord[0] << 8) + (nextWord[1]);

          dataInputStream.read(nextWord, 0, 2);
          fileNameType = (nextWord[0] << 8) + (nextWord[1]);

          dataInputStream.read(logFileNameBytes, 0, DS_TOTAL_FNAME_BUFSIZE);
          description = new String(logFileNameBytes, StandardCharsets.UTF_8);

          log.info(
              "Parsing file "
                  + fileName
                  + "."
                  + "  Length="
                  + length
                  + "  SCID="
                  + spacecraftID
                  + "  ProcID="
                  + processorID
                  + "  AppID="
                  + applicationID
                  + "  Time="
                  + timeSeconds
                  + ":"
                  + timeSubSeconds
                  + "  Description='"
                  + description
                  + "'"
                  + "  Closed="
                  + closeSeconds
                  + ":"
                  + closeSubsecs
                  + "  FileIndex="
                  + fileTableIndex
                  + "  FileNameType="
                  + fileNameType);

          packetInputStream.init(dataInputStream, packetInputStreamArgs);

          TmPacket pwt = null;
          System.out.println("1 *******************************");
          while (isRunningAndEnabled()) {
            System.out.println("2 *******************************");
            TmPacket tmpkt = getNextPacket();
            if (tmpkt == null) {
              System.out.println("4 *******************************");
              break;
            }
            System.out.println("3 *******************************");
            System.out.println(tmpkt.getPacket().length);
            System.out.println(tmpkt.getPacket()[0]);
            System.out.println(tmpkt.getPacket()[1]);
            System.out.println(tmpkt.getPacket()[2]);
            System.out.println(tmpkt.getPacket()[3]);
            System.out.println(tmpkt.getPacket()[4]);
            System.out.println(tmpkt.getPacket()[5]);
            System.out.println("5 *******************************");
            // processPacket(tmpkt);
            System.out.println("6 *******************************");
          }
          System.out.println("7 *******************************");
        }
      }

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public TmPacket getNextPacket() {
    TmPacket pwt = null;
    while (isRunningAndEnabled()) {
      byte[] packet;
      try {
        packet = packetInputStream.readPacket();
        updateStats(packet.length);
        TmPacket pkt = new TmPacket(timeService.getMissionTime(), packet);
        pkt.setEarthRceptionTime(timeService.getHresMissionTime());
        pwt = packetPreprocessor.process(pkt);
        if (pwt != null) {
          break;
        }
      } catch (PacketTooLongException | IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    return pwt;
  }

  //	public TmPacket getNextPacket() {
  //	    TmPacket pwt = null;
  //	    while (isRunningAndEnabled()) {
  //	        try {
  //	            openDevice();
  //	            byte[] packet = packetInputStream.readPacket();
  //	            updateStats(packet.length);
  //	            TmPacket pkt = new TmPacket(timeService.getMissionTime(), packet);
  //	            pkt.setEarthRceptionTime(timeService.getHresMissionTime());
  //	            pwt = packetPreprocessor.process(pkt);
  //	            if (pwt != null) {
  //	                break;
  //	            }
  //	        } catch (IOException e) {
  //	            if (isRunningAndEnabled()) {
  //	                log.info(
  //	                      "Cannot open or read serial device {}::{}:{}'. Retrying in 10s",
  //	                      deviceName,
  //	                      e.getMessage(),
  //	                      e.toString());
  //	                }
  //	                try {
  //	                    serialPort.close();
  //	                } catch (Exception e2) {
  //	            }
  //	            serialPort = null;
  //	            for (int i = 0; i < 10; i++) {
  //	                if (!isRunningAndEnabled()) {
  //	                    break;
  //	                }
  //	                try {
  //	                    Thread.sleep(10);
  //	                } catch (InterruptedException e1) {
  //	                    Thread.currentThread().interrupt();
  //	                    return null;
  //	                }
  //	            }
  //	        } catch (PacketTooLongException e) {
  //	            log.warn(e.toString());
  //	            try {
  //	                serialPort.close();
  //	            } catch (Exception e2) {
  //	            }
  //	            serialPort = null;
  //	        }
  //	    }
  //	    return pwt;
  //    }

  public static String byteArrayToHex(byte[] a) {
    StringBuilder sb = new StringBuilder(a.length * 2);
    for (byte b : a) sb.append(String.format("%02x", b));
    return sb.toString();
  }

  @Override
  protected Status connectionStatus() {
    return Status.OK;
  }
}
