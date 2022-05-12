package com.windhoverlabs.yamcs.cfs.ds;

import com.google.common.io.BaseEncoding;
import com.windhoverlabs.yamcs.tctm.CcsdsPacketInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
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
import org.yamcs.yarch.Bucket;
import org.yamcs.yarch.FileSystemBucket;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.rocksdb.protobuf.Tablespace.ObjectProperties;

public class CfsDsPlugin extends AbstractTmDataLink implements Runnable {
  List<BucketContents> buckets;
  ScheduledFuture<?> collectionFuture;
  static long frequencyMillisec = 5000;
  static Map<String, CfsDsPlugin> instances = new HashMap<>();
  String packetInputStreamClassName;
  YConfiguration packetInputStreamArgs;
  PacketInputStream packetInputStream;
  protected long initialDelay;
  protected long period;
  protected WatchService watcher;
  protected List<WatchKey> watchKeys;
  //protected WatchKey watchKey;

  int DS_FILE_HDR_SUBTYPE;
  int DS_TOTAL_FNAME_BUFSIZE;

  static final byte[] CFE_FS_FILE_CONTENT_ID_BYTE =
      BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase());

  @Override
  public void init(String yamcsInstance, String serviceName, YConfiguration config) {
    super.init(yamcsInstance, serviceName, config);

    buckets = new LinkedList<BucketContents>();
    watchKeys = new LinkedList<WatchKey>();

    List<String> bucketNames = config.getList("buckets");
    DS_FILE_HDR_SUBTYPE = config.getInt("DS_FILE_HDR_SUBTYPE", 12345);
    DS_TOTAL_FNAME_BUFSIZE = config.getInt("DS_TOTAL_FNAME_BUFSIZE", 64);
    this.initialDelay = config.getLong("initialDelay", -1);
    this.period = config.getLong("period", 5000);
    
    try {
		watcher = FileSystems.getDefault().newWatchService();
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}

    for (String bucketName : bucketNames) {
        YarchDatabaseInstance yarch = YarchDatabase.getInstance(YamcsServer.GLOBAL_INSTANCE);

   	    try {
   	    	FileSystemBucket bucket = (FileSystemBucket) yarch.getBucket(bucketName);

   	    	System.out.println("Bucket Name = " + bucketName);
   	    	System.out.println("       Root = " + bucket.getBucketRoot().toString());
   	    	
		    Path fullPath = Paths.get(bucket.getBucketRoot().toString()).toAbsolutePath();
   	    	System.out.println("       Path = " + fullPath);
            try {
				WatchKey key = fullPath.register(
						  watcher,
						  StandardWatchEventKinds.ENTRY_CREATE,
						  StandardWatchEventKinds.ENTRY_MODIFY);
				
				this.watchKeys.add(key);
				
				//watchKeys.add(key);
			  } catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				break;
			  }
		    
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	
    	
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
    if (!isDisabled()) {
      new Thread(this).start();
    }
    notifyStarted();
  }

  @Override
  protected void doStop() {
    //    if (serialPort != null) {
    //      try {
    //        serialPort.close();
    //      } catch (IOException e) {
    //        log.warn("Exception got when closing the serial port:", e);
    //      }
    //      serialPort = null;
    //    }
    notifyStopped();
  }

  //  @Override
  //  protected void doStart() {
  //    YamcsServer server = YamcsServer.getServer();
  //    timeService = server.getInstance(yamcsInstance).getTimeService();
  //    ScheduledThreadPoolExecutor timer = server.getThreadPoolExecutor();
  //    collectionFuture =
  //        timer.scheduleAtFixedRate(this, 1000L, frequencyMillisec, TimeUnit.MILLISECONDS);
  //    notifyStarted();
  //  }

  //  @Override
  //  protected void doStop() {
  //    collectionFuture.cancel(true);
  //    synchronized (instances) {
  //      instances.remove(yamcsInstance);
  //    }
  //    try {
  //      collectionFuture.get();
  //      notifyStopped();
  //    } catch (CancellationException e) {
  //      notifyStopped();
  //    } catch (Exception e) {
  //      notifyFailed(e);
  //    }
  //  }

  @Override
  public void run() {
    if (initialDelay > 0) {
      try {
        Thread.sleep(initialDelay);
        initialDelay = -1;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    while (isRunningAndEnabled()) {
      for(WatchKey watchKey: this.watchKeys) {
    	  
        Path dir = (Path)watchKey.watchable();
        
        for (WatchEvent<?> evnt: watchKey.pollEvents()) {
          WatchEvent.Kind<?> kind = evnt.kind();
          
          // This key is registered only
          // for ENTRY_CREATE events,
          // but an OVERFLOW event can
          // occur regardless if events
          // are lost or discarded.
          if (kind == StandardWatchEventKinds.OVERFLOW) {
        	  System.out.println("OVERFLOW");
              continue;
          }
          
          // The filename is the
          // context of the event.
          WatchEvent<Path> ev = (WatchEvent<Path>)evnt;
          Path fullPath = dir.resolve(ev.context());
          
          ParseFile(fullPath.toString());
          
          // Reset the key -- this step is critical if you want to
          // receive further watch events.  If the key is no longer valid,
          // the directory is inaccessible so exit the loop.
          watchKey.reset();
        }
      }
    
      try {
        Thread.sleep(this.period);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
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
          logFileName = new String(logFileNameBytes, StandardCharsets.UTF_8);

          log.info(
              "Parsing file "
                  + fileName
                  + " - "
                  + logFileName
                  + "."
                  + "  SubType="
                  + subType
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

          while (isRunningAndEnabled()) {
            TmPacket tmpkt = getNextPacket();
            if (tmpkt == null) {
              break;
            }

            String fullMsg = new String();

            fullMsg = tmpkt.getPacket().length + ": ";
            for (int i = 0; i < tmpkt.getPacket().length; ++i) {
              fullMsg = fullMsg + String.format("%02X", tmpkt.getPacket()[i]) + " ";
            }
            
            processPacket(tmpkt);
          }
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
      try {
        byte[] packet = packetInputStream.readPacket();
        if(packet == null) {
            break;
        }
        updateStats(packet.length);
        TmPacket pkt = new TmPacket(timeService.getMissionTime(), packet);
        pkt.setEarthRceptionTime(timeService.getHresMissionTime());
        pwt = packetPreprocessor.process(pkt);
        if (pwt != null) {
          break;
        }
      } catch (EOFException e) {
        pwt = null;
        break;
      } catch (PacketTooLongException | IOException e) {
        // TODO Auto-generated catch block
        pwt = null;
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
