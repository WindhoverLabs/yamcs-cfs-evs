/****************************************************************************
 *
 *   Copyright (c) 2022 Windhover Labs, L.L.C. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 * 3. Neither the name Windhover Labs nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 *****************************************************************************/

package com.windhoverlabs.yamcs.cfs.evs;

import com.google.common.io.BaseEncoding;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.Spec.OptionType;
import org.yamcs.TmPacket;
import org.yamcs.ValidationException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SystemParametersProducer;
import org.yamcs.parameter.SystemParametersService;
import org.yamcs.protobuf.Yamcs;
import org.yamcs.tctm.AbstractTmDataLink;
import org.yamcs.tctm.CcsdsPacketInputStream;
import org.yamcs.tctm.Link.Status;
import org.yamcs.tctm.PacketInputStream;
import org.yamcs.tctm.PacketTooLongException;
import org.yamcs.utils.FileUtils;
import org.yamcs.utils.YObjectLoader;
import org.yamcs.xtce.Parameter;
import org.yamcs.yarch.FileSystemBucket;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.protobuf.Db.Event;
import org.yamcs.yarch.rocksdb.protobuf.Tablespace.ObjectProperties;

public class CfsEvsPlugin extends AbstractTmDataLink
    implements Runnable, StreamSubscriber, SystemParametersProducer {
  /* Configuration Defaults */
  static long POLLING_PERIOD_DEFAULT = 1000;
  static int INITIAL_DELAY_DEFAULT = -1;
  static boolean IGNORE_INITIAL_DEFAULT = true;
  static boolean CLEAR_BUCKETS_AT_STARTUP_DEFAULT = false;
  static boolean DELETE_FILE_AFTER_PROCESSING_DEFAULT = false;

  private boolean outOfSync = false;

  private Parameter outOfSyncParam;
  private Parameter streamEventCountParam;
  private Parameter logEventCountParam;
  private int streamEventCount;
  private int logEventCount;

  /* Configuration Parameters */
  protected long initialDelay;
  protected long period;
  protected boolean ignoreInitial;
  protected boolean clearBucketsAtStartup;
  protected boolean deleteFileAfterProcessing;
  protected int EVS_FILE_HDR_SUBTYPE;
  protected int DS_TOTAL_FNAME_BUFSIZE;

  /* Internal member attributes. */
  protected List<FileSystemBucket> buckets;
  protected YConfiguration packetInputStreamArgs;
  protected PacketInputStream packetInputStream;
  protected WatchService watcher;
  protected List<WatchKey> watchKeys;
  protected Thread thread;

  private String eventStreamName;

  static final String RECTIME_CNAME = "rectime";
  static final String DATA_EVENT_CNAME = "data";

  /* Constants */
  static final byte[] CFE_FS_FILE_CONTENT_ID_BYTE =
      BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase());

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  @Override
  public Spec getSpec() {
    Spec spec = new Spec();
    Spec preprocessorSpec = new Spec();
    Spec packetInputStreamSpec = new Spec();

    /* Define our configuration parameters. */
    spec.addOption("name", OptionType.STRING).withRequired(true);
    spec.addOption("class", OptionType.STRING).withRequired(true);
    spec.addOption("eventStream", OptionType.STRING).withRequired(true);
    spec.addOption("EVS_FILE_HDR_SUBTYPE", OptionType.INTEGER).withRequired(true);
    spec.addOption("stream", OptionType.STRING).withRequired(true);
    //    spec.addOption("initialDelay", OptionType.INTEGER)
    //        .withDefault(INITIAL_DELAY_DEFAULT)
    //        .withRequired(false);
    //    spec.addOption("pollingPeriod", OptionType.INTEGER)
    //        .withDefault(POLLING_PERIOD_DEFAULT)
    //        .withRequired(false);
    //    spec.addOption("ignoreInitial", OptionType.BOOLEAN)
    //        .withDefault(IGNORE_INITIAL_DEFAULT)
    //        .withRequired(false);
    //    spec.addOption("deleteFileAfterProcessing", OptionType.BOOLEAN)
    //        .withDefault(DELETE_FILE_AFTER_PROCESSING_DEFAULT)
    //        .withRequired(false);
    //    spec.addOption("clearBucketsAtStartup", OptionType.BOOLEAN)
    //        .withDefault(CLEAR_BUCKETS_AT_STARTUP_DEFAULT)
    //        .withRequired(false);
    spec.addOption("buckets", OptionType.LIST_OR_ELEMENT).withElementType(OptionType.STRING);
    //        .withRequired(true);
    spec.addOption("packetInputStreamClassName", OptionType.STRING).withRequired(false);
    spec.addOption("packetPreprocessorClassName", OptionType.STRING).withRequired(true);
    /* Set the preprocessor argument config parameters to "allowUnknownKeys".  We don't know
    or care what
        * these parameters are.  Let the preprocessor define them. */
    preprocessorSpec.allowUnknownKeys(true);
    spec.addOption("packetPreprocessorArgs", OptionType.MAP)
        .withRequired(true)
        .withSpec(preprocessorSpec);
    //
    //    /* Set the packet input stream argument config parameters to "allowUnknownKeys".  We don't
    // know or care what
    //     * these parameters are.  Let the packet input stream plugin define them. */
    //    packetInputStreamSpec.allowUnknownKeys(true);
    //    spec.addOption("packetInputStreamArgs", OptionType.MAP)
    //        .withRequired(false)
    //        .withSpec(packetInputStreamSpec);

    return spec;
  }

  @Override
  public void init(String yamcsInstance, String serviceName, YConfiguration config) {
    super.init(yamcsInstance, serviceName, config);

    /* Local variables */
    String packetInputStreamClassName;
    List<String> bucketNames;
    this.config = config;
    /* Calidate the configuration that the user passed us. */
    try {
      config = getSpec().validate(config);
    } catch (ValidationException e) {
      log.error("Failed configuration validation.", e);
    }

    /* Instantiate our member objects. */
    this.buckets = new LinkedList<FileSystemBucket>();
    this.watchKeys = new LinkedList<WatchKey>();
    this.eventStreamName = this.config.getString("eventStream");

    Stream stream = YarchDatabase.getInstance(yamcsInstance).getStream(eventStreamName);

    stream.addSubscriber(this);
    streamEventCount = 0;

    scheduler.scheduleAtFixedRate(
        () -> {
          this.outOfSync = this.logEventCount != this.streamEventCount;
        },
        30,
        30,
        TimeUnit.SECONDS);

    /* Read in our configuration parameters. */
    bucketNames = config.getList("buckets");
    this.EVS_FILE_HDR_SUBTYPE = config.getInt("EVS_FILE_HDR_SUBTYPE");
    this.initialDelay = config.getLong("initialDelay", INITIAL_DELAY_DEFAULT);
    this.period = config.getLong("pollingPeriod", POLLING_PERIOD_DEFAULT);
    this.ignoreInitial = config.getBoolean("ignoreInitial", IGNORE_INITIAL_DEFAULT);
    this.clearBucketsAtStartup =
        config.getBoolean("clearBucketsAtStartup", CLEAR_BUCKETS_AT_STARTUP_DEFAULT);
    this.deleteFileAfterProcessing =
        config.getBoolean("deleteFileAfterProcessing", DELETE_FILE_AFTER_PROCESSING_DEFAULT);

    /* Create the WatchService from the file system.  We're going to use this later to monitor
     * the files and directories in YAMCS Buckets. */
    try {
      watcher = FileSystems.getDefault().newWatchService();
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    /* Iterate through the bucket names passed to us by the configuration file.  We're going
    to add the buckets
        * to our internal list so we can process them later. */
    for (String bucketName : bucketNames) {
      YarchDatabaseInstance yarch = YarchDatabase.getInstance(YamcsServer.GLOBAL_INSTANCE);

      try {
        FileSystemBucket bucket;
        bucket = (FileSystemBucket) yarch.getBucket(bucketName);
        buckets.add(bucket);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    /* Iterate through the bucket and create a WatchKey on the path.  This will be used in the
    main
        * thread to get notification of any new or modified files. */
    for (FileSystemBucket bucket : buckets) {
      Path fullPath = Paths.get(bucket.getBucketRoot().toString()).toAbsolutePath();
      try {
        WatchKey key =
            fullPath.register(
                watcher,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY);

        this.watchKeys.add(key);
      } catch (IOException e1) {
        e1.printStackTrace();
        break;
      }
    }

    /* Now get the packet input stream processor class name.  This is optional, so
     * if its not provided, use the CcsdsPacketInputStream as default. */
    if (config.containsKey("packetInputStreamClassName")) {
      packetInputStreamClassName = config.getString("packetInputStreamClassName");
      if (config.containsKey("packetInputStreamArgs")) {
        packetInputStreamArgs = config.getConfig("packetInputStreamArgs");
      } else {
        packetInputStreamArgs = YConfiguration.emptyConfig();
      }
    } else {
      packetInputStreamClassName = CcsdsPacketInputStream.class.getName();
      packetInputStreamArgs = YConfiguration.emptyConfig();
    }

    /* Now create the packet input stream process */
    try {
      packetInputStream = YObjectLoader.loadObject(packetInputStreamClassName);
    } catch (ConfigurationException e) {
      log.error("Cannot instantiate the packetInput stream", e);
      throw e;
    }
  }

  @Override
  public void doDisable() {
    /* If the thread is created, interrupt it. */
    if (thread != null) {
      thread.interrupt();
    }
  }

  @Override
  public void doEnable() {
    /* Create and start the new thread. */
    thread = new Thread(this);
    thread.setName(this.getClass().getSimpleName() + "-" + linkName);
    thread.start();
  }

  @Override
  public String getDetailedStatus() {
    if (isDisabled()) {
      return String.format("DISABLED");
    } else {
      return String.format("OK, received %d packets", packetCount.get());
    }
  }

  @Override
  protected Status connectionStatus() {
    return Status.OK;
  }

  @Override
  protected void doStart() {
    if (!isDisabled()) {
      doEnable();
    }
    notifyStarted();
  }

  @Override
  protected void doStop() {
    if (thread != null) {
      thread.interrupt();
    }

    notifyStopped();
  }

  @Override
  public void run() {
    /* Delay the start, if configured to do so. */
    if (initialDelay > 0) {
      try {
        Thread.sleep(initialDelay);
        initialDelay = -1;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    /* Are we supposed to ignore the initial files in the buckets? */
    if (!ignoreInitial) {
      /* No.  Process them all.  Lets start by iterating through the buckets. */
      for (FileSystemBucket bucket : buckets) {
        try {
          /* Get the contents of the bucket. */
          List<ObjectProperties> fileOjects = bucket.listObjects();

          /* Iterate through the objects, which should be files and directories. */
          for (ObjectProperties fileObject : fileOjects) {
            /* Get the full absolute path to the file/directory. */
            Path fullPath =
                Paths.get(bucket.getBucketRoot().toString(), fileObject.getName()).toAbsolutePath();

            /* Is this a file? */
            if (Files.isRegularFile(fullPath)) {
              /* It is.  Parse the file. */
              ParseFile(fullPath);
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    /* Are we supposed to clear all the buckets at startup? */
    if (clearBucketsAtStartup) {
      /* Yes we are.  Iterate through all the buckets. */
      for (FileSystemBucket bucket : buckets) {
        try {
          /* Recursively delete the contents of the bucket, which is a directory. */
          log.info("Clearing '" + bucket.getBucketRoot() + "'");
          FileUtils.deleteContents(bucket.getBucketRoot());
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
    }

    /* Enter our main loop */
    while (isRunningAndEnabled()) {
      /* Iterate through all our watch keys. */
      for (WatchKey watchKey : this.watchKeys) {
        Path dir = (Path) watchKey.watchable();

        /* Iterate through the events queued in this watch key, if any. */
        for (WatchEvent<?> evnt : watchKey.pollEvents()) {
          WatchEvent.Kind<?> kind = evnt.kind();

          /* This key is registered only for ENTRY_CREATE events,
          but an OVERFLOW event can occur regardless if events
          are lost or discarded. */
          if (kind == StandardWatchEventKinds.OVERFLOW) {
            log.error("WatchEvent OVERFLOW detected.");
            watchKey.reset();
            continue;
          }

          if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
            /* The filename is the context of the event. */
            WatchEvent<Path> ev = (WatchEvent<Path>) evnt;
            Path fullPath = dir.resolve(ev.context());

            /* Check if the file exists first.  These events sometimes pop up when a file is deleted, so we don't
             * want to do anything if the file was actually deleted. */
            if (java.nio.file.Files.exists(fullPath)) {
              /* It exists.  Is this a file or directory? */
              if (Files.isRegularFile(fullPath)) {
                /* It is a file.  Parse it. */
                ParseFile(fullPath);
              }
            }

            /* Reset the key -- this step is critical if you want to
            receive further watch events.  If the key is no longer valid,
            the directory is inaccessible so exit the loop. */
            watchKey.reset();
          }
        }
      }

      /* Sleep for the configured amount of time.  We normally sleep so we don't needlessly chew up resources. */
      try {
        Thread.sleep(this.period);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void ParseFile(Path inputFile) {
    log.debug("Interrogating file " + inputFile);

    try {
      /* Create a DataInputStream from this FileInputStream. */
      InputStream inputStream = new FileInputStream(inputFile.toString());
      DataInputStream dataInputStream = new DataInputStream(inputStream);

      /* Check for the "cFE1" tattoo */
      byte[] nextWord = new byte[4];
      dataInputStream.read(nextWord, 0, 4);
      if (java.util.Arrays.equals(nextWord, CFE_FS_FILE_CONTENT_ID_BYTE)) {
        /* It does have the "cFE1" tattoo.  Now read the CFE FS header. */
        int subType;
        int length;
        int spacecraftID;
        int processorID;
        int applicationID;
        int timeSeconds;
        int timeSubSeconds;
        byte[] descriptionBytes = new byte[32];
        String description;

        log.debug("CFE log file detected");

        /* Read each field 1 byte at a time so we can ensure we process the fields with the correct
         * endianness and ABI.
         */
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

        /* Is this a DS log? */
        if (subType == EVS_FILE_HDR_SUBTYPE) {
          /* It is a EVS log.  Start reading the secondary header for the EVS log. */

          log.info(
              "Parsing EVS log "
                  + inputFile.toString()
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
                  + applicationID);

          /* Initialze the packet input stream with the data input stream.  We reinitialize it
           * with every file to ensure the byte stream is at the correct location, immediately
           * after the secondary header. */
          packetInputStream.init(dataInputStream, packetInputStreamArgs);

          /* Loop on the messages.  Look at the 'isRunningAndEnabled' since this
           * might take some time and we might need to be interrupted. */
          while (isRunningAndEnabled()) {
            TmPacket tmpkt = getNextPacket();
            if (tmpkt == null) {
              break;
            }
            updateStats(tmpkt.getPacket().length);
            logEventCount++;

            processPacket(tmpkt);
          }

          /* Are we supposed to delete the file? */
          if (this.deleteFileAfterProcessing) {
            /* Yes.  Delete it with extreme prejudice. */
            log.info("Deleting '" + inputFile + "'");
            java.nio.file.Files.delete(inputFile);
          }
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public TmPacket getNextPacket() {
    TmPacket pwt = null;
    while (isRunningAndEnabled()) {
      try {
        /* Get a packet from the packet input stream plugin. */
        byte[] packet = packetInputStream.readPacket();
        if (packet == null) {
          /* Something went wrong.  Return null. */
          break;
        }

        TmPacket pkt = new TmPacket(timeService.getMissionTime(), packet);
        pkt.setEarthRceptionTime(timeService.getHresMissionTime());

        /* Give the preprocessor a chance to process the packet. */
        pwt = packetPreprocessor.process(pkt);
        if (pwt != null) {
          /* We successfully processed the packet.  Break out so we can return it. */
          break;
        }
      } catch (EOFException e) {
        /* We read the EOF.  This is not an error condition, so don't throw the exception. Just
         * return a null to let the caller know we're at the end. */
        pwt = null;
        break;
      } catch (PacketTooLongException | IOException e) {
        /* Something went wrong.  Return null. */
        pwt = null;
        e.printStackTrace();
      }
    }

    return pwt;
  }

  @Override
  public void onTuple(Stream stream, Tuple tuple) {
    if (isRunningAndEnabled()) {
      Event event = (Event) tuple.getColumn("body");
      updateStats(event.getMessage().length());
      streamEventCount++;
    }
  }

  @Override
  public void setupSystemParameters(SystemParametersService sysParamCollector) {
    super.setupSystemParameters(sysParamCollector);
    outOfSyncParam =
        sysParamCollector.createSystemParameter(
            linkName + "/outOfSync",
            Yamcs.Value.Type.BOOLEAN,
            "Are the downlinked events not in sync wtih the ones from the log?");
    streamEventCountParam =
        sysParamCollector.createSystemParameter(
            linkName + "/streamEventCountParam",
            Yamcs.Value.Type.UINT64,
            "Event count in realtime event stream");
    logEventCountParam =
        sysParamCollector.createSystemParameter(
            linkName + "/logEventCountParam",
            Yamcs.Value.Type.UINT64,
            "Event count from log files");
  }

  @Override
  public List<ParameterValue> getSystemParameters() {
    long time = getCurrentTime();

    ArrayList<ParameterValue> list = new ArrayList<>();
    try {
      collectSystemParameters(time, list);
    } catch (Exception e) {
      log.error("Exception caught when collecting link system parameters", e);
    }
    return list;
  }

  @Override
  protected void collectSystemParameters(long time, List<ParameterValue> list) {
    super.collectSystemParameters(time, list);
    list.add(SystemParametersService.getPV(outOfSyncParam, time, outOfSync));
    list.add(SystemParametersService.getPV(streamEventCountParam, time, streamEventCount));
    list.add(SystemParametersService.getPV(logEventCountParam, time, logEventCount));
  }
}
