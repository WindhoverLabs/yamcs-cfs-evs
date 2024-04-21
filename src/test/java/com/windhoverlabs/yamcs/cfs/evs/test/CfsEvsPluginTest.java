package com.windhoverlabs.yamcs.cfs.evs.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.BaseEncoding;
import com.windhoverlabs.yamcs.cfs.evs.CfsEvsPlugin;
import com.windhoverlabs.yamcs.cfs.evs.CfsEvsPlugin.CFE_FS_Header_Content;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.yamcs.YConfiguration;
import org.yamcs.client.CommandSubscription;
import org.yamcs.client.processor.ProcessorClient;
import org.yamcs.tests.AbstractIntegrationTest.TcDataLink;
import org.yamcs.utils.parser.ParseException;
import org.yamcs.yarch.ColumnDefinition;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;
import org.yamcs.yarch.streamsql.StreamSqlException;

/** Unit test for simple App. */
public class CfsEvsPluginTest extends AbstractIntegrationTest {

  private ProcessorClient processorClient;
  private CommandSubscription subscription;
  TcDataLink mtdl1;
  TcDataLink mtdl2;

  String yamcsInstance2 = "IntegrationTest";

  @Test
  public void testGetCFSLogHeader() {

    CfsEvsPlugin plugin = new CfsEvsPlugin();

    Map<String, Object> config = new HashMap<>();

    YarchDatabaseInstance ydb = YarchDatabase.getInstance(yamcsInstance);

    TupleDefinition gftdef;

    final String RECTIME_CNAME = "rectime";
    final String DATA_CNAME = "data";

    gftdef = new TupleDefinition();
    gftdef.addColumn(new ColumnDefinition(RECTIME_CNAME, DataType.TIMESTAMP));
    gftdef.addColumn(new ColumnDefinition(DATA_CNAME, DataType.BINARY));

    assertNotNull(ydb);

    try {
      ydb.execute("create stream events_realtime " + gftdef.getStringDefinition());
    } catch (StreamSqlException | ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    plugin.init("IntegrationTest", "CfsEvsPluginService", YConfiguration.wrap(config));

    assertNotNull(getClass().getResource("_ppd_aft_evs_log"));

    /* Create a DataInputStream from this FileInputStream. */
    InputStream inputStream = null;

    try {
      inputStream = new FileInputStream(getClass().getResource("_ppd_aft_evs_log").getPath());

      assertNotNull(inputStream);
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    DataInputStream dataInputStream = new DataInputStream(inputStream);

    try {
      CFE_FS_Header_Content header = plugin.getCFSLogHeader(dataInputStream);
      assertEquals(header.applicationID, 0);
      assertEquals(header.subType, 16);
      assertEquals(header.length, 64);
      assertEquals(header.spacecraftID, 1);
      assertEquals(header.processorID, 1);
      assertTrue(
          java.util.Arrays.equals(
              header.ContentType,
              BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase())));
      assertEquals("cFE EVS Log File", header.descriptionBytes);

      assertEquals(header.subType, 16);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //    assertTrue(true);
  }
}
