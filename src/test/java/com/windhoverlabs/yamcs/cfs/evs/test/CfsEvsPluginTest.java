package com.windhoverlabs.yamcs.cfs.evs.test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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
import org.yamcs.tests.AbstractIntegrationTest;

/** Unit test for simple App. */
public class CfsEvsPluginTest extends AbstractIntegrationTest {

  @Test
  public void testGetCFSLogHeader() {
    CfsEvsPlugin plugin = new CfsEvsPlugin();

    Map<String, Object> config = new HashMap<>();

    assertNotNull(getClass().getResource("cfe_evs.log"));

    System.out.println("**********" + getClass().getResource("cfe_evs.log").toString());

    /* Create a DataInputStream from this FileInputStream. */
    InputStream inputStream = null;
    try {
      inputStream = new FileInputStream(getClass().getResource("cfe_evs.log").toString());
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    DataInputStream dataInputStream = new DataInputStream(inputStream);

    try {
      CFE_FS_Header_Content header = plugin.getCFSLogHeader(dataInputStream);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //    assertTrue(true);
  }
}
