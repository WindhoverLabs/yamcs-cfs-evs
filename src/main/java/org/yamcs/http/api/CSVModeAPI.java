package org.yamcs.http.api;

import com.windhoverlabs.yamcs.csv.api.AbstractCSVModeApi;
import com.windhoverlabs.yamcs.csv.api.EvsCSVModeConfig;
import com.windhoverlabs.yamcs.csv.api.SetModeRequest;
import org.yamcs.YamcsServer;
import org.yamcs.api.Observer;
import org.yamcs.events.EventProducer;
import org.yamcs.events.EventProducerFactory;
import org.yamcs.http.Context;
import org.yamcs.tctm.Link;

public class CSVModeAPI extends AbstractCSVModeApi<Context> {

  private EventProducer eventProducer =
      EventProducerFactory.getEventProducer(null, this.getClass().getSimpleName(), 10000);

  @Override
  public void setMode(Context c, SetModeRequest request, Observer<EvsCSVModeConfig> observer) {
    Link l =
        YamcsServer.getServer()
            .getInstance(request.getInstance())
            .getLinkManager()
            .getLink(request.getLinkName());

    if (l == null) {
      eventProducer.sendInfo("Link:" + request.getLinkName() + " does not exist");
      observer.complete();
      return;
    }
    ((com.windhoverlabs.yamcs.cfs.evs.CfsEvsPlugin) l).setMode(request.getMode());
    observer.complete(EvsCSVModeConfig.newBuilder().build());
  }
}
