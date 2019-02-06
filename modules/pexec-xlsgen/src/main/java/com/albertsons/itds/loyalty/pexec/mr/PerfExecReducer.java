package com.albertsons.itds.loyalty.pexec.mr;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfExecReducer
    extends Reducer<PerfExecKey, PerfExecValue, PerfExecKey, PerfExecValue> {

  private static Logger LOG = LoggerFactory.getLogger(PerfExecReducer.class);

  @Override
  protected void reduce(
      PerfExecKey key,
      Iterable<PerfExecValue> values,
      Context context)
      throws IOException, InterruptedException {
    for (PerfExecValue value : values) {
      context.write(key, value);
    }
  }
}
