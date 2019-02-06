package com.albertsons.itds.loyalty.pexec.mr;

import com.albertsons.itds.loyalty.pexec.core.PerfExecConstants;
import com.albertsons.itds.loyalty.pexec.core.PerfExecHelper;
import com.albertsons.itds.loyalty.pexec.core.PerfExecSchema;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfExecMapper extends Mapper<LongWritable, Text, PerfExecKey, PerfExecValue> {

  private static Logger LOG = LoggerFactory.getLogger(PerfExecMapper.class);

  private PerfExecKey perfExecKey = new PerfExecKey();

  private PerfExecValue perfExecValue = new PerfExecValue();

  private PerfExecHelper helper;

  private PerfExecSchema schema;

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String record = value.toString();
    String[] columnValues = record.split(Pattern.quote(PerfExecConstants.COLUMN_DELIMITER));
    Preconditions.checkArgument(schema.getColumnNames().length == columnValues.length);
    perfExecKey.setDivisionId(columnValues[schema.getIndexOfDivisionId()]);
    perfExecValue.setColumnValues(columnValues);
    int indexOfReportGroup = schema.getIndexOfReportGroup();
    String[] reportGroups =
        columnValues[indexOfReportGroup].split(
            Pattern.quote(PerfExecConstants.REPORT_GROUP_DELIMITER));
    for (int i = 0; i < reportGroups.length; i++) {
      perfExecKey.setReportGroup(reportGroups[i]);
      context.write(perfExecKey, perfExecValue);
    }
  }

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    helper = PerfExecHelper.getInstance();
    schema = helper.getSchema(context.getConfiguration());
  }
}
