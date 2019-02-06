package com.albertsons.itds.loyalty.pexec;

import com.albertsons.itds.loyalty.pexec.core.PerfExecConstants;
import com.albertsons.itds.loyalty.pexec.core.PerfExecContext;
import com.albertsons.itds.loyalty.pexec.core.PerfExecDivisionDetails;
import com.albertsons.itds.loyalty.pexec.core.PerfExecHelper;
import com.albertsons.itds.loyalty.pexec.mr.PerfExecFileOutputFormat;
import com.albertsons.itds.loyalty.pexec.mr.PerfExecKey;
import com.albertsons.itds.loyalty.pexec.mr.PerfExecMapper;
import com.albertsons.itds.loyalty.pexec.mr.PerfExecReducer;
import com.albertsons.itds.loyalty.pexec.mr.PerfExecValue;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tool to generate performance execution tool excels */
public class PerfExecTool extends Configured implements Tool {

  private static final int JOB_FAILED_EXIT_CODE = 1;
  private static Logger LOG = LoggerFactory.getLogger(PerfExecTool.class);

  /**
   * Entry point for the tool
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int exitCode = ToolRunner.run(conf, new PerfExecTool(), args);
    System.exit(exitCode);
  }

  void assertPathDoesNotExists(Path path) throws IOException {
    if (path.getFileSystem(getConf()).exists(path)) {
      LOG.error("Input data path [ {} ] already exists.", path);
      throw new IllegalArgumentException("Path already exists " + path.toString());
    }
  }

  void assertPathExists(Path path) throws IOException {
    if (!path.getFileSystem(getConf()).exists(path)) {
      LOG.error("Input data path [ {} ] does not exists.", path);
      throw new IllegalArgumentException("Path does not exists " + path.toString());
    }
  }

  /**
   * Calculate number of reducers. It can be send from command line or by default it uses then
   * number of divisions to do the calculation.
   *
   * @param context
   * @return
   * @throws IOException
   */
  public int getNumOfReduceTasks(PerfExecContext context) throws IOException {
    if (context.getNumOfExcelWriters() != null) {
      return context.getNumOfExcelWriters();
    }
    PerfExecDivisionDetails divisionDetails =
        PerfExecHelper.getInstance().getDivisionDetails(getConf());
    int reducerCount = divisionDetails.getDivisions().size();
    Preconditions.checkArgument(reducerCount > PerfExecConstants.ZERO_DIVISIONS);
    return reducerCount;
  }

  void loadConfigurationFiles(PerfExecContext context) throws IOException {
    getConf().addResource(PerfExecConstants.PEXEC_DEFAULT_XML);
    getConf().addResource(PerfExecConstants.PEXEC_SITE_XML);
    List<Path> configFiles = context.getConfigFiles();
    for (Path path : configFiles) {
      assertPathExists(path);
      getConf().addResource(path);
    }
  }

  PerfExecContext loadContext(String[] args) throws IOException {
    PerfExecContext context = parseContext(args);
    loadConfigurationFiles(context);
    refreshConfigsFromCmdArgs(context);
    return context;
  }

  PerfExecContext parseContext(String[] args) throws IOException {
    PerfExecContext context = new PerfExecContext();
    JCommander jCommander = JCommander.newBuilder().addObject(context).build();
    try {
      jCommander.parse(args);
    } catch (ParameterException e) {
      jCommander.usage();
      throw new IllegalArgumentException(e);
    }
    if (context.isHelp()) {
      jCommander.usage();
      System.exit(0);
    }
    validateContext(context);
    return context;
  }

  /**
   * Command line arguments always take precedence over values set in configuration files.
   *
   * @param context
   */
  void refreshConfigsFromCmdArgs(PerfExecContext context) {
    if (context.getMinNumOfInputReaders() != null) {
      getConf().setInt("mapreduce.local.map.tasks.maximum", context.getMinNumOfInputReaders());
    }
    getConf().set(PerfExecConstants.PERF_EXEC_REPORT_DATE, context.getReportDate());
    getConf()
        .set(
            PerfExecConstants.PER_EXEC_DIVISIONS_FILE_PATH,
            context.getDivisionsFilePath().toString());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    // parse command line arguments into context
    PerfExecContext context = loadContext(args);
    // store additional information in the configurations share across tasks
    storeAdditionalInfo(context);
    Job job = Job.getInstance(getConf(), context.getJobName());
    job.setJarByClass(PerfExecTool.class);
    job.setMapperClass(PerfExecMapper.class);
    job.setReducerClass(PerfExecReducer.class);
    job.setNumReduceTasks(getNumOfReduceTasks(context));
    job.setOutputKeyClass(PerfExecKey.class);
    job.setOutputValueClass(PerfExecValue.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(PerfExecFileOutputFormat.class);
    FileInputFormat.addInputPath(job, context.getDataPath());
    FileOutputFormat.setOutputPath(job, context.getOutputPath());
    boolean result = job.waitForCompletion(true);
    if (result) {
      return extractAllStats(context);
    } else {
      return JOB_FAILED_EXIT_CODE;
    }
  }

  private int extractAllStats(PerfExecContext context) throws IOException, FileNotFoundException {
    Path outputPath = context.getOutputPath();
    FileSystem fileSystem = outputPath.getFileSystem(getConf());
    RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(outputPath, false);
    Map<String, Integer> fileNameToRowCount = new TreeMap<>();
    while (files.hasNext()) {
      LocatedFileStatus fileStatus = files.next();
      Path path = fileStatus.getPath();
      String pathName = path.getName();
      if (pathName.endsWith(".stats")) {
        FSDataInputStream inputStream = fileSystem.open(path);
        int rowCount = inputStream.readInt();
        fileNameToRowCount.put(pathName.substring(0, pathName.indexOf(".stats")), rowCount);
        inputStream.close();
      }
    }
    FSDataOutputStream outputStream = fileSystem.create(new Path(outputPath, "all-stats.csv"));
    long totalRows = 0;
    Set<Entry<String, Integer>> entrySet = fileNameToRowCount.entrySet();
    for (Entry<String, Integer> entry : entrySet) {
      Integer rowCount = entry.getValue();
      outputStream.writeBytes(rowCount.toString());
      outputStream.writeBytes(",");
      outputStream.writeBytes(entry.getKey());
      outputStream.writeBytes("\n");
      totalRows += rowCount;
    }
    outputStream.writeBytes("" + totalRows);
    outputStream.writeBytes(",");
    outputStream.writeBytes("Total Rows");
    outputStream.close();
    deleteStatFiles(outputPath, fileSystem);
    return 0;
  }

  private Map<String, Integer> deleteStatFiles(Path outputPath, FileSystem fileSystem)
      throws FileNotFoundException, IOException {
    RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(outputPath, false);
    Map<String, Integer> fileNameToRowCount = new TreeMap<>();
    while (files.hasNext()) {
      LocatedFileStatus fileStatus = files.next();
      Path path = fileStatus.getPath();
      String pathName = path.getName();
      if (pathName.endsWith(".stats")) {
        fileSystem.delete(path, false);
      }
    }
    return fileNameToRowCount;
  }

  void storeAdditionalInfo(PerfExecContext context) {
    getConf()
        .set(PerfExecConstants.PER_EXEC_SCHEMA_FILE_PATH, context.getSchemaFilePath().toString());
    getConf()
        .set(
            PerfExecConstants.PER_EXEC_TEMPLATE_FILE_PATH,
            context.getTemplateFilePath().toString());
  }

  void validateContext(PerfExecContext context) throws IOException {
   // context.getOutputPath().getFileSystem(getConf()).delete(context.getOutputPath(), true);
    assertPathExists(context.getDataPath());
    assertPathExists(context.getDivisionsFilePath());
    assertPathExists(context.getSchemaFilePath());
    assertPathExists(context.getTemplateFilePath());
    assertPathDoesNotExists(context.getOutputPath());
  }
}
