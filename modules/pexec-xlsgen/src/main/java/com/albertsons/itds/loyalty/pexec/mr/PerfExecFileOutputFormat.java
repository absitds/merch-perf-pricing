package com.albertsons.itds.loyalty.pexec.mr;

import com.albertsons.itds.loyalty.pexec.core.PerfExecConstants;
import com.albertsons.itds.loyalty.pexec.core.PerfExecDivisionDetails.Division;
import com.albertsons.itds.loyalty.pexec.core.PerfExecHelper;
import com.albertsons.itds.loyalty.pexec.core.PerfExecSchema;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFFormulaEvaluator;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.TempFile;
import org.apache.poi.util.TempFileCreationStrategy;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfExecFileOutputFormat
    extends FileOutputFormat<PerfExecKey, PerfExecValue> {

  public static class PerfExecTempFileCreationStrategy
      implements TempFileCreationStrategy {

    private static AtomicInteger counter = new AtomicInteger();

    private TaskAttemptContext context;
    private Path workPath;

    public PerfExecTempFileCreationStrategy(TaskAttemptContext context,
        Path workPath) {
      this.context = context;
      this.workPath = workPath;
    }

    @Override
    public File createTempFile(String prefix, String suffix)
        throws IOException {
      // LocalFileSystem localFileSystem = LocalFileSystem
      // .getLocal(context.getConfiguration());
      String fileName = getUniqueFile(context,
          prefix + "-" + counter.incrementAndGet(), suffix);
      // Path tempFilePath = new Path(workPath, );
      // Path.getPathWithoutSchemeAndAuthority(tempFilePath);
      // boolean successfullyCreatedTempFile = localFileSystem
      // .createNewFile(tempFilePath);
      // if (!successfullyCreatedTempFile) {
      // throw new IOException("Could not create file " + tempFilePath);
      // }
      // return localFileSystem.pathToFile(tempFilePath);
      return new File(fileName);
    }

    @Override
    public File createTempDirectory(String prefix) throws IOException {
      LocalFileSystem localFileSystem = (LocalFileSystem) LocalFileSystem
          .get(context.getConfiguration());
      Path tempDirectoryPath = new Path(workPath,
          prefix + "-" + counter.incrementAndGet());
      boolean successfullyCreatedTempDirectory = localFileSystem
          .mkdirs(tempDirectoryPath);
      if (!successfullyCreatedTempDirectory) {
        throw new IOException(
            "Could not create directory " + tempDirectoryPath);
      }
      return localFileSystem.pathToFile(tempDirectoryPath);
    }
  }

  public static class PerfExecFileRecordWriter
      extends RecordWriter<PerfExecKey, PerfExecValue> {
    private static final String NUMBER_FORMAT = "0";
    private static final String DB_NULL_VALUE = "\\N";
    private FSDataInputStream templateInputStream;
    private SXSSFWorkbook workbook;
    private SXSSFSheet sheet;
    private TaskAttemptContext context;
    private PerfExecKey perfExeckey;
    private Path workPath;
    private int rowIndex = 2;
    private PerfExecSchema schema;
    private static Logger LOG = LoggerFactory.getLogger(PerfExecFileOutputFormat.class);


    public PerfExecFileRecordWriter(TaskAttemptContext context,
        PerfExecKey perfExeckey, Path workPath) throws IOException {
      this.context = context;
      this.perfExeckey = PerfExecKey.copy(perfExeckey);
      this.workPath = workPath;
      TempFile.setTempFileCreationStrategy(
              new PerfExecTempFileCreationStrategy(context, workPath));
      Configuration conf = context.getConfiguration();
      PerfExecHelper perfExecHelper = PerfExecHelper.getInstance();
      Path templateFilePath = perfExecHelper.getTemplateFilePath(conf);
      schema = perfExecHelper.getSchema(conf);
      templateInputStream = templateFilePath.getFileSystem(conf)
              .open(templateFilePath);
      templateWorkbook = new XSSFWorkbook(templateInputStream);
      workbook = new SXSSFWorkbook(templateWorkbook,100);
      sheet = workbook.getSheetAt(PerfExecConstants.INDEX_OF_MAIN_SHEET);
      templateSheet = templateWorkbook
              .getSheetAt(PerfExecConstants.INDEX_OF_MAIN_SHEET);
      evaluator = workbook.getCreationHelper().createFormulaEvaluator();

    }

    @Override
    public synchronized void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      FSDataOutputStream out = createWorkbookWriter(perfExeckey);
      workbook.write(out);
      out.close();
      // workbook.dispose();
      workbook.close();
      templateInputStream.close();
      FSDataOutputStream workbookStatsWriter = createWorkbookStatsWriter(
          perfExeckey);
      workbookStatsWriter.writeInt((rowIndex - 1));
      workbookStatsWriter.close();
    }

    private Map<String, SimpleDateFormat> dateFormats = new TreeMap<>();

    public SimpleDateFormat getSimpleDateFormat(
        Map<String, String> columnDetails) {
      String inputDateFormat = columnDetails.get("inputDateFormat");
      if (inputDateFormat == null) {
        inputDateFormat = "yyyy-MM-dd";
      }
      SimpleDateFormat simpleDateFormat = dateFormats.get(inputDateFormat);
      if (simpleDateFormat != null) {
        return simpleDateFormat;
      }
      simpleDateFormat = new SimpleDateFormat(inputDateFormat);
      dateFormats.put(inputDateFormat, simpleDateFormat);
      return simpleDateFormat;
    }

    @Override
    public void write(PerfExecKey key, PerfExecValue value)
        throws IOException, InterruptedException {
      SXSSFRow sheetrow = sheet.createRow(rowIndex);
      XSSFRow templateRow = templateSheet.getRow( 1);
      LOG.info("templateRow size "+templateRow.getLastCellNum());
      String[] columnValues = value.getColumnValues();
      String[] columnNames = schema.getColumnNames();
      String divisionName=getDivisionName(context.getConfiguration(),key.getDivisionId());

      int columnIndex = 0;
      for (int i = 0; i < columnNames.length; i++) {
        Map<String, String> columnDetails = schema
            .getColumnDetails(columnNames[i]);
        if (!columnDetails.containsKey("ignoreInOutput")) {
          LOG.info("Cell Index1 "+columnIndex);
          Cell cell = sheetrow.createCell(columnIndex);
          XSSFCell existingCellFormatting = templateRow.getCell(columnIndex);
          //LOG.info("Cell Index1 "+existingCellFormatting);
          if(existingCellFormatting!=null){
          CellStyle cellStyle = existingCellFormatting.getCellStyle(); // getCellStyle(cell,
          // columnDetails);
          cell.setCellStyle(cellStyle);}
          if (columnDetails.containsKey("formula")) {
            String cellFormula = columnDetails.get("formula");
            String formattedFormula = String.format(cellFormula, rowIndex + 1,divisionName.toUpperCase());
            cell.setCellType(CellType.FORMULA);
            cell.setCellFormula(formattedFormula);

          } else {
            String cellValue = columnValues[i];
            try {
              if (DB_NULL_VALUE.equals(cellValue)
                  || StringUtils.isEmpty(cellValue)) {
                cell.setCellType(CellType.BLANK);
              } else {
                String type = columnDetails.get("inputType");
                if (type.equals("Integer") || type.equals("Short")) {
                  cell.setCellType(CellType.NUMERIC);
                  cell.setCellValue(Long.parseLong(cellValue));
                } else if (type.equals("Decimal")) {
                  cell.setCellValue(Double.parseDouble(cellValue));
                } else if (type.equals("Date")) {

                  cell.setCellValue( getSimpleDateFormat(columnDetails).parse(cellValue));
                 //getSimpleDateFormat(columnDetails).parse(cellValue).getDate()
                  //cell.setCellValue(getSimpleDateFormat(columnDetails).parse(cellValue));

                } else {
                  cell.setCellType(CellType.STRING);
                  cell.setCellValue(cellValue);
                }
              }
            } catch (ParseException e) {
              throw new RuntimeException(e);
            }
          }
          columnIndex++;
        }
        LOG.info("outside ignoreInput");
      }

      int columnIndex2 = 0;
      for (int i = 0; i < columnNames.length; i++) {
        Map<String, String> columnDetails = schema
                .getColumnDetails(columnNames[i]);
        if (!columnDetails.containsKey("ignoreInOutput")) {
          LOG.info("Cell Index2 "+columnIndex2);
          Cell cell = sheetrow.getCell(columnIndex2);

          if (columnDetails.containsKey("formula")) {
            String paSheet=divisionName.toUpperCase()+" PAs";
            String kviSheet=divisionName.toUpperCase()+" KVIs";
            if(workbook.getSheet(paSheet)!=null && workbook.getSheet(kviSheet)!=null) {
              CellValue a = evaluator.evaluate(cell);
              cell.setCellValue(a.toString());

            }
          }
          columnIndex2++;
        }

      }

      rowIndex++;
    }

    private Map<String, CellStyle> cellStyles = new TreeMap<>();
    private XSSFWorkbook templateWorkbook;
    private XSSFSheet templateSheet;
    private FormulaEvaluator evaluator;

    private FSDataOutputStream createWorkbookWriter(PerfExecKey perfExeckey)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      Division division = PerfExecHelper.getInstance().getDivisionDetails(conf)
          .getDivision(perfExeckey.getDivisionId());
      String divisionName = null;
      if (division == null) {
        divisionName = "UNKNOWN-DIVISION-" + perfExeckey.getDivisionId();
      } else {
        divisionName = division.getName();
      }
      StringBuilder result = new StringBuilder();
      result.append(divisionName);
      result.append(" Pricing Tool ");
      FileSystem fs = workPath.getFileSystem(conf);
      Path toolDir = new Path(result.toString());
      fs.mkdirs(toolDir);
      Path outputExcel = new Path(workPath,
          new Path(toolDir, getUniquePerfExecFile(
              context.getTaskAttemptID().getTaskID(), conf, perfExeckey)));

      FSDataOutputStream fileOut = fs.create(outputExcel, false);
      return fileOut;
    }

    private FSDataOutputStream createWorkbookStatsWriter(
        PerfExecKey perfExeckey) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      Path outputExcelStatsFile = new Path(workPath,
          getUniquePerfExecFile(context.getTaskAttemptID().getTaskID(), conf,
              perfExeckey) + ".stats");
      FileSystem fs = outputExcelStatsFile.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(outputExcelStatsFile, false);
      return fileOut;
    }
  }

  protected class PerfExecFileRecordWriterPool
      extends RecordWriter<PerfExecKey, PerfExecValue> {

    private TaskAttemptContext context;

    private Map<String, PerfExecFileRecordWriter> divGrpIdToWriterMap = new TreeMap<>();

    public PerfExecFileRecordWriterPool(TaskAttemptContext context) {
      this.context = context;
    }

    @Override
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      Collection<PerfExecFileRecordWriter> recordWriters = divGrpIdToWriterMap
          .values();

      for (PerfExecFileRecordWriter perfExecFileRecordWriter : recordWriters) {
        perfExecFileRecordWriter.close(context);
      }
    }

    private PerfExecFileRecordWriter createNewRecordWriter(
        PerfExecKey perfExeckey) throws IOException, InterruptedException {
      Path workPath = getPathForPerfExecWorkFolder(context, perfExeckey);
      return new PerfExecFileRecordWriter(context, perfExeckey, workPath);
    }

    private PerfExecFileRecordWriter getRecordWriter(PerfExecKey perfExeckey)
        throws IOException, InterruptedException {
      String mapKey = perfExeckey.getDivisionId() + "-"
          + perfExeckey.getReportGroup();
      PerfExecFileRecordWriter recordWriter = divGrpIdToWriterMap.get(mapKey);
      if (recordWriter == null) {
        recordWriter = createNewRecordWriter(perfExeckey);
        divGrpIdToWriterMap.put(mapKey, recordWriter);
      }
      return recordWriter;
    }

    @Override
    public void write(PerfExecKey key, PerfExecValue value)
        throws IOException, InterruptedException {
      PerfExecFileRecordWriter recordWriter = getRecordWriter(key);
      recordWriter.write(key, value);
    }
  }

  public static synchronized  String getDivisionName(Configuration conf,
                               String divisionId) throws IOException{
    Division division = PerfExecHelper.getInstance().getDivisionDetails(conf)
            .getDivision(divisionId);
    String divisionName = null;
    if (division == null) {
      divisionName = "UNKNOWN-DIVISION-" + divisionId;
    } else {
      divisionName = division.getName();
    }

    return divisionName;

  }

  public static synchronized String getUniquePerfExecFile(TaskID taskId,
      Configuration conf, PerfExecKey perfExeckey) throws IOException {

    int partition = taskId.getId();
    StringBuilder result = getOutputFileName(conf, getDivisionName(conf,perfExeckey.getDivisionId()),
        perfExeckey.getReportGroup(), partition);
    return result.toString();
  }

  public static StringBuilder getOutputFileName(Configuration conf,
      String divisionName, String groupReport, int partition) {
    StringBuilder result = new StringBuilder();
    result.append(divisionName);
    result.append(" Pricing Tool ");
    result.append(conf.get(PerfExecConstants.PERF_EXEC_REPORT_DATE));
    result.append(" - Group ");
    result.append(groupReport);
    result.append(" - ");
    result.append(PerfExecConstants.NUMBER_FORMAT.format(partition));
    result.append(PerfExecConstants.XLSX_FILE_EXTENSION);
    return result;
  }

  public Path getPathForPerfExecWorkFolder(TaskAttemptContext context,
      PerfExecKey perfExeckey) throws IOException, InterruptedException {
    FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(
        context);
    return committer.getWorkPath();
  }

  @Override
  public RecordWriter<PerfExecKey, PerfExecValue> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    return new PerfExecFileRecordWriterPool(job);
  }
}
