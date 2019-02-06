package com.albertsons.itds.loyalty.pexec.core;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;

/**
 * Context object to parse to be used to store tool input. It is populated by parsing command line
 * arguments.
 */
public class PerfExecContext {

  /** Convert String to Path. */
  public static class PathConverter implements IStringConverter<Path> {
    @Override
    public Path convert(String value) {
      return new Path(value);
    }
  }

  /** Arguments passed from command line */
  private String[] args;

  /**
   * Configuration file. Option can be repeated multiple times. The sequence of files is the order
   * of precedence. Every next configuration file takes precedence over previous..
   */
  @Parameter(
      names = {"--config-file"},
      description = "Configuration file")
  private List<Path> configFiles = new ArrayList<>();

  /**
   * Location of file or directory which contains the input data for generating the performance
   * execution tool output files.
   */
  @Parameter(
      required = true,
      names = {"--data-path"},
      converter = PathConverter.class,
      description = "Input data path")
  private Path dataPath;

  /** A file containing divition id to division name mapping */
  @Parameter(
      required = true,
      names = {"--divisions-file-path"},
      converter = PathConverter.class,
      description = "Path of file containing division details")
  private Path divisionsFilePath;

  /** Prints the help for all the opptions available */
  @Parameter(
      names = {"--help", "-h"},
      help = true,
      description = "Help for command line arguments for this tool")
  private boolean help = false;

  /** Name of the job to be used while executing in distributed execution. */
  @Parameter(
      required = true,
      names = {"--job-name"},
      description = "Job name")
  private String jobName;

  /**
   * If the number of input splits is more than this number then this will be honored. Else the
   * split count is used.
   */
  @Parameter(
      names = {"--min-input-readers"},
      description = "Minimum number of input readers")
  private Integer minNumOfInputReaders;

  @Parameter(
      names = {"--num-excel-writers"},
      description = "Number of excel writers")
  private Integer numOfExcelWriters;

  @Parameter(
      required = true,
      names = {"--output-path"},
      converter = PathConverter.class,
      description = "Output path for the excel")
  private Path outputPath;

  @Parameter(
      required = true,
      names = {"--report-date"},
      description = "Report date")
  private String reportDate;

  @Parameter(
      required = true,
      names = {"--schema-file-path"},
      converter = PathConverter.class,
      description = "Schema file path for the excel")
  private Path schemaFilePath;

  @Parameter(
      required = true,
      names = {"--template-file-path"},
      converter = PathConverter.class,
      description = "Path of the Excel template file to be used")
  private Path templateFilePath;

  public String[] getArgs() {
    return args;
  }

  public List<Path> getConfigFiles() {
    return configFiles;
  }

  /** @return the dataPath */
  public Path getDataPath() {
    return dataPath;
  }

  public Path getDivisionsFilePath() {
    return divisionsFilePath;
  }

  /** @return the jobName */
  public String getJobName() {
    return jobName;
  }

  /** @return the minNumOfInputReaders */
  public Integer getMinNumOfInputReaders() {
    return minNumOfInputReaders;
  }

  /** @return the numOfExcelWriters */
  public Integer getNumOfExcelWriters() {
    return numOfExcelWriters;
  }

  /** @return the outputPath */
  public Path getOutputPath() {
    return outputPath;
  }

  public String getReportDate() {
    return reportDate;
  }

  public Path getSchemaFilePath() {
    return schemaFilePath;
  }

  public Path getTemplateFilePath() {
    return templateFilePath;
  }

  /** @return the help */
  public boolean isHelp() {
    return help;
  }

  public void setArgs(String[] args) {
    this.args = args;
  }
}
