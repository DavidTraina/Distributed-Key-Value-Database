package logger;

import java.io.IOException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/** Represents the initialization for the server logging with Log4J. */
public class LogSetup {

  public static final String UNKNOWN_LEVEL = "UnknownLevel";
  private static final Logger logger = Logger.getRootLogger();
  private final String logdir;
  private boolean logToStdout = false;

  /**
   * Initializes the logging for the echo server. Logs are appended to the console output and
   * written into a separated server log file at a given destination.
   *
   * @param logdir the destination (i.e. directory + filename) for the persistent logging
   *     information.
   * @throws IOException if the log destination could not be found.
   */
  public LogSetup(String logdir, Level level, boolean logToStdout) throws IOException {
    this.logdir = logdir;
    this.logToStdout = logToStdout;
    initialize(level);
  }

  private void initialize(Level level) throws IOException {
    PatternLayout layout = new PatternLayout("%-5p | %d{ISO8601} | [%t] | %l --> %m%n");

    FileAppender fileAppender = new FileAppender(layout, logdir, false);

    if (logToStdout) {
      ConsoleAppender consoleAppender = new ConsoleAppender(layout);
      logger.addAppender(consoleAppender);
    }
    logger.addAppender(fileAppender);
    logger.setLevel(level);
  }
}
