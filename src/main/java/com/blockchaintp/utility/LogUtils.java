/*
 * Copyright 2021 Blockchain Technology Partners
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.blockchaintp.utility;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 *
 */
public final class LogUtils {
  private static final Logger LOG = LogManager.getLogger();
  private static final int WARN_LOG = 0;
  private static final int INFO_LOG = 1;
  private static final int DEBUG_LOG = 2;
  private static final int TRACE_LOG = 3;

  /**
   * Set the root log level to the specified level. If the levelName is invalid or unknown, the level
   * will not be changed.
   *
   * @param levelName
   */
  public static void setRootLogLevel(final String levelName) {
    setLogLevel(LogManager.getRootLogger().getName(), levelName);
  }

  /**
   * Set the log level for the level corresponding to the flag number.
   *
   * @param levelCount
   *          number of levels to set
   */
  public static void setRootLogLevel(final int levelCount) {
    int vCount;
    if (levelCount > TRACE_LOG) {
      vCount = TRACE_LOG;
    } else {
      vCount = levelCount;
    }

    switch (vCount) {
    case INFO_LOG:
      setRootLogLevel(Level.INFO);
      break;
    case DEBUG_LOG:
      setRootLogLevel(Level.DEBUG);
      break;
    case TRACE_LOG:
      setRootLogLevel(Level.TRACE);
      break;
    case WARN_LOG:
    default:
      setRootLogLevel(Level.WARN);
      break;
    }

  }

  /**
   * Set the root log level to the given level.
   *
   * @param level
   */
  public static void setRootLogLevel(final Level level) {
    setLogLevel(LogManager.getRootLogger(), level);
  }

  /**
   * Set the logger for the given class to be the given level.
   *
   * @param clazz
   * @param levelName
   */
  public static void setLogLevel(final Class<?> clazz, final String levelName) {
    setLogLevel(clazz.getName(), levelName);
  }

  /**
   * Set the logger of the given name to the given level. If the level name is unknown, then the level
   * will be unchanged.
   *
   * @param loggerName
   * @param levelName
   */
  public static void setLogLevel(final String loggerName, final String levelName) {
    LogManager.getLogger(LogUtils.class);
    var logger = LogManager.getLogger(loggerName);
    var defaultLevel = logger.getLevel();
    var level = levelForName(levelName, defaultLevel);
    setLogLevel(logger, level);
  }

  /**
   * Set the log level for the given logger and level.
   *
   * @param logger
   *          the logger
   * @param level
   *          the level
   */
  public static void setLogLevel(final Logger logger, final Level level) {
    var defaultLevel = logger.getLevel();
    var levelName = level.name();
    var loggerDisplayName = logger.getName();
    if (loggerDisplayName.equals(LogManager.ROOT_LOGGER_NAME)) {
      loggerDisplayName = "ROOT";
    }
    if (level.equals(defaultLevel)) {
      LOG.trace("Logger {} already at level {}", loggerDisplayName, levelName);
    } else {
      Configurator.setLevel(logger.getName(), level);
      LOG.info("Logger {} log level set to {}", loggerDisplayName, levelName);
    }
  }

  /**
   * Set the given logger to the given level.
   *
   * @param loggerName
   * @param level
   */
  public static void setLogLevel(final String loggerName, final Level level) {
    setLogLevel(LogManager.getLogger(loggerName), level);
  }

  /**
   * Fetches the level for the given level name defaulting to the given defaultLevel if there are any
   * issues.
   */
  private static Level levelForName(final String levelName, final Level defaultLevel) {
    try {
      return Level.valueOf(levelName.toUpperCase());
    } catch (NullPointerException | IllegalArgumentException e) {
      LOG.warn("Invalid log level {}", levelName);
      return defaultLevel;
    }
  }

  private LogUtils() {
  }
}
