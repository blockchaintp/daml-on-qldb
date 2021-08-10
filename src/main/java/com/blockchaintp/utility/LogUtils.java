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
  private static final int TRACE = 3;

  /**
   *
   * @param levelName
   */
  public static void setRootLogLevel(final String levelName) {
    Level level = Level.valueOf(levelName.toUpperCase());
    LOG.info("Requested log level {}", new Object[] { levelName });
    if (level == null) {
      LOG.info("Invalid log level {}, root log level still set to the default value {}",
          new Object[] { levelName, LogManager.getRootLogger().getLevel().name() });
    } else {
      setRootLogLevel(level);
    }
  }

  /**
   *
   * @param levelCount
   */
  @SuppressWarnings("checkstyle:MagicNumber")
  public static void setRootLogLevel(final int levelCount) {
    int vCount;
    if (levelCount > TRACE) {
      vCount = TRACE;
    } else {
      vCount = levelCount;
    }

    switch (vCount) {
    case 0:
    default:
      setRootLogLevel(Level.WARN);
      break;
    case 1:
      setRootLogLevel(Level.INFO);
      break;
    case 2:
      setRootLogLevel(Level.DEBUG);
      break;
    case TRACE:
      setRootLogLevel(Level.TRACE);
    }

  }

  /**
   *
   * @param level
   */
  public static void setRootLogLevel(final Level level) {
    Configurator.setRootLevel(level);
    LOG.info("Root log level set to {}", new Object[] { level });
  }

  /**
   *
   * @param clazz
   * @param levelName
   */
  public static void setLogLevel(final Class<?> clazz, final String levelName) {
    setLogLevel(clazz.getName(), levelName);
  }

  /**
   *
   * @param loggerName
   * @param levelName
   */
  public static void setLogLevel(final String loggerName, final String levelName) {
    LogManager.getLogger(LogUtils.class);
    Level level = Level.getLevel(levelName.toUpperCase());
    if (level == null) {
      LOG.info("Invalid log level {}, logger {} log level still set to the default value",
          new Object[] { levelName, LogManager.getLogger(loggerName).getLevel().name() });
    } else {
      setLogLevel(loggerName, level);
    }
  }

  /**
   *
   * @param loggerName
   * @param level
   */
  public static void setLogLevel(final String loggerName, final Level level) {
    Configurator.setLevel(loggerName, level);
    LOG.info("Logger {} log level set to {}", new Object[] { loggerName, level });
  }

  private LogUtils() {
  }
}
