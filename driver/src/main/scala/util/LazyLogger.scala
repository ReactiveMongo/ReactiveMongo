/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.util

@deprecated("Internal: will be made private", "1.0.0-rc.1")
object LazyLogger {
  import org.slf4j.{ LoggerFactory, Logger }

  /**
   * Returns the lazy logger matching the SLF4J `name`.
   *
   * @param name the logger name
   */
  def apply(name: String): LazyLogger =
    new LazyLogger(LoggerFactory getLogger name)

  final class LazyLogger private[reactivemongo] (logger: Logger) {
    /** Returns the corresponding SLF4J logger. */
    def slf4j = logger

    def trace(s: => String): Unit = { if (logger.isTraceEnabled) logger.trace(s) }
    def trace(s: => String, e: => Throwable): Unit = {
      if (logger.isTraceEnabled) logger.trace(s, e)
    }

    lazy val isDebugEnabled = logger.isDebugEnabled
    def debug(s: => String): Unit = { if (isDebugEnabled) logger.debug(s) }
    def debug(s: => String, e: => Throwable): Unit = {
      if (isDebugEnabled) logger.debug(s, e)
    }

    def info(s: => String): Unit = { if (logger.isInfoEnabled) logger.info(s) }
    def info(s: => String, e: => Throwable): Unit = {
      if (logger.isInfoEnabled) logger.info(s, e)
    }

    def warn(s: => String): Unit = { if (logger.isWarnEnabled) logger.warn(s) }
    def warn(s: => String, e: => Throwable): Unit = {
      if (logger.isWarnEnabled) logger.warn(s, e)
    }

    def error(s: => String): Unit = { if (logger.isErrorEnabled) logger.error(s) }
    def error(s: => String, e: => Throwable): Unit = {
      if (logger.isErrorEnabled) logger.error(s, e)
    }
  }
}
