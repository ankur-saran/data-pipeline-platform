package com.pipeline.common

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.logging.log4j.LogManager

import java.nio.file.{Files, Paths}

/** Loads layered HOCON config:
 *
 *   config/<env>/application.conf  (env-specific overrides)
 *     overrides
 *   resources/application.conf     (base defaults on classpath)
 *
 *  Any key of the form `\${?ENV_VAR}` is resolved from the process environment,
 *  so secrets never appear in config files.
 */
object ConfigLoader {

  private val log = LogManager.getLogger(getClass)

  /** @param env  Pipeline environment: dev | staging | prod */
  def load(env: String): Config = {
    val envConfPath = s"config/$env/application.conf"

    val envConf: Config = if (Files.exists(Paths.get(envConfPath))) {
      log.info(s"Loading env config from: $envConfPath")
      ConfigFactory.parseFile(Paths.get(envConfPath).toFile)
    } else {
      log.warn(s"Env config not found at $envConfPath, using defaults only")
      ConfigFactory.empty()
    }

    val resolved = envConf
      .withFallback(ConfigFactory.load("application"))
      .resolve()

    log.debug(s"Resolved config:\n${resolved.root().render(ConfigRenderOptions.concise())}")
    resolved
  }

  implicit class RichConfig(val config: Config) extends AnyVal {
    def getStringOpt(path: String): Option[String] =
      if (config.hasPath(path)) Some(config.getString(path)) else None

    def getIntOpt(path: String): Option[Int] =
      if (config.hasPath(path)) Some(config.getInt(path)) else None

    def getDoubleOpt(path: String): Option[Double] =
      if (config.hasPath(path)) Some(config.getDouble(path)) else None
  }
}
