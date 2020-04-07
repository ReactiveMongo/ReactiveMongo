import sbt._
import sbt.Keys._

object Travis {
  val travisEnv = taskKey[Unit]("Print Travis CI env")

  lazy val settings = Seq(
    travisEnv in Test := { // test:travisEnv from SBT CLI
      val (mongoLower, mongoUpper) = "3" -> "4"
      val (jdkLower, jdkUpper) = "openjdk8" -> "oraclejdk9"

      // Scala
      import Common.{ scala211, scala213 }
      val scala212 = scalaVersion.value

      // Major libs
      val (akkaLower, akkaUpper) = "2.3.13" -> "2.6.5"
      val (playLower, playUpper) = "2.3.8" -> "2.6.1"

      // Base specifications for the integration envs
      val integrationSpecs = List[(String, List[String])](
        "MONGO_VER" -> List(mongoLower, mongoUpper),
        "MONGO_PROFILE" -> List(
          "default", "invalid-ssl", "mutual-ssl", "rs", "x509"),
        "AKKA_VERSION" -> List(akkaLower, akkaUpper)
      )

      // Base specifications about JDK/Scala
      val javaSpecs = List(
        "openjdk8", "oraclejdk9",
        s"scala${scala213}", s"scala${scala212}", s"scala${scala211}"
      ).combinations(2).flatMap {
        case jdk :: scala :: Nil if (
          !jdk.startsWith("scala") && scala.startsWith("scala")) => {
          val sv = scala.drop(5)

          if (jdk == jdkLower && sv != scala211) {
            List.empty[(String, String)]
          } else {
            List(jdk -> sv)
          }
        }

        case _ => List.empty
      }.toList

      type Variables = List[(String, String)]

      lazy val integrationEnv: List[Variables] = integrationSpecs.flatMap {
        case (key, values) => values.map(key -> _)
      }.combinations(integrationSpecs.size).filterNot { flags =>
        /* chrono-compat exclusions */
        (flags.contains("MONGO_VER" -> mongoUpper) && flags.
          contains("AKKA_VERSION" -> akkaLower)) ||
        (flags.contains("AKKA_VERSION" -> akkaLower) && flags.
          contains("MONGO_PROFILE" -> "rs")) ||
        /* profile exclusions */
        (!flags.contains("MONGO_VER" -> mongoUpper) && flags.
          contains("MONGO_PROFILE" -> "invalid-ssl")) ||
        (!flags.contains("MONGO_VER" -> mongoUpper) && flags.
          contains("MONGO_PROFILE" -> "mutual-ssl")) ||
        (!flags.contains("MONGO_VER" -> mongoUpper) && flags.
          contains("MONGO_PROFILE" -> "x509")) ||
        (flags.contains("MONGO_VER" -> mongoLower) && flags.
          contains("MONGO_PROFILE" -> "rs"))
      }.collect {
        case flags if (flags.map(_._1).toSet.size == integrationSpecs.size) =>
          flags.sortBy(_._1)
      }.toList

      @inline def integrationVars(flags: List[(String, String)]): String =
        flags.map { case (k, v) => s"$k=$v" }.mkString(" ")

      // Integration according desired constraints per Java combination
      case class JavaIntegrationEnv(
        jdk: String,
        scalaVer: String,
        variables: List[Variables] = List.empty)

      val javaIntegration = javaSpecs.map {
        case env @ (`jdkUpper`, scala) =>
          JavaIntegrationEnv(
            jdk = jdkUpper,
            scalaVer = scala,
            variables = integrationEnv.filter { vars =>
              !vars.exists {
                case ("AKKA_VERSION", `akkaLower`) => true
                case ("MONGO_VER", `mongoLower`) => true
                case ("MONGO_PROFILE", p) =>
                  scala == scala213 && p != "rs" && p != "x509"

                case _ => false
              }
            })

        case env @ (jdk, scala) =>
          JavaIntegrationEnv(jdk, scala,
            variables = integrationEnv.filter { vars =>
              !vars.exists {
                case ("AKKA_VERSION", `akkaUpper`) => true
                case ("MONGO_VER", ver) => ver != mongoLower
                case _ => false
              }
            })

      }

      def integrationMatrix =
        integrationEnv.map(integrationVars).map { c => s"  - $c" }

      val unitTestEnv = "CI_CATEGORY=UNIT_TESTS"

      val unitTestIncludes = List(
        "  - os: osx",
        s"    osx_image: xcode9.4",
        s"    language: java",
        s"    env: ${unitTestEnv} OS_NAME=osx TRAVIS_SCALA_VERSION=${scala211} REACTIVEMONGO_SHADED=false",
        "  - os: linux",
        s"    env: ${unitTestEnv} REACTIVEMONGO_SHADED=false",
        s"    jdk: ${jdkUpper}",
        s"    scala: ${scala212}")

      // Linux only integration
      val linuxIncludes = javaIntegration.flatMap {
        case JavaIntegrationEnv(jdk, scala, vars) =>
          vars.flatMap { cs =>
            val env = integrationVars(cs)

            List(
              "  - os: linux",
              s"    jdk: ${jdk}",
              s"    scala: ${scala}",
              s"    env: CI_CATEGORY=INTEGRATION_TESTS ${env}")
          }
      }

      val _matrix = List(
        "matrix:",
        "  include:") ++ unitTestIncludes ++ linuxIncludes

      def matrix = (List("env:", "  - CI_CATEGORY=UNIT_TESTS").iterator ++ (
        integrationMatrix :+ "matrix: " :+ "  exclude: ") ++ (
        integrationEnv.map(integrationVars).flatMap { v => Seq(
          "    - os: osx", s"      env: ${v}")
        }) ++ Seq(
        "    - os: osx", //
          s"      scala: ${scala212}",
          "      env: CI_CATEGORY=UNIT_TESTS",
        "    - os: linux", //
          s"      scala: ${scala212}",
          "      env: CI_CATEGORY=UNIT_TESTS",
        "    - os: linux", //
          s"      scala: ${scala211}",
          "      env: CI_CATEGORY=UNIT_TESTS",
        s"    - scala: ${scala212}", //
          "      jdk: openjdk8",
          "      env: CI_CATEGORY=UNIT_TESTS") ++ (
        integrationEnv.flatMap { flags =>
          if (flags.contains("CI_CATEGORY" -> "INTEGRATION_TESTS") &&
            (/* time-compat exclusions: */
                flags.contains("AKKA_VERSION" -> akkaUpper) ||
                flags.contains("MONGO_VER" -> mongoUpper) ||
                /* profile priority exclusions: */
                flags.contains("MONGO_PROFILE" -> "invalid-ssl") ||
                flags.contains("MONGO_PROFILE" -> "mutual-ssl"))) {
            List(
              s"    - scala: ${scala211}",
              s"      env: ${integrationVars(flags)}",
              "    - jdk: openjdk8",
              s"      env: ${integrationVars(flags)}"
            )
          } else if (flags.contains("CI_CATEGORY" -> "INTEGRATION_TESTS") &&
            (/* time-compat exclusions: */
                flags.contains("AKKA_VERSION" -> akkaLower) ||
                flags.contains("MONGO_VER" -> mongoLower)
            )) {
            List(
              s"    - scala: ${scala212}",
              s"      env: ${integrationVars(flags)}",
              "    - jdk: oraclejdk9",
              s"      env: ${integrationVars(flags)}"
            )
          } else List.empty[String]
        })
      ).mkString("\r\n")

      println(s"# Travis CI env\r\n${_matrix.mkString("\r\n")}")
    }
  )
}
