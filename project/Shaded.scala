import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts

import sbtassembly.{
  AssemblyKeys, MergeStrategy, PathList, ShadeRule
}, AssemblyKeys._

object Shaded {
  import XmlUtil.transformPomDependencies

  lazy val commonModule = Project("ReactiveMongo-Shaded", file("shaded")).
    settings(
      Common.baseSettings ++ Publish.settings ++ Seq(
        mimaPreviousArtifacts := Set.empty,
        crossPaths := false,
        autoScalaLibrary := false,
        resolvers += Resolver.mavenLocal,
        libraryDependencies ++= Seq(
          "io.netty" % "netty-handler" % Dependencies.netty
        ),
        assemblyShadeRules in assembly := Seq(
          ShadeRule.rename("io.netty.**" -> "reactivemongo.io.netty.@1").inAll
        ),
        assemblyMergeStrategy in assembly := {
          case "META-INF/io.netty.versions.properties" => MergeStrategy.last
          case x => (assemblyMergeStrategy in assembly).value(x)
        },
        pomPostProcess := transformPomDependencies(_ => None),
        makePom := makePom.dependsOn(assembly).value,
        packageBin in Compile := target.value / (
          assemblyJarName in assembly).value
      )
    )

  def nativeModule(classifier: String, nettyVariant: String): Project =
    Project(s"ReactiveMongo-Shaded-Native-${classifier}",
      file(s"shaded-native-${classifier}")).settings(
      Common.baseSettings ++ Publish.settings ++ Seq(
        name := "ReactiveMongo-Shaded-Native",
        version := {
          val ver = (version in ThisBuild).value
          val verClassifier = classifier.replaceAll("_", "-")

          if (ver endsWith "-SNAPSHOT") {
            s"${ver dropRight 9}-${verClassifier}-SNAPSHOT"
          } else {
            s"${ver}-${verClassifier}"
          }
        },
        mimaPreviousArtifacts := Set.empty,
        crossPaths := false,
        autoScalaLibrary := false,
        resolvers += Resolver.mavenLocal,
        libraryDependencies ++= Seq(
          (("io.netty" % s"netty-transport-native-${nettyVariant}" % Dependencies.netty).classifier(classifier)).
            exclude("io.netty", "netty-common").
            exclude("io.netty", "netty-transport")
            exclude("io.netty", "netty-buffer")
        ),
        assemblyShadeRules in assembly := Seq(
          ShadeRule.rename("io.netty.**" -> "reactivemongo.io.netty.@1").inAll
        ),
        assemblyMergeStrategy in assembly := {
          case "META-INF/io.netty.versions.properties" => MergeStrategy.last
          case x => (assemblyMergeStrategy in assembly).value(x)
        },
        pomPostProcess := transformPomDependencies(_ => None),
        makePom := makePom.dependsOn(assembly).value,
        packageBin in Compile := Def.task[File] {
          val dir = baseDirectory.value / "target" / (
            s"asm-${System.currentTimeMillis()}")

          IO.unzip(assembly.value, dir)

          // META-INF
          val metaInf = dir / "META-INF"

          IO.listFiles(metaInf, AllPassFilter).foreach { f =>
            val nme = f.getName

            if (nme startsWith "io.netty") {
              f.renameTo(metaInf / s"reactivemongo.${nme}")
            }
          }

          // Rename native libs
          val nativeDir = metaInf / "native"

          IO.listFiles(nativeDir, AllPassFilter).foreach { f =>
            val nme = f.getName

            if (nme startsWith "libnetty") {
              f.renameTo(nativeDir / s"libreactivemongo_${nme drop 3}")
            }
          }

          // New JAR
          IO.zip(Path.contentOf(dir), assembly.value)

          assembly.value
        }.dependsOn(assembly).value
      )
    ).dependsOn(commonModule % Provided)
}
