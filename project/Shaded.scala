import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts

import sbtassembly.{
  AssemblyKeys, MergeStrategy, PathList, ShadeRule
}, AssemblyKeys._

object Shaded {
  import XmlUtil.transformPomDependencies

  lazy val module = Project("ReactiveMongo-Shaded", file("shaded")).
    settings(
      BuildSettings.baseSettings ++ Publish.settings ++ Seq(
        mimaPreviousArtifacts := Set.empty,
        crossPaths := false,
        autoScalaLibrary := false,
        resolvers += Resolver.mavenLocal,
        libraryDependencies ++= Seq(
          "io.netty" % "netty-handler" % Dependencies.netty,
          "com.google.guava" % "guava" % "19.0" cross CrossVersion.Disabled
        ),
        assemblyShadeRules in assembly := Seq(
          ShadeRule.rename("io.netty.**" -> "shaded.netty.@1").inAll,
          ShadeRule.rename("com.google.**" -> "shaded.google.@1").inAll
        ),
        assemblyMergeStrategy in assembly := {
          case "META-INF/io.netty.versions.properties" => MergeStrategy.last
          case x => (assemblyMergeStrategy in assembly).value(x)
        },
        pomPostProcess := transformPomDependencies { _ => None },
        makePom := makePom.dependsOn(assembly).value,
        packageBin in Compile := target.value / (
          assemblyJarName in assembly).value
      )
    )
}
