import sbt._
import sbt.Keys._

object ReactiveMongoBuild {
  /*
  import BuildSettings._
  import Resolvers._
  import Dependencies._
  import XmlUtil._

  import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts

  import sbtassembly.{
    AssemblyKeys, MergeStrategy, PathList, ShadeRule
  }, AssemblyKeys._

  lazy val native = Project(
    "ReactiveMongo-Native",
    file("shaded-native"),
    settings = baseSettings ++ Publish.settings ++ Seq(
      mimaPreviousArtifacts := Set.empty,
      crossPaths := false,
      autoScalaLibrary := false,
      resolvers += Resolver.mavenLocal,
      libraryDependencies ++= Seq(
        ("io.netty" % "netty-transport-native-kqueue" % Dependencies.netty classifier "osx-x86_64").
          exclude("io.netty", "netty-common").
          exclude("io.netty", "netty-transport")
          exclude("io.netty", "netty-buffer")
      ),
      assemblyShadeRules in assembly := Seq(
        ShadeRule.rename("io.netty.**" -> "shaded.io.netty.@1").inAll
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
  ).dependsOn(shaded % Provided)
   */
}
