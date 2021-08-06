import sbt._
import sbt.Keys._

import sbtunidoc.{ ScalaUnidocPlugin => UnidocPlugin }
import sbtunidoc.BaseUnidocPlugin.autoImport._//

final class Documentation(excludes: Seq[ProjectReference]) {
  import UnidocPlugin.autoImport.ScalaUnidoc

  val settings = UnidocPlugin.projectSettings ++ Seq(
    ScalaUnidoc / unidoc / unidocProjectFilter := {
      inAnyProject -- inProjects(excludes: _*)
    },
    apiMappings ++= Documentation.mappings(
      "org.scala-lang", "http://scala-lang.org/api/%s/")("scala-library").value
  )
}

object Documentation {
  def apply(excludes: Seq[ProjectReference]): Documentation =
    new Documentation(excludes)

  def mappings(
    org: String,
    location: String,
    revision: String => String = identity)(
    names: String*) = Def.task[Map[File, URL]] {

    (for {
      entry: Attributed[File] <- (Compile / fullClasspath).value
      module: ModuleID <- entry.get(moduleID.key)
      if module.organization == org
      if names.exists(module.name.startsWith)
      rev = revision(module.revision)
    } yield entry.data -> url(location.format(rev)))(scala.collection.breakOut)
  }
}
