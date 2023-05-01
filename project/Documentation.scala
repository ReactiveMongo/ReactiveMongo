import sbt._
import sbt.Keys._

final class Documentation {

  val settings = Seq(
    apiMappings ++= Documentation
      .mappings("org.scala-lang", "http://scala-lang.org/api/%s/")(
        "scala-library"
      )
      .value,
    Compile / doc / tastyFiles ~= {
      _.filter {
        _.toString.indexOf("/external/") == -1
      }
    },
    Compile / doc / scalacOptions ++= {
      if (scalaBinaryVersion.value != "3") {
        Seq("-implicits")
      } else {
        Seq.empty
      }
    },
    Compile / doc / scalacOptions ++= Opts.doc.title("ReactiveMongo API"),
    Compile / doc / scalacOptions ++= Opts.doc.version(
      Common.majorVersion.value
    )
  )
}

object Documentation {
  def mappings(
      org: String,
      location: String,
      revision: String => String = identity
    )(names: String*
    ) = Def.task[Map[File, URL]] {

    (for {
      entry: Attributed[File] <- (Compile / fullClasspath).value
      module: ModuleID <- entry.get(moduleID.key)
      if module.organization == org
      if names.exists(module.name.startsWith)
      rev = revision(module.revision)
    } yield entry.data -> url(location.format(rev)))(scala.collection.breakOut)
  }
}
