import sbt._
import sbt.Keys._

import xsbti.HashedVirtualFileRef

final class Documentation {

  val settings = Seq(
    apiMappings ++= Def.uncached {
      Documentation
        .mappings("org.scala-lang", "http://scala-lang.org/api/%s/")(
          "scala-library"
        )
        .value
    },
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
    ) = Def.task[Map[HashedVirtualFileRef, URI]] {

    (for {
      entry <- (Compile / fullClasspath).value
      module: ModuleID <- entry
        .get(moduleIDStr)
        .map(Classpaths.moduleIdJsonKeyFormat.read)
      if module.organization == org
      if names.exists(module.name.startsWith)
    } yield entry.data -> url(location.format(revision(module.revision)))).toMap
  }
}
