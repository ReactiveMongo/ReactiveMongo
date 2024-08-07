ThisBuild / dynverVTagPrefix := false

ThisBuild / version := {
  val Stable = """([0-9]+)\.([0-9]+)\.([0-9]+)(|-RC[0-9]+)""".r

  (ThisBuild / dynverGitDescribeOutput).value match {
    case Some(descr) => {
      if ((ThisBuild / isSnapshot).value) {
        val previous = descr.ref match {
          case r @ sbtdynver.GitRef(tag) if r.isTag =>
            Some(tag)

          case _ =>
            (ThisBuild / previousStableVersion).value
        }

        previous match {
          case Some(previousVer) => {
            val current = (for {
              Seq(maj, min, patch, rc) <- Stable.unapplySeq(previousVer)
              nextPatch <- scala.util.Try(patch.toInt).map(_ + 1).toOption
              nextRc = {
                if (rc startsWith "-RC") {
                  scala.util
                    .Try(rc.stripPrefix("-RC").toInt)
                    .map(_ + 1)
                    .toOption
                } else {
                  Option.empty[Int]
                }
              }
            } yield {
              nextRc match {
                case Some(nrc) =>
                  s"${maj}.${min}.${patch}-RC${nrc}.SNAPSHOT"

                case _ =>
                  s"${maj}.${min}.${nextPatch}-SNAPSHOT"
              }
            }).getOrElse {
              println("Fails to determine qualified snapshot version")
              previousVer
            }

            current
          }

          case _ =>
            sys.error("Fails to determine previous stable version")
        }
      } else {
        descr.ref.value
      }
    }

    case _ =>
      sys.error("Fails to resolve Git information")
  }
}
