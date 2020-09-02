dynverVTagPrefix in ThisBuild := false

version in ThisBuild := {
  val Stable = """([0-9]+)\.([0-9]+)\.([0-9]+)""".r

  (dynverGitDescribeOutput in ThisBuild).value match {
    case Some(descr) => {
      if ((isSnapshot in ThisBuild).value) {
        (previousStableVersion in ThisBuild).value match {
          case Some(previousVer) => {
            val current = (for {
              Seq(maj, min, patch) <- Stable.unapplySeq(previousVer)
              nextPatch <- scala.util.Try(patch.toInt).map(_ + 1).toOption
            } yield {
              s"${maj}.${min}.${nextPatch}-SNAPSHOT"
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
