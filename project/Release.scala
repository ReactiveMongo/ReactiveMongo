import sbt.Keys._
import sbt._

import sbtrelease.{ ReleaseStateTransformations, Version }

object Release {
  import sbtrelease.ReleasePlugin.autoImport._

  private val gitRemote = "origin"

  private def createLocalBranch(f: (String, String) => String) = Def.setting {
    val vcs = releaseVcs.value.get
    val next = releaseVersion.value
    val ver = version.value
    val releaseBranch = f(ver, next(ver))

    ReleaseStep(action = { st =>
      if ((vcs.cmd("checkout", "-b", releaseBranch) ! st.log) == 0) {
        vcs.cmd("push", "-u", gitRemote, releaseBranch) !! st.log
        // Need to push for the plugin checks
      }

      st
    })
  }

  // Create a local `release/$releaseVer` branch
  private val createReleaseBranch =
    createLocalBranch { (_, ver) => s"release/$ver" }

  // Create a local `bump/$releaseVer` branch
  private val createBumpBranch = createLocalBranch { (_, ver) => s"bump/$ver" }

  private val pushCurrentBranch = Def.setting {
    val vcs = releaseVcs.value.get

    ReleaseStep(action = { st =>
      vcs.cmd("push", gitRemote, vcs.currentBranch) !! st.log

      st
    })
  }

  // 1. Prepare the release the SNAPSHOT from develop to master, with a branch
  private val releaseMaster = Def.setting {
    Seq[ReleaseStep](
      createReleaseBranch.value, // Create a release branch
      ReleaseStateTransformations.checkSnapshotDependencies,
      ReleaseStateTransformations.inquireVersions,
      ReleaseStateTransformations.setReleaseVersion,
      ReleaseStateTransformations.commitNextVersion,
      pushCurrentBranch.value
    )
  }

  // No tracking branch is set up -> set upstream

  // 2. Validate the pushed release branch with CI
  // 3. Merge the branch on master
  // 4. Add a tag

  // 5. Prepare the coming release
  private val bumpMaster = Def.setting {
    Seq[ReleaseStep](
      createBumpBranch.value, // Create a bump branch
      ReleaseStateTransformations.checkSnapshotDependencies,
      ReleaseStateTransformations.inquireVersions,
      ReleaseStateTransformations.setNextVersion,
      ReleaseStateTransformations.commitNextVersion,
      pushCurrentBranch.value
    )
  }

  val major = Def.setting[String] {
    Version(version.value) match {
      case Some(Version(maj, Seq(min, _), _)) => s"${maj}.${min}"
      case _ => sys.error(s"Invalid version: ${version.value}")
    }
  }

  val settings = Seq(
    releaseVersion := { ver =>
      Version(ver).map(_.withoutQualifier.string).
        getOrElse(sbtrelease.versionFormatError)
    },
    releaseNextVersion := { ver =>
      // e.g. 1.2 => 1.3-SNAPSHOT
      Version(ver).map(_.bumpBugfix.asSnapshot.string).
        getOrElse(sbtrelease.versionFormatError)
    },
    releaseCommitMessage := {
      val ver = (version in ThisBuild).value

      if (ver endsWith "-SNAPSHOT") {
        // Bump for the next coming sprint, on develop
        s"Bump to $ver"
      } else {
        // Prepare the release the SNAPSHOT from master, with a release branch
        s"Release $ver"
      }
    },
    releaseProcess := {
      if (version.value endsWith "-SNAPSHOT") releaseMaster.value
      else bumpMaster.value
    }
  )
}
