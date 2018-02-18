import scala.xml.{ NodeSeq, XML }, XML.{ loadFile => loadXML }

import sbt._
import sbt.Keys._

import com.github.sbt.findbugs.FindbugsPlugin, FindbugsPlugin.autoImport._

object Findbugs {
  //@inline def task = FindBugs.findbugs

  lazy val settings = Seq(
    findbugsReportType := Some(FindbugsReport.PlainHtml),
    findbugsReportPath := Some(target.value / "findbugs.html"),
    findbugsExcludeFilters := {
      val commonFilters = loadXML(baseDirectory.value / ".." / "project" / (
        "findbugs-exclude-filters.xml"))

      val filters = {
        val f = baseDirectory.value / "findbugs-exclude-filters.xml"
        if (!f.exists) NodeSeq.Empty else loadXML(f).child
      }

      Some(
        <FindBugsFilter>${commonFilters.child}${filters}</FindBugsFilter>
      )
    }
  )
}
