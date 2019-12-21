import scala.xml.{ Elem => XmlElem, Node => XmlNode }

import sbt._
import sbt.Keys._

import com.typesafe.tools.mima.plugin.MimaKeys.mimaPreviousArtifacts

import com.github.sbt.cpd.CpdPlugin

final class Jmx(driver: Project) {
  import Dependencies._
  import XmlUtil._

  lazy val module = Project("ReactiveMongo-JMX", file("jmx")).
    enablePlugins(CpdPlugin).
    dependsOn(driver).
    settings(
      Common.settings ++ Findbugs.settings ++ Seq(
        mimaPreviousArtifacts := Set.empty,
        testOptions in Test += Tests.Cleanup(Common.cleanup.value),
        libraryDependencies ++= Seq(specs.value) ++ logApi,
        libraryDependencies ++= {
          if (!Common.useShaded.value) {
            Seq(Dependencies.netty % Provided)
          } else {
            Seq.empty[ModuleID]
          }
        },
        pomPostProcess := providedInternalDeps
      )
    )

  // ---

  private lazy val providedInternalDeps: XmlNode => XmlNode = {
    import scala.xml.NodeSeq
    import scala.xml.transform.{ RewriteRule, RuleTransformer }

    val asProvided = new RuleTransformer(new RewriteRule {
      override def transform(node: XmlNode): NodeSeq = node match {
        case e: XmlElem if e.label == "scope" =>
          NodeSeq.Empty

        case _ => node
      }
    })

    transformPomDependencies { dep: XmlElem =>
      if ((dep \ "groupId").text == "org.reactivemongo") {
        asProvided.transform(dep).headOption.collectFirst {
          case e: XmlElem => e.copy(
            child = e.child :+ <scope>provided</scope>)
        }
      } else Some(dep)
    }
  }
}
