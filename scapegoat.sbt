scapegoatVersion in ThisBuild := "1.3.9"

scapegoatReports in ThisBuild := Seq("text") // xml in 2.13 require extra dep

scapegoatDisabledInspections in ThisBuild := Seq("FinalModifierOnCaseClass")

pomPostProcess := {
  object Processor {
    def apply() = transformPomDependencies { dep =>
      if ((dep \ "groupId").text == "com.sksamuel.scapegoat") {
        None
      } else Some(dep)
    }

    // ---

    import scala.xml.{ Elem => XmlElem, Node => XmlNode }

    private def transformPomDependencies(tx: XmlElem => Option[XmlNode]): XmlNode => XmlNode = { node: XmlNode =>
      import scala.xml.{ NodeSeq, XML }
      import scala.xml.transform.{ RewriteRule, RuleTransformer }

      val tr = new RuleTransformer(new RewriteRule {
        override def transform(node: XmlNode): NodeSeq = node match {
          case e: XmlElem if e.label == "dependency" => tx(e) match {
            case Some(n) => n
            case _       => NodeSeq.Empty
          }

          case _ => node
        }
      })

      tr.transform(node).headOption match {
        case Some(transformed) => transformed
        case _                 => sys.error("Fails to transform the POM")
      }
    }
  }

  Processor()
}
