import scala.xml.{ Elem => XmlElem, Node => XmlNode, NodeSeq, XML }
import scala.xml.transform.{ RewriteRule, RuleTransformer }

object XmlUtil {
  def transformPomDependencies(tx: XmlElem => Option[XmlNode]): XmlNode => XmlNode = { node: XmlNode =>
    val tr = new RuleTransformer(new RewriteRule {
      override def transform(node: XmlNode): NodeSeq = node match {
        case e: XmlElem if e.label == "dependency" => tx(e) match {
          case Some(n) => n
          case _ => NodeSeq.Empty
        }

        case _ => node
      }
    })

    tr.transform(node).headOption match {
      case Some(transformed) => transformed
      case _ => sys.error("Fails to transform the POM")
    }
  }
}
