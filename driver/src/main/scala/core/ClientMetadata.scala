package reactivemongo.core

import reactivemongo.api.{ SerializationPack, Version }

private[reactivemongo] final class ClientMetadata(
  val application: String) extends AnyVal {
  @inline override def toString = application
}

private[reactivemongo] object ClientMetadata {
  case class Driver(name: String, version: String)

  def serialize[P <: SerializationPack](pack: P): ClientMetadata => Option[pack.Document] = {
    val maxSize = 512
    val builder = pack.newBuilder

    import builder.{ elementProducer => elem, document, string => str }

    lazy val driver = document(Seq(
      elem("name", str("ReactiveMongo")),
      elem("version", str(Version.toString))))

    lazy val fullOs: pack.ElementProducer = {
      import sys.props

      val operatingSystemName =
        props.getOrElse("os.name", "unknown").toLowerCase

      val osType = {
        if (operatingSystemName startsWith "linux") "Linux"
        else if (operatingSystemName startsWith "mac") "Darwin"
        else if (operatingSystemName startsWith "windows") "Windows"
        else if (operatingSystemName.startsWith("hp-ux") ||
          operatingSystemName.startsWith("aix") ||
          operatingSystemName.startsWith("irix") ||
          operatingSystemName.startsWith("solaris") ||
          operatingSystemName.startsWith("sunos")) {
          "Unix"
        } else {
          "unknown"
        }
      }

      elem("os", document(Seq(
        elem("type", str(osType)),
        elem("name", str(operatingSystemName)),
        elem("architecture", str(props.getOrElse("os.arch", "unknown"))),
        elem("version", str(props.getOrElse("os.version", "unknown"))))))

    }

    lazy val miniOs: pack.ElementProducer =
      elem("os", document(Seq(elem("type", str("unknown")))))

    lazy val miniMeta: Option[pack.Document] = {
      val doc = document(Seq(elem("driver", driver), miniOs))

      if (pack.bsonSize(doc) > maxSize) {
        Option.empty[pack.Document]
      } else {
        Some(doc)
      }
    }

    // ---

    { metadata =>
      val base = Seq(
        elem("application", document(Seq(
          elem("name", str(metadata.application))))),
        elem("driver", driver))

      val baseDoc = document(base)

      if (pack.bsonSize(baseDoc) > maxSize) {
        miniMeta
      } else {
        val platformElms =
          elem("platform", str(s"Scala ${Version.scalaBinaryVersion}")) +: base

        val fullMeta = document(fullOs +: platformElms)

        if (pack.bsonSize(fullMeta) < maxSize) {
          Some(fullMeta)
        } else {
          val osMeta = document(miniOs +: platformElms)

          if (pack.bsonSize(osMeta) < maxSize) Some(osMeta)
          else miniMeta
        }
      }
    }
  }
}
