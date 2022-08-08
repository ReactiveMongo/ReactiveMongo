package reactivemongo.core.nodeset

private[reactivemongo] sealed trait ConnectionStatus

private[reactivemongo] object ConnectionStatus {

  object Disconnected extends ConnectionStatus {
    override def toString = "Disconnected"
  }

  object Connecting extends ConnectionStatus {
    override def toString = "Connecting"
  }

  object Connected extends ConnectionStatus {
    override def toString = "Connected"
  }
}
