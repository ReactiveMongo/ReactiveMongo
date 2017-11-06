package reactivemongo.core.nodeset

sealed trait ConnectionStatus

object ConnectionStatus {
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
