package external.reactivemongo

final class StaticListenerBinder {
  def connectionListener(): ConnectionListener =
    new reactivemongo.jmx.ConnectionListener()
}
