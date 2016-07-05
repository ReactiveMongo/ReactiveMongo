package reactivemongo.core

final class StaticListenerBinder {
  def connectionListener(): ConnectionListener =
    new reactivemongo.jmx.ConnectionListener()
}
