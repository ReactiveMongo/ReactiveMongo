package reactivemongo.core

final class NettyPackSpec extends org.specs2.mutable.Specification {
  "Netty pack".title

  "Pack" should {
    import netty.Pack

    lazy val expected: Pack = _root_.tests.Common.nettyNativeArch match {
      case Some("linux") => Pack.epoll.orNull
      case Some("osx")   => Pack.kqueue.orNull
      case _             => Pack.nio
    }

    s"be ${expected}" in {
      Pack() must_=== expected
    }
  }
}
