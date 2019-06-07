package reactivemongo.api

import java.util.UUID

final class SessionSpec extends org.specs2.mutable.Specification {
  "Session" title

  sequential

  section("unit")

  "ReplicaSet session" should {
    val id = UUID.randomUUID()
    val session = new ReplicaSetSession(lsid = id)

    "be created (without transaction)" in {
      session.lsid must_=== id and (session.causalConsistency must beTrue) and {
        session.transaction must beNone
      } and {
        session.operationTime must beNone
      }
    }

    "be updated" >> {
      "without cluster time" in {
        session.update(1L, None)

        session.operationTime must beSome(1L)
      }

      "with cluster time" in {
        session.update(2L, Some(3L))

        session.operationTime must beSome(2L)
      }
    }

    "not end or flag transaction before it's started" in {
      session.endTransaction() must beNone and {
        session.transactionToFlag() must beFalse
      }
    }

    "start transaction if none is already started" in {
      session.startTransaction(WriteConcern.Default).
        aka("started tx") must beSome[SessionTransaction].like {
          case SessionTransaction(1L, Some(wc), false) =>
            wc must_=== WriteConcern.Default

        }
    }

    "not start transaction if already started" in {
      session.startTransaction(WriteConcern.Default) must beNone and {
        session.transaction must beSome[SessionTransaction].like {
          case SessionTransaction(1L, Some(wc), false) =>
            wc must_=== WriteConcern.Default
        }
      }
    }

    "flag transaction to start" in {
      session.transactionToFlag() must beFalse and {
        session.transactionToFlag() must beTrue
      }
    }

    "end transaction" in {
      session.endTransaction() must beSome[SessionTransaction].like {
        case SessionTransaction(1L, Some(_), true) =>
          session.endTransaction() must beNone
      }
    }

    "start a second transaction" in {
      session.startTransaction(WriteConcern.Default).
        aka("started tx") must beSome[SessionTransaction].like {
          case SessionTransaction(2L, Some(wc), false) =>
            wc must_=== WriteConcern.Default

        }
    }
  }

  section("unit")
}
