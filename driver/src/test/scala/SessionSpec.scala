package reactivemongo

import java.util.UUID

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.{
  DistributedSession,
  NodeSetSession,
  Session,
  SessionTransaction,
  WriteConcern
}

final class SessionSpec extends org.specs2.mutable.Specification {
  "Session" title

  sequential

  section("unit")

  "NodeSet session" should {
    val id = UUID.randomUUID()
    val session1 = new NodeSetSession(lsid = id)
    val session2 = new DistributedSession(lsid = id)

    "be created (without transaction)" >> {
      def spec(s: Session) = {
        s.lsid must_=== id and {
          s.causalConsistency must beTrue
        } and {
          s.transaction must beFailedTry[SessionTransaction]
        } and {
          s.operationTime must beNone
        }
      }

      "when not distributed" in spec(session1)

      "when distributed" in spec(session2)
    }

    {
      def specs(s: Session) = {
        "without cluster time" in {
          s.update(1L, None, None)

          s.operationTime must beSome(1L)
        }

        "with cluster time" in {
          s.update(2L, Some(3L), None)

          s.operationTime must beSome(2L)
        }
      }

      "be updated when not distributed" >> specs(session1)
      "be updated when distributed" >> specs(session2)
    }

    "not end or flag transaction before it's started" >> {
      def spec(s: Session) = s.endTransaction() must beNone and {
        s.transactionToFlag() must beFalse
      }

      "when not distributed" in spec(session1)

      "when distributed" in spec(session2)
    }

    "start transaction" >> {
      "if none is already started (when not distributed)" in {
        session1.startTransaction(WriteConcern.Default, None).
          aka("started tx") must beSuccessfulTry[(SessionTransaction, Boolean)].like {
            case (SessionTransaction(1L, Some(wc), None, false, None), true) =>
              wc must_=== WriteConcern.Default

          }
      }

      "when distributed" >> {
        "with failure without pinned node" in {
          session2.startTransaction(WriteConcern.Default, None).
            aka("started tx") must beFailedTry[(SessionTransaction, Boolean)].
            withThrowable[GenericDriverException](
              ".*Cannot start a distributed transaction without a pinned node.*")
        }

        "successfully with a pinned node if none already started" in {
          session2.startTransaction(WriteConcern.Default, Some("pinnedNode")).
            aka("started tx") must beSuccessfulTry[(SessionTransaction, Boolean)].like {
              case (SessionTransaction(
                1L, Some(wc), Some("pinnedNode"), false, None), true) =>
                wc must_=== WriteConcern.Default

            }
        }
      }
    }

    "not start transaction if already started" >> {
      "when not distributed" in {
        session1.startTransaction(WriteConcern.Default, None).
          aka("start tx") must beSuccessfulTry[(SessionTransaction, Boolean)].
          like {
            case (SessionTransaction(
              1L, Some(_), None, false, None),
              startedNewTx) => startedNewTx must beFalse
          }
      }

      "when distributed" in {
        session2.startTransaction(WriteConcern.Default, Some("pinnedNode")).
          aka("start tx") must beSuccessfulTry[(SessionTransaction, Boolean)].
          like {
            case (SessionTransaction(
              1L, Some(_), Some("pinnedNode"), false, None), startedNewTx) =>
              startedNewTx must beFalse
          }
      }
    }

    "flag transaction to start" >> {
      def spec(s: Session) = s.transactionToFlag() must beFalse and {
        s.transactionToFlag() must beTrue
      }

      "when not distributed" in spec(session1)

      "when distributed" in spec(session2)
    }

    "end transaction" in {
      def spec(s: Session) = {
        s.endTransaction() must beSome[SessionTransaction].like {
          case SessionTransaction(1L, Some(_), _, true, None) =>
            s.endTransaction() must beNone
        }
      }

      "when not distributed" in spec(session1)

      "when distributed" in spec(session2)
    }

    "start a second transaction" >> {
      "with pinned node ignored when not distributed" in {
        session1.startTransaction(WriteConcern.Default, Some("node")).
          aka("started tx") must beSuccessfulTry[(SessionTransaction, Boolean)].like {
            case (SessionTransaction(2L, Some(wc), None, false, None), true) =>
              wc must_=== WriteConcern.Default

          }
      }

      "when distributed" in {
        session2.startTransaction(WriteConcern.Default, Some("node2")).
          aka("started tx") must beSuccessfulTry[(SessionTransaction, Boolean)].like {
            case (SessionTransaction(
              2L, Some(wc), Some("node2"), false, None), true) =>
              wc must_=== WriteConcern.Default

          }
      }
    }
  }

  section("unit")
}
