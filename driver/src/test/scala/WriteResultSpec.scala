import reactivemongo.api.commands.{ CommandError, DefaultWriteResult }

class WriteResultSpec extends org.specs2.mutable.Specification {
  "Write result" title

  section("unit")
  "WriteResult" should {
    val error = DefaultWriteResult(
      ok = false,
      n = 1,
      writeErrors = Nil,
      writeConcernError = None,
      code = Some(23),
      errmsg = Some("Foo"))

    "be matched as a CommandError when failed" in {
      error must beLike {
        case CommandError.Code(code) => code must_== 23
      } and (error must beLike {
        case CommandError.Message(msg) => msg must_== "Foo"
      })
    }

    "not be matched as a CommandError when successful" in {
      (error.copy(ok = true) match {
        case CommandError.Code(_) | CommandError.Message(_) => true
        case _ => false
      }) must beFalse
    }
  }
  section("unit")
}
