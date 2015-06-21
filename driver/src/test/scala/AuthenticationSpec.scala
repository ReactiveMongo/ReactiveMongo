import org.specs2.mutable.{BeforeAfter, Specification}
import reactivemongo.api.commands.bson.CommonImplicits
import reactivemongo.api.commands.{Role, DropUser, RoleInTheSameDB, CreateUser}
import reactivemongo.core.commands.CommandError

import scala.concurrent.Await

/**
 * Created by sh1ng on 20/06/15.
 */
class AuthenticationSpec extends Specification {
  sequential

  import Common._
  import reactivemongo.api.commands.bson.BSONCreateUserImplicits._
  import reactivemongo.api.commands.bson.BSONDropUserImplicits._
  import CommonImplicits._

  val coll = db("authencicationSpec")

  val userName = "test_user"
  val pass = "password"
  val role = "readWrite"

  "Authentication" should  {
    "create user with readWrite role" in {
      Await.ready(db.runCommand(CreateUser(userName, pass, List[Role](RoleInTheSameDB(role)))), timeout)
      success
    }
    "unsuccessfully authenticate with incorrect password" in {
      Await.result(db.authenticate(userName, ""), timeout) should throwA[CommandError]
    }
    "successfully authenticate with user and password" in {
      Await.result(db.authenticate(userName, pass), timeout)
      success
    }
    "drop user" in {
      Await.ready(db.runCommand(DropUser(userName)), timeout)
      success
    }
  }
}
