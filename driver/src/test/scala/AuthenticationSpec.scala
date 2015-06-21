import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfter
import reactivemongo.api.commands.bson.CommonImplicits
import reactivemongo.api.commands.{Role, DropUser, RoleInTheSameDB, CreateUser}

import scala.concurrent.Await

/**
 * Created by sh1ng on 20/06/15.
 */
class AuthenticationSpec extends Specification with BeforeAfter {

  import Common._
  import reactivemongo.api.commands.bson.BSONCreateUserImplicits._
  import reactivemongo.api.commands.bson.BSONDropUserImplicits._
  import CommonImplicits._

  val coll = db("authencicationSpec")

  val userName = "test_user"
  val pass = "password"
  val role = "readWrite"

  override def before: Any = {
    Await.ready(db.runCommand(CreateUser(userName, pass, List[Role](RoleInTheSameDB(role)))), timeout)
  }

  override def after: Any = {
    Await.ready(db.runCommand(DropUser(userName)), timeout)
  }

  "Authentication" should  {
    "successfully authenticate with user and password" in {
      Await.result(db.authenticate(userName, pass), timeout)
      success
    }
  }
}
