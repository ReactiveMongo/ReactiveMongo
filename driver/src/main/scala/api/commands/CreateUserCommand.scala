package reactivemongo.api.commands

import reactivemongo.api.{
  AuthenticationMode,
  PackSupport,
  SerializationPack,
  WriteConcern
}

/**
 * [[https://docs.mongodb.com/manual/reference/command/createUser/#roles User role]]
 *
 * @param name the role name (e.g. `readWrite`)
 */
sealed class UserRole private[api] (val name: String) {
  override def equals(that: Any): Boolean = that match {
    case other: UserRole => this.name == other.name
    case _               => false
  }

  override def hashCode: Int = name.hashCode

  override def toString = s"UserRole($name)"
}

object UserRole {
  /**
   * @param name the role name (e.g. `readWrite`)
   */
  def apply(name: String): UserRole = new UserRole(name)
}

/**
 * @param name the role name
 * @param db the name of the database
 */
final class DBUserRole private[api] (
  override val name: String,
  val db: String) extends UserRole(name) {

  private[api] lazy val tupled = name -> db

  override def equals(that: Any): Boolean = that match {
    case other: DBUserRole =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"DBUserRole${tupled.toString}"
}

object DBUserRole {
  /**
   * @param name the role name
   * @param db the name of the database
   */
  def apply(name: String, db: String): DBUserRole = new DBUserRole(name, db)
}

/**
 * [[https://docs.mongodb.com/manual/reference/command/createUser/#authentication-restrictions Authentication restriction]] for user (since MongoDB 3.6)
 */
sealed trait AuthenticationRestriction {
  /** List of IP addresses and/or CIDR ranges */
  def clientSource: List[String]

  /** List of IP addresses and/or CIDR ranges */
  def serverAddress: List[String]

  override def equals(that: Any): Boolean = that match {
    case other: AuthenticationRestriction => tupled == other.tupled
    case _                                => false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String = s"""AuthenticationRestriction(clientSource = ${clientSource.mkString("[", ",", "]")}, serverAddress = ${serverAddress.mkString("[", ", ", "]")})"""

  private lazy val tupled = clientSource -> serverAddress
}

object AuthenticationRestriction {
  private[api] def apply(
    clientSource: List[String],
    serverAddress: List[String]): AuthenticationRestriction =
    new Impl(clientSource, serverAddress)

  // ---

  private final class Impl(
    val clientSource: List[String],
    val serverAddress: List[String]) extends AuthenticationRestriction
}

private[reactivemongo] trait CreateUserCommand[P <: SerializationPack with Singleton] { _: PackSupport[P] =>

  /**
   * The [[https://docs.mongodb.com/manual/reference/command/createUser/ createUser]] command.
   *
   * @param name the name of the user to be created
   * @param pwd the user password (not required if the database uses external credentials)
   * @param roles the roles granted to the user, possibly an empty to create users without roles
   * @param digestPassword when true, the mongod instance will create the hash of the user password (default: `true`)
   * @param writeConcern the optional level of [[https://docs.mongodb.com/manual/reference/write-concern/ write concern]]
   * @param customData the custom data to associate with the user account
   */
  protected final class CreateUser(
    val name: String,
    val pwd: Option[String],
    val customData: Option[pack.Document],
    val roles: List[UserRole],
    val digestPassword: Boolean,
    val writeConcern: Option[WriteConcern],
    val authenticationRestrictions: List[AuthenticationRestriction],
    val mechanisms: List[AuthenticationMode])
    extends Command with CommandWithPack[P]
    with CommandWithResult[UnitBox.type]

  protected final implicit lazy val createUserWriter: pack.Writer[CreateUser] = {
    val builder = pack.newBuilder

    import builder.{ document, elementProducer => elmt, string }

    def writeRole(role: UserRole): pack.Value = role match {
      case r: DBUserRole =>
        document(Seq(
          elmt("role", string(r.name)),
          elmt("db", string(r.db))))

      case r =>
        string(r.name)
    }

    val writeWriteConcern = CommandCodecs.writeWriteConcern[pack.type](builder)

    def writeRestriction(restriction: AuthenticationRestriction) = {
      val elmts = Seq.newBuilder[pack.ElementProducer]

      restriction.clientSource match {
        case head :: tail =>
          elmts += elmt(
            "clientSource", builder.array(string(head), tail.map(string)))

        case _ =>
          ()
      }

      restriction.serverAddress match {
        case head :: tail =>
          elmts += elmt(
            "serverAddress", builder.array(string(head), tail.map(string)))

        case _ =>
          ()
      }

      document(elmts.result())
    }

    pack.writer[CreateUser] { create =>
      val base = Seq[pack.ElementProducer](
        elmt("createUser", string(create.name)),
        elmt("digestPassword", builder.boolean(create.digestPassword)))

      val roles: Seq[pack.ElementProducer] = create.roles match {
        case head :: tail => Seq(elmt("roles", builder.array(
          writeRole(head), tail.map(writeRole(_)))))

        case _ => Seq.empty
      }

      val extra = Seq.newBuilder[pack.ElementProducer]

      create.customData.foreach { data =>
        extra += elmt("customData", data)
      }

      create.pwd.foreach { pwd =>
        extra += elmt("pwd", string(pwd))
      }

      create.writeConcern.foreach { wc =>
        extra += elmt("writeConcern", writeWriteConcern(wc))
      }

      create.authenticationRestrictions match {
        case head :: tail =>
          extra += elmt("authenticationRestrictions", builder.array(
            writeRestriction(head), tail.map(writeRestriction)))

        case _ =>
          ()
      }

      create.mechanisms match {
        case head :: tail =>
          extra += elmt("mechanisms", builder.array(
            string(head.name), tail.map { m => string(m.name) }))

        case _ => ()
      }

      document(base ++ roles ++ extra.result())
    }
  }
}
