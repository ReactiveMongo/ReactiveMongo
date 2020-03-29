package reactivemongo.api.commands

import reactivemongo.api.{ AuthenticationMode, SerializationPack, WriteConcern }

/**
 * [[https://docs.mongodb.com/manual/reference/command/createUser/#roles User role]]
 *
 * @param name the role name (e.g. `readWrite`)
 */
class UserRole(val name: String)

/**
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
  def apply(name: String, db: String): DBUserRole =
    new DBUserRole(name, db)

  private[api] def unapply(role: DBUserRole) = Option(role).map(_.tupled)
}

/** User role extractor */
object UserRole {
  def apply(name: String): UserRole = new UserRole(name)

  private[reactivemongo] def unapply(role: UserRole): Option[String] = Some(role.name)
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
  def apply(
    clientSource: List[String],
    serverAddress: List[String]): AuthenticationRestriction =
    new Impl(clientSource, serverAddress)

  def unapply(restriction: AuthenticationRestriction): Option[(List[String], List[String])] = Some(restriction.tupled)

  // ---

  private final class Impl(
    val clientSource: List[String],
    val serverAddress: List[String]) extends AuthenticationRestriction
}

@deprecated("Internal: will be made private", "0.16.0")
trait CreateUserCommand[P <: SerializationPack]
  extends ImplicitCommandHelpers[P] {

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
  class CreateUser(
    val name: String,
    val pwd: Option[String],
    val customData: Option[pack.Document],
    val roles: List[UserRole],
    val digestPassword: Boolean,
    val writeConcern: Option[WriteConcern],
    val authenticationRestrictions: List[AuthenticationRestriction],
    val mechanisms: List[AuthenticationMode])
    extends Command with CommandWithPack[P]
    with CommandWithResult[UnitBox.type] with Product with Serializable {

    @deprecated("Use the complete constructor", "0.18.4")
    def this(
      name: String,
      pwd: Option[String],
      roles: List[UserRole],
      digestPassword: Boolean,
      writeConcern: Option[WriteConcern],
      customData: Option[pack.Document]) = this(name, pwd, customData, roles, digestPassword, writeConcern, List.empty, List.empty)

    @deprecated("No longer a product", "0.18.4")
    def canEqual(that: Any): Boolean = that match {
      case _: CreateUser => true
      case _             => false
    }

    @deprecated("No longer a product", "0.18.4")
    val productArity: Int = 8

    @deprecated("No longer a product", "0.18.4")
    def productElement(n: Int): Any = (n: @annotation.switch) match {
      case 0 => name
      case 1 => pwd
      case 2 => customData
      case 3 => roles
      case 4 => digestPassword
      case 5 => writeConcern
      case 6 => authenticationRestrictions
      case _ => mechanisms
    }
  }

  object CreateUser extends scala.runtime.AbstractFunction6[String, Option[String], List[UserRole], Boolean, Option[WriteConcern], Option[pack.Document], CreateUser] {

    @deprecated("Use the complete constructor", "0.18.4")
    @inline def apply(
      name: String,
      pwd: Option[String],
      roles: List[UserRole],
      digestPassword: Boolean,
      writeConcern: Option[WriteConcern],
      customData: Option[pack.Document]): CreateUser = new CreateUser(name, pwd, roles, digestPassword, writeConcern, customData)
  }
}

private[reactivemongo] object CreateUserCommand {
  def writer[P <: SerializationPack with Singleton](pack: P): pack.Writer[CreateUserCommand[pack.type]#CreateUser] = {
    val builder = pack.newBuilder

    import builder.{ document, elementProducer => elmt, string }

    def writeRole(role: UserRole): pack.Value = role match {
      case DBUserRole(name, dbn) =>
        document(Seq(
          elmt("role", string(name)),
          elmt("db", string(dbn))))

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

    pack.writer[CreateUserCommand[pack.type]#CreateUser] { create =>
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
