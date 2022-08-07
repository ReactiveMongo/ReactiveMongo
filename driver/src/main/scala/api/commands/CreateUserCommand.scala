package reactivemongo.api.commands

import reactivemongo.api.{
  AuthenticationMode,
  PackSupport,
  SerializationPack,
  WriteConcern
}

import reactivemongo.core.protocol.MongoWireVersion

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
    val db: String)
    extends UserRole(name) {

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
 *
 * @param clientSource the client list of IP addresses and/or CIDR ranges
 * @param serverAddress the server list of IP addresses and/or CIDR ranges
 */
final class AuthenticationRestriction private[api] (
    val clientSource: List[String],
    val serverAddress: List[String]) {

  override def equals(that: Any): Boolean = that match {
    case other: AuthenticationRestriction => tupled == other.tupled
    case _                                => false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String =
    s"""AuthenticationRestriction(clientSource = ${clientSource.mkString(
        "[",
        ",",
        "]"
      )}, serverAddress = ${serverAddress.mkString("[", ", ", "]")})"""

  private lazy val tupled = clientSource -> serverAddress
}

object AuthenticationRestriction {

  def apply(
      clientSource: List[String],
      serverAddress: List[String]
    ): AuthenticationRestriction =
    new AuthenticationRestriction(clientSource, serverAddress)
}

private[reactivemongo] trait CreateUserCommand[P <: SerializationPack] {
  _self: PackSupport[P] =>

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
      extends Command
      with CommandWithPack[P]
      with CommandWithResult[Unit] {
    val commandKind = CommandKind.CreateUser
  }

  protected final def createUserWriter(
      version: MongoWireVersion
    ): pack.Writer[CreateUser] = {
    val builder = pack.newBuilder

    import builder.{ document, elementProducer => elmt, string }

    def writeRole(role: UserRole): pack.Value = role match {
      case r: DBUserRole =>
        document(Seq(elmt("role", string(r.name)), elmt("db", string(r.db))))

      case r =>
        string(r.name)
    }

    val writeWriteConcern = CommandCodecs.writeWriteConcern[pack.type](builder)

    @SuppressWarnings(Array("VariableShadowing"))
    def writeRestriction(restriction: AuthenticationRestriction) = {
      val elmts = Seq.newBuilder[pack.ElementProducer]

      import restriction.{ clientSource, serverAddress }

      if (clientSource.nonEmpty) {
        elmts += elmt("clientSource", builder.array(clientSource.map(string)))
      }

      if (serverAddress.nonEmpty) {
        elmts += elmt("serverAddress", builder.array(serverAddress.map(string)))
      }

      document(elmts.result())
    }

    pack.writer[CreateUser] { create =>
      val base = Seq[pack.ElementProducer](
        elmt("createUser", string(create.name)),
        elmt("digestPassword", builder.boolean(create.digestPassword))
      )

      val roles: Seq[pack.ElementProducer] = {
        if (create.roles.isEmpty) Seq.empty
        else {
          Seq(elmt("roles", builder.array(create.roles.map(writeRole(_)))))
        }
      }

      val extra = Seq.newBuilder[pack.ElementProducer]

      create.customData.foreach { data => extra += elmt("customData", data) }

      create.pwd.foreach { pwd => extra += elmt("pwd", string(pwd)) }

      create.writeConcern.foreach { wc =>
        extra += elmt("writeConcern", writeWriteConcern(wc))
      }

      if (create.authenticationRestrictions.nonEmpty) {
        extra += elmt(
          "authenticationRestrictions",
          builder.array(create.authenticationRestrictions.map(writeRestriction))
        )
      }

      if (version >= MongoWireVersion.V40 && create.mechanisms.nonEmpty) {
        extra += elmt(
          "mechanisms",
          builder.array(create.mechanisms.map { m => string(m.name) })
        )
      }

      document(base ++ roles ++ extra.result())
    }
  }
}
