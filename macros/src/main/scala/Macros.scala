package reactivemongo.bson

/**
 * Macros for generating `BSONReader` and `BSONWriter` implementations for case
 * at compile time. Invoking these macros is equivalent to writing anonymous
 * class implementations by hand.
 *
 * {{{
 * case class Person(name: String, surname: String)
 * implicit val personHandler = Macros.handler[Person]
 * }}}
 *
 * @see Macros.Options for specific options
 */
object Macros {
  import language.experimental.macros

  /**
   * Creates an instance of BSONReader for case class A
   * @see Macros
   */
  def reader[A]: BSONDocumentReader[A] = macro MacroImpl.reader[A, Options.Default]

  /** Creates an instance of BSONReader for case class A and takes additional options */
  def readerOpts[A, Opts <: Options.Default]: BSONDocumentReader[A] = macro MacroImpl.reader[A, Opts]

  /** Creates an instance of BSONWriter for case class A */
  def writer[A]: BSONDocumentWriter[A] = macro MacroImpl.writer[A, Options.Default]

  /** Creates an instance of BSONWriter for case class A and takes additional options */
  def writerOpts[A, Opts <: Options.Default]: BSONDocumentWriter[A] = macro MacroImpl.writer[A, Opts]

  /** Creates an instance of BSONReader and BSONWriter for case class A */
  def handler[A]: BSONDocumentHandler[A] = macro MacroImpl.handler[A, Options.Default]

  /**
   * Creates an instance of BSONReader and BSONWriter for case class A,
   * and takes additional options.
   */
  def handlerOpts[A, Opts <: Options.Default]: BSONDocumentHandler[A] = macro MacroImpl.handler[A, Opts]

  /**
   * Methods with 'Opts' postfix will take additional options in the form of
   * type parameters that will customize behaviour of
   * the macros during compilation.
   */
  object Options {

    /**
     * The default options that are implied if invoking "non-Opts" method.
     * All other options extend this.
     */
    trait Default

    /** Print out generated code during compilation. */
    trait Verbose extends Default

    /**
     * In `write` method also store class name (dynamic type) as a string
     * in a property named "className".
     */
    @deprecated(message = "Default behaviour for sealed trait, if union types are not explicitly defined", since = "0.12-RC2")
    trait SaveClassName extends Default

    /**
     * Same as [[SaveClassName]] but using the class simple name
     * (i.e. Not the fully-qualified name).
     */
    trait SaveSimpleName extends SaveClassName with Default

    /**
     * Use type parameter `A` as static type but use pattern matching to handle
     * different possible subtypes. This makes it easy to persist algebraic
     * data types(pattern where you have a sealed trait and several implementing
     * case classes). When writing a case class into BSON its dynamic type
     * will be pattern matched, when reading from BSON the pattern matching
     * will be done on the `className` string. This option extends
     * [[reactivemongo.bson.Macros.Options.SaveClassName]] in to ensure class
     * names are always serialized.
     *
     * @tparam Types to use in pattern matching. Listed in a "type list" \/
     */
    trait UnionType[Types <: \/[_, _]] extends SaveClassName with Default

    /**
     * Same as [[UnionType]] but saving the class’ simple name io. the
     * fully-qualified name.
     * @tparam Types to use in pattern matching. Listed in a "type list" \/
     */
    trait SimpleUnionType[Types <: \/[_, _]]
      extends UnionType[Types] with SaveSimpleName with Default

    /**
     * Type for making type-level lists for UnionType.
     * If second parameter is another \/ it will be flattend out into a list
     * and so on. Using infix notation makes much more sense since it then
     * looks like a logical disjunction.
     *
     * `Foo \/ Bar \/ Baz` is interpreted as type Foo or type Bar or type Baz
     */
    trait \/[A, B]

    /**
     * Similar to [[reactivemongo.bson.Macros.Options.UnionType]] but finds all
     * implementations of the top trait automatically.
     */
    @deprecated(message = "Default behaviour for sealed trait, if union types are not explicitly defined", since = "0.12-RC2")
    trait AllImplementations extends SaveClassName with Default

    /**
     * Same as [[AllImplementations]] but saving the simple name
     * (e.g. the fully-qualified name).
     */
    @deprecated(message = "Default behaviour for sealed trait, if union types are not explicitly defined", since = "0.12-RC2")
    trait SimpleAllImplementations
      extends AllImplementations with SaveSimpleName with Default

    /**
     * For a sealed family (all implementations of a sealed trait
     * or defined explicit union types), this option enables the automatic
     * materialization of handlers for the member types.
     *
     * If used, make sure it cannot lead to type recursion issue
     * (disabled by default).
     */
    trait AutomaticMaterialization extends Default
  }

  /** Annotations to use on case classes that are being processed by macros. */
  object Annotations {
    import scala.annotation.{ StaticAnnotation, meta }

    /**
     * Specify a key different from field name in your case class.
     * Convenient to use when you'd like to leverage mongo's `_id` index
     * but don't want to actually use `_id` in your code.
     *
     * {{{
     * case class Website(@Key("_id") url: String)
     * }}}
     *
     * Generated handler will map the `url` field in your code
     * to `_id` field in BSON
     *
     * @param key the desired key to use in BSON
     */
    @meta.param
    case class Key(key: String) extends StaticAnnotation

    /** Ignores a field */
    @meta.param
    case class Ignore() extends StaticAnnotation

    /**
     * Indicates that if a property is represented as a document itself,
     * the document fields are directly included in top document,
     * rather than nesting it.
     *
     * {{{
     * case class Range(start: Int, end: Int)
     *
     * case class LabelledRange(
     *   name: String,
     *   @Flatten range: Range)
     *
     * // Flattened
     * BSONDocument("name" -> "foo", "start" -> 0, "end" -> 1)
     *
     * // Rather than:
     * // BSONDocument("name" -> "foo", "range" -> BSONDocument(
     * //   "start" -> 0, "end" -> 1))
     * }}}
     */
    @meta.param
    case class Flatten() extends StaticAnnotation
  }

  /** Only for internal purposes */
  final class Placeholder private () {}

  /** Only for internal purposes */
  object Placeholder {
    private val instance = new Placeholder()

    implicit object Handler extends BSONHandler[BSONDocument, Placeholder] {
      def read(bson: BSONDocument) = instance
      def write(pl: Placeholder) = BSONDocument.empty
    }
  }
}
