package reactivemongo.bson



object Query{
  def on[T] = Queryable[T]
}


case class Queryable[T] {
  import language.experimental.macros
  
	def eq[A](p: T => A, value: A) : BSONDocument = macro QueryMacroImpl.eq[T,A]
	def gt[A](p: T => A, value: A) : BSONDocument = BSONDocument()
	def gte[A](p: T => A, value: A) : BSONDocument = BSONDocument()
	def in[A](p: T => A, values: A *) : BSONDocument = BSONDocument()
	def lt[A](p: T => A, value: A) : BSONDocument = BSONDocument()
	def lte[A](p: T => A, value: A) : BSONDocument = BSONDocument()
	def ne[A](p: T => A, values: A *) : BSONDocument = BSONDocument()
	def nin[A](p: T => A, value: A) : BSONDocument = BSONDocument()
	
	
	
	def and(exps: Queryable[T] => BSONDocument *) = BSONDocument()
	def or(exps: Queryable[T] => BSONDocument *) = BSONDocument()
	def not(expr: Queryable[T] => BSONDocument) = BSONDocument()
	def nor(expr: Queryable[T] => BSONDocument) = BSONDocument()
}

/**
 * Macros for generating `BSONReader` and `SONWriter` implementations for case
 * at compile time. Invoking these macros is equivalent to writing anonymous
 * class implementations by hand.
 *
 * Example
 * {{{
 * case class Person(name: String, surname: String)
 * implicit val personHandler = Macros.handler[Person]
 * }}}
 *
 * Use `reader` to generate the `BSONReader` and `writer` for `BSONWriter` or
 * `handler` for a class that extends both. Respective methods with 'Opts'
 * appended take additional options in form of type parameters.
 *
 * The `A` type parameter defines case class that will be the basis for
 * auto-generated implementation. Some other types with matching apply-unapply
 * might work but behaviour is undefined. Since macros will match the
 * apply-unapply pair you are free to overload these methods in the companion
 * object.
 *
 * Fields in the case class get mapped into BSON properties with respective
 * names and BSON handlers are pulled from implicit scope to (de)serialize
 * them. In order to use custom types inside case classes just make sure
 * appropriate handlers are in scope. Note that companion objects are searched
 * too. For example if you have `case class Foo(bar: Bar)` and want to create a
 * handler for it is enough to put an implicit handler for `Bar` in it's
 * companion object. That handler might be macro generated or written by hand.
 *
 * Case classes can also be defined inside other classes, objects or traits
 * but not inside functions(known limitation). In order to work you should
 * have the case class in scope(where you call the macro) so you can refer to
 * it by it's short name - without package. This is necessary because the
 * generated implementations refer to it by the short name to support
 * nested declarations. You can work around this with local imports.
 *
 * Example
 * {{{
 * implicit val handler = {
 *   import some.package.Foo
 *   Macros.handler[Foo]
 * }
 * }}}
 *
 * Option types are handled somewhat specially: a field of type Option[T]
 * will only be appended to the document if it contains a value. Similarly
 * if a document does not contain a value it will be read as None.
 *
 * Also supported neat trick are 'union types' that make for easy work with
 * algebraic data types. See the UnionType option for more details.
 *
 * You can also create recursive structures by explicitly annotating types of
 * the implicit handlers. (To let the compiler know they exist)
 * Example
 * {{{
 * sealed trait Tree
 * case class Node(left: Tree, right: Tree) extends Tree
 * case class Leaf(data: String) extends Tree
 *
 * object Tree {
 *   import Macros.Options._
 *   implicit val bson: Handler[Tree] = Macros.handlerOpts[Tree, UnionType[Node \/ Leaf]]
 * }
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
  def writerOpts[A, Opts <: Options.Default]:BSONDocumentWriter[A] = macro MacroImpl.writer[A, Opts]

  /** Creates an instance of BSONReader and BSONWriter for case class A */
  def handler[A]: BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A] = macro MacroImpl.handler[A, Options.Default]

  /**Creates an instance of BSONReader and BSONWriter for case class A and takes additional options */
  def handlerOpts[A, Opts <: Options.Default]: BSONDocumentReader[A] with BSONDocumentWriter[A]  with BSONHandler[BSONDocument, A] = macro MacroImpl.handler[A, Opts]
  
  def where[A](p: A => Boolean) : BSONDocument = BSONDocument()

  /**
   * Methods with 'Opts' postfix will take additional options in the form of
   * type parameters that will customize behavior of the macros during compilation.
   */
  object Options {

    /**
     * Default options that are implied if invoking "non-Opts" method.
     * All other options extend this.
     */
    trait Default

    /** Print out generated code during compilation. */
    trait Verbose extends Default

    /**
     * In `write` method also store class name(dynamic type) as a string
     * in a property named "className".
     */
    trait SaveClassName extends Default

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
     * If there are handlers available in implicit scope for any of the types
     * in the union they will be used to handle (de)serialization, otherwise
     * handlers for all types will be generated.
     *
     * You can also use case object but you have to refer to their types as to
     * singleton types. e.g. case object MyObject, type would be MyObject.type
     *
     * Example
     * {{{
     * sealed trait Tree
     * case class Node(left: Tree, right: Tree) extends Tree
     * case class Leaf(data: String) extends Tree
     *
     * import Macros.Options._
     * implicit val treeHandler = Macros.handlerOpts[Tree, UnionType[Node \/ Leaf]]
     * }}}
     *
     * @tparam Types to use in pattern matching. Listed in a "type list" \/
     */
    trait UnionType[Types <: \/[_, _]] extends SaveClassName with Default

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
     * implementations of the top trait automatically. For this to be possible
     * the top trait has to be sealed. If your tree is deeper(class extends trait
     * that extends top trait) all the intermediate traits have to be sealed too!
     *
     * Example
     * {{{
     * sealed trait TopTrait
     * case class OneImplementation(data: String) extends TopTrait
     * case class SecondImplementation(data: Int) extends TopTrait
     * sealed trait RefinedTrait extends TopTrait
     * case class ThirdImplementation(data: Float, name: String) extends RefinedTrait
     * case object StaticImplementation extends RefinedTrait
     *
     * val handler = Macros.handlerOpts[TopTrait, AllImplementations]
     * }}}
     */
    trait AllImplementations extends SaveClassName with Default
  }

  /**
   * Annotations to use on case classes that are being processed by macros
   */
  object Annotations {
    import scala.annotation.{StaticAnnotation, meta}

    /**
     * Specify a key different from field name in your case class.
     * Convenient to use when you'd like to leverage mongo's _id index but don't want
     * to actually use `_id` in your code.
     *
     * Example
     * {{{
     * case class Website(@Key("_id") url: String, content: Content)
     * }}}
     *
     * Generated handler will map the `url` field in your code to `_id` field in BSON
     *
     * @param key the desired key to use in BSON
     */
    @meta.param
    case class Key(key: String) extends StaticAnnotation
  }
}