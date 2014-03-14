package reactivemongo.bson

import collection.mutable.ListBuffer
import reactivemongo.bson.Macros.Annotations.Key
import scala.reflect.macros.Context

/**
 * User: andraz
 * Date: 2/21/13
 * Time: 6:51 PM
 */
private object MacroImpl {
  def reader[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentReader[A]] = {
    val h = Helper[A,Opts](c)
    val body = h.readBody
    c.universe.reify {
      new BSONDocumentReader[A] {
        def read(document: BSONDocument): A = body.splice
      }
    }
  }

  def writer[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentWriter[A]] = {
    val h = Helper[A,Opts](c)
    val body = h.writeBody
    c.universe.reify (
      new BSONDocumentWriter[A]{
        def write(document: A): BSONDocument = body.splice
      }
    )
  }

  def handler[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A]] = {
    val h = Helper[A,Opts](c)
    val r = h.readBody
    val w = h.writeBody
    c.universe.reify(
      new BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A] {
        def read(document: BSONDocument): A = r.splice

        def write(document: A): BSONDocument = w.splice
      }
    )
  }

  private def Helper[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context) = new Helper[c.type, A](c) {
    val A = c.weakTypeOf[A]
    val Opts = c.weakTypeOf[Opts]
  }

  private abstract class Helper[C <: Context, A](val c: C) {
    protected def A: c.Type
    protected def Opts: c.Type
    import c.universe._

    lazy val readBody: c.Expr[A] = {
      val writer = unionTypes map { types =>
        val cases = types map { typ =>
          val pattern = Literal(Constant(typ.typeSymbol.fullName)) //todo
        val body = readBodyFromImplicit(typ)
          CaseDef(pattern, body)
        }
        val className = c.parse("""document.getAs[String]("className").get""")
        Match(className, cases)
      } getOrElse readBodyConstruct(A)

      val result = c.Expr[A](writer)

      if(hasOption[Macros.Options.Verbose]){
        c.echo(c.enclosingPosition, show(result))
      }
      result
    }

    lazy val writeBody: c.Expr[BSONDocument] = {
      val writer = unionTypes map { types =>
        val cases = types map { typ =>
          val pattern = Bind(newTermName("document"), Typed(Ident(nme.WILDCARD), TypeTree(typ)))
          val body = writeBodyFromImplicit(typ)
          CaseDef(pattern, body)
        }
        Match(Ident("document"), cases)
      } getOrElse writeBodyConstruct(A)

      val result = c.Expr[BSONDocument](writer)

      if(hasOption[Macros.Options.Verbose]){
        c.echo(c.enclosingPosition, show(result))
      }
      result
    }

    private def readBodyFromImplicit(A: c.Type) = {
      val reader = c.inferImplicitValue(appliedType(readerType, List(A)))
      if(! reader.isEmpty)
        Apply(Select(reader, "read"), List(Ident("document")))
      else
        readBodyConstruct(A)
    }

    private def readBodyConstruct(implicit A: c.Type) = {
      if(isSingleton(A))
        readBodyConstructSingleton
      else
        readBodyConstructClass
    }

    private def readBodyConstructSingleton(implicit A: c.Type) = {
      val sym = A match {
        case SingleType(_, sym) => sym
        case TypeRef(_, sym, _) => sym
        case _ => c.abort(c.enclosingPosition, s"Something weird is going on with '$A'. Should be a singleton but can't parse it")
      }
      val name = stringToTermName(sym.name.toString) //this is ugly but quite stable compared to other attempts
      Ident(name)
    }

    private def readBodyConstructClass(implicit A: c.Type) = {
      val (constructor, _) = matchingApplyUnapply

      val values = constructor.paramss.head map {
        param =>
          val sig = param.typeSignature
          val optTyp = optionTypeParameter(sig)
          val typ = optTyp getOrElse sig

          val getter = Apply(
            TypeApply(
              Select(Ident("document"), "getAsTry"),
              List(TypeTree(typ))

            ),
            List(Literal(Constant(paramName(param))))
          )

          if (optTyp.isDefined)
            Select(getter, "toOption")
          else
            Select(getter, "get")
      }

      val constructorTree = Select(Ident(companion.name.toString), "apply")
      Apply(constructorTree, values)
    }

    private def writeBodyFromImplicit(A: c.Type) = {
      val writer = c.inferImplicitValue(appliedType(writerType, List(A)))
      if(! writer.isEmpty) {
        val doc = Apply(Select(writer, "write"), List(Ident("document")))
        classNameTree(A) map { className =>
          val nameE = c.Expr[(String, BSONString)](className)
          val docE = c.Expr[BSONDocument](doc)
          reify{
            docE.splice ++ BSONDocument(Seq((nameE.splice)))
          }.tree
        } getOrElse doc
      } else
        writeBodyConstruct(A)
    }

    private def writeBodyConstruct(A: c.Type): c.Tree = {
      if(isSingleton(A))
        writeBodyConstructSingleton(A)
      else
        writeBodyConstructClass(A)
    }

    private def writeBodyConstructSingleton(A: c.Type): c.Tree = {
      val expr =classNameTree(A) map { className =>
        val nameE = c.Expr[(String, BSONString)](className)
        reify{ BSONDocument(Seq((nameE.splice))) }
      } getOrElse reify{ BSONDocument.empty }
      expr.tree
    }

    private def writeBodyConstructClass(A: c.Type): c.Tree = {
      val (constructor, deconstructor) = matchingApplyUnapply(A)
      val types = unapplyReturnTypes(deconstructor)
      val constructorParams = constructor.paramss.head

      val tuple = Ident(newTermName("tuple"))
      val (optional, required) = constructorParams.zipWithIndex zip types partition (t => isOptionalType(t._2))
      val values = required map {
        case ((param, i), typ) => {
          val neededType = appliedType(writerType, List(typ))
          val writer = c.inferImplicitValue(neededType)
          if (writer.isEmpty) c.abort(c.enclosingPosition, s"Implicit $typ for '$param' not found")
          val tuple_i = if (types.length == 1) tuple else Select(tuple, "_" + (i + 1))
          val bs_value = c.Expr[BSONValue](Apply(Select(writer, "write"), List(tuple_i)))
          val name = c.literal(paramName(param))
          reify(
            (name.splice, bs_value.splice): (String, BSONValue)
          ).tree
        }
      }

      val appends = optional map {
        case ((param, i), optType) => {
          val typ = optionTypeParameter(optType).get
          val neededType = appliedType(writerType, List(typ))
          val writer = c.inferImplicitValue(neededType)
          if (writer.isEmpty) c.abort(c.enclosingPosition, s"Implicit $typ for '$param' not found")
          val tuple_i= if (types.length == 1) tuple else Select(tuple, "_" + (i + 1))
          val buf = Ident("buf")
          val bs_value = c.Expr[BSONValue](Apply(Select(writer, "write"), List(Select(tuple_i, "get"))))
          val name = c.literal(paramName(param))
          If(
            Select(tuple_i, "isDefined"),
            Apply(Select(Ident("buf"), "$plus$colon$eq"), List(reify((name.splice,bs_value.splice)).tree)),
            EmptyTree
          )
        }
      }

      val mkBSONdoc = Apply(bsonDocPath, values ++ classNameTree(A))

      val withAppends = List(
        ValDef(Modifiers(), newTermName("bson"), TypeTree(), mkBSONdoc),
        c.parse("var buf = scala.collection.immutable.Stream[(String,reactivemongo.bson.BSONValue)]()")
      ) ++ appends :+ c.parse("bson.add(reactivemongo.bson.BSONDocument(buf))")

      val writer = if(optional.length == 0) List(mkBSONdoc) else withAppends

      val unapplyTree = Select(Ident(companion(A).name.toString), "unapply")
      val document = Ident(newTermName("document"))
      val invokeUnapply = Select(Apply(unapplyTree, List(document)), "get")
      val tupleDef = ValDef(Modifiers(), newTermName("tuple"), TypeTree(), invokeUnapply)

      if(values.length + appends.length > 0){
        Block( (tupleDef :: writer): _*)
      } else {
        writer.head
      }
    }

    private def classNameTree(A: c.Type) = {
      val className = if (hasOption[Macros.Options.SaveClassName]) Some {
        val name = c.literal(A.typeSymbol.fullName)
        reify {
          ("className", BSONStringHandler.write(name.splice))
        }.tree
      } else None
      className
    }

    private lazy val unionTypes: Option[List[c.Type]] = {
      parseUnionTypes orElse parseAllImplementationTypes
    }

    private def parseUnionTypes: Option[List[c.Type]] = {
      val unionOption = c.typeOf[Macros.Options.UnionType[_]]
      val union = c.typeOf[Macros.Options.\/[_,_]]
      def parseUnionTree(tree: Type): List[Type] = {
        if(tree <:< union) {
          tree match {
            case TypeRef(_,_, List(a,b)) => parseUnionTree(a) ::: parseUnionTree(b)
          }
        } else List(tree)
      }

      val tree = Opts match {
        case t @ TypeRef(_, _, lst) if t <:< unionOption =>
          lst.headOption

        case RefinedType(types, _) =>
          types.filter(_ <:< unionOption).flatMap{ case TypeRef(_,_,args) => args }.headOption

        case _ => None
      }
      tree map {t => parseUnionTree(t)}
    }

    private def parseAllImplementationTypes: Option[List[Type]] = {
      val allOption = typeOf[Macros.Options.AllImplementations]
      if(Opts <:< allOption){
        Some(allImplementations(A).toList)
      } else{
        None
      }
    }

    private def hasOption[O: c.TypeTag]: Boolean = Opts <:< typeOf[O]

    private def unapplyReturnTypes(deconstructor: c.universe.MethodSymbol): List[c.Type] = {
      val opt = deconstructor.returnType match {
        case TypeRef(_, _, Nil) => Some(Nil)
        case TypeRef(_, _, args) =>
          args.head match {
            case t@TypeRef(_, _, Nil) => Some(List(t))
            case typ@TypeRef(_, t, args) =>
              Some(
                if (t.name.toString.matches("Tuple\\d\\d?")) args else List(typ)
              )
            case _ => None
          }
        case _ => None
      }
      opt getOrElse c.abort(c.enclosingPosition, "something wrong with unapply type")
    }

    //Some(A) for Option[A] else None
    private def optionTypeParameter(implicit A: c.Type): Option[c.Type] = {
      if(isOptionalType(A))
        A match {
          case TypeRef(_, _, args) => args.headOption
          case _ => None
        }
      else None
    }

    private def isOptionalType(implicit A: c.Type): Boolean = {
      c.typeOf[Option[_]].typeConstructor == A.typeConstructor
    }

    private def bsonDocPath: c.universe.Select = {
      Select(Select(Ident(newTermName("reactivemongo")), "bson"), "BSONDocument")
    }

    private def paramName(param: c.Symbol): String = {
      param.annotations.collect{
        case ann if ann.tpe =:= typeOf[Key] =>
          ann.scalaArgs.collect{
            case l: Literal => l.value.value
          }.collect{
            case value: String => value
          }
        case other =>
          c.abort(c.enclosingPosition, other.tpe + " " + other.scalaArgs)
      }.flatten.headOption getOrElse param.name.toString
    }

    private def allSubclasses(A: Symbol): Set[Symbol] = {
      val sub = A.asClass.knownDirectSubclasses
      val subsub = sub flatMap allSubclasses
      subsub ++ sub + A
    }

    private def allImplementations(A: Type) = {
      val classes = allSubclasses(A.typeSymbol) map (_.asClass)
      classes filterNot (_.isAbstractClass) map (_.toType)
    }

    private def applyMethod(implicit A: c.Type): c.universe.Symbol = {
      companion(A).typeSignature.declaration(stringToTermName("apply")) match {
        case NoSymbol => c.abort(c.enclosingPosition, s"No apply function found for $A")
        case s => s
      }
    }

    private def unapplyMethod(implicit A: c.Type): c.universe.MethodSymbol= {
      companion(A).typeSignature.declaration(stringToTermName("unapply")) match {
        case NoSymbol => c.abort(c.enclosingPosition, s"No unapply function found for $A")
        case s => s.asMethod
      }
    }

    private def matchingApplyUnapply(implicit A: c.Type): (c.universe.MethodSymbol, c.universe.MethodSymbol) = {
      val applySymbol = applyMethod(A)
      val unapply = unapplyMethod(A)

      val alternatives = applySymbol.asTerm.alternatives map (_.asMethod)
      val u = unapplyReturnTypes(unapply)
      val applys = alternatives filter { alt =>
        val sig = alt.paramss.head.map(_.typeSignature)
        sig.size == u.size && sig.zip(u).forall {
          case (left, right) => left =:= right
        }
      }

      val apply = applys.headOption getOrElse c.abort(c.enclosingPosition, "No matching apply/unapply found")
      (apply,unapply)
    }

    type Reader[A] = BSONReader[_ <: BSONValue, A]
    type Writer[A] = BSONWriter[A, _ <: BSONValue]

    private def isSingleton(t: Type): Boolean = t <:< typeOf[Singleton]
    private def writerType: c.Type = typeOf[Writer[_]].typeConstructor
    private def readerType: c.Type = typeOf[Reader[_]].typeConstructor

    private def companion(implicit A: c.Type): c.Symbol = A.typeSymbol.companionSymbol
  }
}