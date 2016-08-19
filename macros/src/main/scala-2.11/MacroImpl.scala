package reactivemongo.bson

import reactivemongo.bson.Macros.Annotations.{ Ignore, Key }
import reactivemongo.bson.Macros.Options.SaveSimpleName

import scala.collection.immutable.Set
import scala.reflect.macros.whitebox.Context

private object MacroImpl {
  def reader[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentReader[A]] = c.universe.reify(BSONDocumentReader[A] {
    document: BSONDocument => Helper[A, Opts](c).readBody.splice
  })

  def writer[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentWriter[A]] = c.universe.reify(BSONDocumentWriter[A] {
    document: A => Helper[A, Opts](c).writeBody.splice
  })

  def handler[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A]] = {
    val helper = Helper[A, Opts](c)

    c.universe.reify(
      new BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A] {
        def read(document: BSONDocument): A = helper.readBody.splice
        def write(document: A): BSONDocument = helper.writeBody.splice
      })
  }

  private def Helper[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context) = new Helper[c.type, A](c) {
    val A = c.weakTypeOf[A]
    val Opts = c.weakTypeOf[Opts]
  }

  private abstract class Helper[C <: Context, A](val c: C) {
    import c.universe._

    protected def A: c.Type
    protected def Opts: c.Type

    lazy val readBody: c.Expr[A] = {
      val writer = unionTypes.map { types =>
        val cases = types map { typ =>
          val pattern = if (hasOption[SaveSimpleName])
            Literal(Constant(typ.typeSymbol.name.decodedName.toString))
          else
            Literal(Constant(typ.typeSymbol.fullName)) //todo

          val body = readBodyFromImplicit(typ)
          cq"$pattern => $body"
        }
        val className = q"""document.getAs[String]("className").get"""

        Match(className, cases)
      } getOrElse readBodyConstruct(A)

      val result = c.Expr[A](writer)

      if (hasOption[Macros.Options.Verbose]) {
        c.echo(c.enclosingPosition, show(result))
      }

      result
    }

    lazy val writeBody: c.Expr[BSONDocument] = {
      val writer = unionTypes.map { types =>
        val cases = types.map { typ =>
          val pattern = Bind(TermName("document"), Typed(Ident(termNames.WILDCARD), TypeTree(typ)))
          val body = writeBodyFromImplicit(typ)
          cq"$pattern => $body"
        }

        Match(Ident(TermName("document")), cases)
      } getOrElse writeBodyConstruct(A)

      val result = c.Expr[BSONDocument](writer)

      if (hasOption[Macros.Options.Verbose]) {
        c.echo(c.enclosingPosition, show(result))
      }

      result
    }

    private def readBodyFromImplicit(A: c.Type) = {
      val reader = c.inferImplicitValue(appliedType(readerType, List(A)))
      if (!reader.isEmpty) {
        Apply(Select(reader, TermName("read")), List(Ident(TermName("document"))))
      } else readBodyConstruct(A)
    }

    private def readBodyConstruct(implicit A: c.Type) = {
      if (isSingleton(A))
        readBodyConstructSingleton
      else
        readBodyConstructClass
    }

    private def readBodyConstructSingleton(implicit A: c.Type) = {
      val sym = A match {
        case SingleType(_, sym) => sym
        case TypeRef(_, sym, _) => sym
        case _                  => c.abort(c.enclosingPosition, s"Something weird is going on with '$A'. Should be a singleton but can't parse it")
      }
      val name = TermName(sym.name.toString) //this is ugly but quite stable compared to other attempts
      Ident(name)
    }

    private def readBodyConstructClass(implicit A: c.Type) = {
      import c.universe._

      val (constructor, _) = matchingApplyUnapply

      val tpeArgs: List[c.Type] = A match {
        case TypeRef(_, _, args) => args
        case i @ ClassInfoType(_, _, _) => i.typeArgs
      }
      val boundTypes = constructor.typeParams.zip(tpeArgs).map {
        case (sym, ty) => sym.fullName -> ty
      }.toMap

      val values = constructor.paramLists.head.map { param =>
        val t = param.typeSignature
        val sig = boundTypes.lift(t.typeSymbol.fullName).getOrElse(t)
        val optTyp = optionTypeParameter(sig)
        val typ = optTyp getOrElse sig

        if (optTyp.isDefined) {
          q"document.getAs[$typ](..${List(Literal(Constant(paramName(param))))})"
        } else {
          q"document.getAsTry[$typ](..${List(Literal(Constant(paramName(param))))}).get"
        }
      }

      q"${Ident(companion.name)}.apply(..$values)"
    }

    private def writeBodyFromImplicit(A: c.Type) = {
      val writer = c.inferImplicitValue(appliedType(writerType, List(A)))

      if (!writer.isEmpty) {
        val doc = q"""$writer.write(${Ident(TermName("document"))})"""

        classNameTree(A) map { className =>
          val nameE = c.Expr[(String, BSONString)](className)
          val docE = c.Expr[BSONDocument](doc)

          reify {
            docE.splice ++ BSONDocument(Seq(nameE.splice))
          }.tree
        } getOrElse doc
      } else writeBodyConstruct(A)
    }

    private def writeBodyConstruct(A: c.Type): c.Tree = {
      if (isSingleton(A)) writeBodyConstructSingleton(A)
      else writeBodyConstructClass(A)
    }

    private def writeBodyConstructSingleton(A: c.Type): c.Tree = (
      classNameTree(A) map { className =>
        val nameE = c.Expr[(String, BSONString)](className)
        reify { BSONDocument(Seq((nameE.splice))) }
      } getOrElse reify { BSONDocument.empty }
      ).tree

    private def writeBodyConstructClass(A: c.Type): c.Tree = {
      val (constructor, deconstructor) = matchingApplyUnapply(A)
      val types = unapplyReturnTypes(deconstructor)

      if (constructor.paramLists.size > 1) {
        c.abort(c.enclosingPosition, s"Constructor with multiple parameter lists is not supported: ${A.typeSymbol.name}${constructor.typeSignature}")
      }

      val constructorParams = constructor.paramLists.head
      val tpeArgs: List[c.Type] = A match {
        case TypeRef(_, _, args) => args
        case i @ ClassInfoType(_, _, _) => i.typeArgs
      }
      val boundTypes = constructor.typeParams.zip(tpeArgs).map {
        case (sym, ty) => sym.fullName -> ty
      }.toMap

      val tuple = Ident(TermName("tuple"))
      val (optional, required) = constructorParams.zipWithIndex.filterNot(p => ignoreField(p._1)) zip types partition (t => isOptionalType(t._2))
      val values = required map {
        case ((param, i), sig) =>
          val typ = boundTypes.lift(sig.typeSymbol.fullName).getOrElse(sig)
          val neededType = appliedType(writerType, List(typ))
          val writer = c.inferImplicitValue(neededType)

          if (writer.isEmpty) {
            c.abort(c.enclosingPosition, s"Implicit ${classOf[Writer[_]].getName}[${A.typeSymbol.name}] for '${param.name}' not found")
          }

          val tuple_i = {
            if (types.length == 1) tuple
            else Select(tuple, TermName("_" + (i + 1)))
          }
          val bs_value = c.Expr[BSONValue](q"$writer.write($tuple_i)")
          val name = c.Expr[String](q"${paramName(param)}")
          reify((name.splice, bs_value.splice): (String, BSONValue)).tree
      }

      val appends = optional map {
        case ((param, i), optType) =>
          val sig = optionTypeParameter(optType).get
          val typ = boundTypes.lift(sig.typeSymbol.fullName).getOrElse(sig)
          val neededType = appliedType(writerType, List(typ))
          val writer = c.inferImplicitValue(neededType)

          if (writer.isEmpty) {
            c.abort(c.enclosingPosition, s"Implicit ${classOf[Writer[_]].getName}[${A.typeSymbol.name}] for '${param.name}' not found")
          }

          val tuple_i = {
            if (types.length == 1) tuple
            else Select(tuple, TermName("_" + (i + 1)))
          }
          val bs_value = c.Expr[BSONValue](q"$writer.write($tuple_i.get)")
          val name = c.Expr[String](q"${paramName(param)}")

          If(
            Select(tuple_i, TermName("isDefined")),
            Apply(Select(Ident(TermName("buf")), TermName("$plus$colon$eq")), List(reify((name.splice, bs_value.splice)).tree)),
            EmptyTree)

      }

      val mkBSONdoc = Apply(bsonDocPath, values ++ classNameTree(A))

      val writer = {
        if (optional.isEmpty) List(mkBSONdoc)
        else List(
          q"val bson = $mkBSONdoc",
          q"var buf = scala.collection.immutable.Stream[(String,reactivemongo.bson.BSONValue)]()") ++ appends :+ q"bson.add(reactivemongo.bson.BSONDocument(buf))"
      }

      val unapplyTree = Select(Ident(companion(A).name), TermName("unapply"))
      val invokeUnapply = Select(Apply(unapplyTree, List(Ident(TermName("document")))), TermName("get"))
      val tupleDef = q"val tuple = $invokeUnapply"

      if (values.length + appends.length > 0) {
        val trees = tupleDef :: writer
        q"{..$trees}"
      } else writer.head
    }

    private def classNameTree(A: c.Type) = {
      if (hasOption[Macros.Options.SaveClassName]) Some {
        val name = if (hasOption[Macros.Options.SaveSimpleName])
          c.Expr[String](q"${A.typeSymbol.name.decodedName.toString}")
        else
          c.Expr[String](q"${A.typeSymbol.fullName}")

        reify(("className", BSONStringHandler.write(name.splice))).tree
      }
      else None
    }

    private lazy val unionTypes: Option[List[c.Type]] =
      parseUnionTypes orElse directKnownSubclasses

    private def parseUnionTypes: Option[List[c.Type]] = {
      val unionOption = c.typeOf[Macros.Options.UnionType[_]]
      val union = c.typeOf[Macros.Options.\/[_, _]]

      @annotation.tailrec
      def parseUnionTree(trees: List[Type], found: List[Type]): List[Type] =
        trees match {
          case tree :: rem => if (tree <:< union) {
            tree match {
              case TypeRef(_, _, List(a, b)) =>
                parseUnionTree(a :: b :: rem, found)

              case _ => c.abort(c.enclosingPosition,
                "Union type parameters expected: $tree")
            }
          } else parseUnionTree(rem, tree :: found)

          case _ => found
        }

      val tree = Opts match {
        case t @ TypeRef(_, _, lst) if t <:< unionOption =>
          lst.headOption

        case RefinedType(types, _) =>
          types.filter(_ <:< unionOption).flatMap {
            case TypeRef(_, _, args) => args
          }.headOption

        case _ => None
      }

      tree.map { t => parseUnionTree(List(t), Nil) }
    }

    private def directKnownSubclasses: Option[List[c.Type]] = {
      // Workaround for SI-7046: https://issues.scala-lang.org/browse/SI-7046
      import c.universe._

      val tpeSym = A.typeSymbol.asClass

      @annotation.tailrec
      def allSubclasses(path: Traversable[Symbol], subclasses: Set[Type]): Set[Type] = path.headOption match {
        case Some(cls: ClassSymbol) if (
          tpeSym != cls && cls.selfType.baseClasses.contains(tpeSym)
        ) => {
          val newSub: Set[Type] = if (!cls.isCaseClass) {
            c.warning(c.enclosingPosition, s"cannot handle class ${cls.fullName}: no case accessor")
            Set.empty
          } else if (!cls.typeParams.isEmpty) {
            c.warning(c.enclosingPosition, s"cannot handle class ${cls.fullName}: type parameter not supported")
            Set.empty
          } else Set(cls.selfType)

          allSubclasses(path.tail, subclasses ++ newSub)
        }

        case Some(o: ModuleSymbol) if (
          o.companion == NoSymbol && // not a companion object
            tpeSym != c && o.typeSignature.baseClasses.contains(tpeSym)
        ) => {
          val newSub: Set[Type] = if (!o.moduleClass.asClass.isCaseClass) {
            c.warning(c.enclosingPosition, s"cannot handle object ${o.fullName}: no case accessor")
            Set.empty
          } else Set(o.typeSignature)

          allSubclasses(path.tail, subclasses ++ newSub)
        }

        case Some(o: ModuleSymbol) if (
          o.companion == NoSymbol // not a companion object
        ) => allSubclasses(path.tail, subclasses)

        case Some(_) => allSubclasses(path.tail, subclasses)

        case _ => subclasses
      }

      if (tpeSym.isSealed && tpeSym.isAbstract) {
        Some(allSubclasses(tpeSym.owner.typeSignature.decls, Set.empty).toList)
      } else None
    }

    private def hasOption[O: c.TypeTag]: Boolean = Opts <:< typeOf[O]

    private def unapplyReturnTypes(deconstructor: c.universe.MethodSymbol): List[c.Type] = {
      val opt = deconstructor.returnType match {
        case TypeRef(_, _, Nil) => Some(Nil)
        case TypeRef(_, _, args) => args.head match {
          case t @ TypeRef(_, _, Nil) => Some(List(t))
          case typ @ TypeRef(_, t, args) =>
            Some(
              if (t.name.toString.matches("Tuple\\d\\d?")) args else List(typ))
          case _ => None
        }
        case _ => None
      }
      opt getOrElse c.abort(c.enclosingPosition, "something wrong with unapply type")
    }

    //Some(A) for Option[A] else None
    private def optionTypeParameter(implicit A: c.Type): Option[c.Type] = {
      if (isOptionalType(A))
        A match {
          case TypeRef(_, _, args) => args.headOption
          case _                   => None
        }
      else None
    }

    private def isOptionalType(implicit A: c.Type): Boolean =
      (c.typeOf[Option[_]].typeConstructor == A.typeConstructor)

    private def bsonDocPath: c.universe.Select = Select(Select(Ident(
      TermName("reactivemongo")), TermName("bson")), TermName("BSONDocument"))

    private def paramName(param: c.Symbol): String = {
      param.annotations.collect {
        case ann if ann.tree.tpe =:= typeOf[Key] =>
          ann.tree.children.tail.collect {
            case l: Literal => l.value.value
          }.collect {
            case value: String => value
          }
      }.flatten.headOption getOrElse param.name.toString
    }

    private def ignoreField(param: c.Symbol): Boolean =
      param.annotations.exists(ann =>
        ann.tree.tpe =:= typeOf[Ignore] || ann.tree.tpe =:= typeOf[transient])

    private def applyMethod(implicit A: c.Type): c.universe.Symbol =
      companion(A).typeSignature.decl(TermName("apply")) match {
        case NoSymbol => c.abort(c.enclosingPosition,
          s"No apply function found for $A")

        case s        => s
      }

    private def unapplyMethod(implicit A: c.Type): c.universe.MethodSymbol =
      companion(A).typeSignature.decl(TermName("unapply")) match {
        case NoSymbol => c.abort(c.enclosingPosition, s"No unapply function found for $A")
        case s        => s.asMethod
      }

    private def matchingApplyUnapply(implicit A: c.Type): (c.universe.MethodSymbol, c.universe.MethodSymbol) = {
      import c.universe._

      val applySymbol = applyMethod(A)
      val unapply = unapplyMethod(A)
      val alternatives = applySymbol.asTerm.alternatives.map(_.asMethod)
      val u = unapplyReturnTypes(unapply)

      val applys = alternatives.filter { alt =>
        alt.paramLists match {
          case params :: Nil => {
            val sig = params.map(_.typeSignature)

            sig.size == u.size && (sig, u).zipped.forall {
              case (TypeRef(NoPrefix, left, _), TypeRef(NoPrefix, right, _)) =>
                left.fullName == right.fullName

              case (left, right) => left =:= right
            }
          }

          case _ => {
            c.warning(c.enclosingPosition, s"""Constructor with multiple parameter lists is not supported: ${A.typeSymbol.name}${alt.typeSignature}""")

            false
          }
        }
      }

      val apply = applys.headOption getOrElse c.abort(c.enclosingPosition, "No matching apply/unapply found")
      (apply, unapply)
    }

    type Reader[A] = BSONReader[_ <: BSONValue, A]
    type Writer[A] = BSONWriter[A, _ <: BSONValue]

    private def isSingleton(t: Type): Boolean = t <:< typeOf[Singleton]

    private def writerType: c.Type = typeOf[Writer[_]].typeConstructor

    private def readerType: c.Type = typeOf[Reader[_]].typeConstructor

    private def companion(implicit A: c.Type): c.Symbol = A.typeSymbol.companion
  }
}
