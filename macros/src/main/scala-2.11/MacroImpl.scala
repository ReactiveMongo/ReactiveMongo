package reactivemongo.bson

import reactivemongo.bson.Macros.Annotations.{ Ignore, Key }
import reactivemongo.bson.Macros.Options.SaveSimpleName

import scala.collection.immutable.Set
import scala.reflect.macros.whitebox.Context

private object MacroImpl {
  def reader[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentReader[A]] = c.universe.reify(new BSONDocumentReader[A] {
    private val r: BSONDocument => A = { document =>
      Helper[A, Opts](c).readBody.splice
    }

    lazy val forwardReader: BSONDocumentReader[A] = BSONDocumentReader[A](r)

    def read(document: BSONDocument) = forwardReader.read(document)
  })

  def writer[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentWriter[A]] = c.universe.reify(new BSONDocumentWriter[A] {
    private val w: A => BSONDocument = { v =>
      Helper[A, Opts](c).writeBody.splice
    }

    lazy val forwardWriter: BSONDocumentWriter[A] = BSONDocumentWriter[A](w)

    def write(v: A) = forwardWriter.write(v)
  })

  def handler[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentHandler[A]] = {
    val helper = Helper[A, Opts](c)

    c.universe.reify(new BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A] {
      private val r: BSONDocument => A = { document =>
        Helper[A, Opts](c).readBody.splice
      }

      lazy val forwardReader: BSONDocumentReader[A] = BSONDocumentReader[A](r)

      private val w: A => BSONDocument = { v =>
        Helper[A, Opts](c).writeBody.splice
      }

      lazy val forwardWriter: BSONDocumentWriter[A] = BSONDocumentWriter[A](w)

      def read(document: BSONDocument): A = forwardReader.read(document)
      def write(v: A): BSONDocument = forwardWriter.write(v)
    })
  }

  private def Helper[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context) =
    new Helper[c.type, A](c) {
      val A = c.weakTypeOf[A]
      val Opts = c.weakTypeOf[Opts]
    }

  private abstract class Helper[C <: Context, A](
    val c: C) extends ImplicitResolver[C] {
    import c.universe._

    protected def A: Type
    protected def Opts: Type

    private val writerType: Type = typeOf[Writer[_]].typeConstructor
    private val readerType: Type = typeOf[Reader[_]].typeConstructor

    lazy val readBody: c.Expr[A] = {
      val reader = unionTypes.map { types =>
        val resolve = resolver(Map.empty, "Reader")(readerType)
        val cases = types map { typ =>
          val pattern = if (hasOption[SaveSimpleName])
            Literal(Constant(typ.typeSymbol.name.decodedName.toString))
          else
            Literal(Constant(typ.typeSymbol.fullName)) //todo

          val body = readBodyFromImplicit(typ)(resolve).getOrElse {
            // No existing implicit, but can fallback to automatic mat
            readBodyConstruct(typ)
          }

          cq"$pattern => $body"
        }
        def className = q"""document.getAs[String]("className").get"""

        Match(className, cases)
      } getOrElse readBodyConstruct(A)

      val result = c.Expr[A](reader)

      if (hasOption[Macros.Options.Verbose]) {
        c.echo(c.enclosingPosition, show(reader))
      }

      result
    }

    lazy val writeBody: c.Expr[BSONDocument] = {
      val writer = unionTypes.map { types =>
        val resolve = resolver(Map.empty, "Writer")(writerType)
        val cases = types.map { typ =>
          val nme = TermName(c.freshName("v"))
          val id = Ident(nme)
          val body = writeBodyFromImplicit(id, typ)(resolve).getOrElse {
            // No existing implicit, but can fallback to automatic mat
            writeBodyConstruct(id, typ)
          }

          cq"$nme: $typ => $body"
        }

        Match(Ident(TermName("v")), cases)
      } getOrElse writeBodyConstruct(Ident(TermName("v")), A)

      val result = c.Expr[BSONDocument](writer)

      if (hasOption[Macros.Options.Verbose]) {
        c.echo(c.enclosingPosition, show(writer))
      }

      result
    }

    // For member of a union
    private def readBodyFromImplicit(tpe: Type)(r: Type => Implicit): Option[Tree] = {
      val (reader, _) = r(tpe)

      if (!reader.isEmpty) Some(q"$reader.read(document)")
      else if (!hasOption[Macros.Options.AutomaticMaterialization]) {
        c.abort(c.enclosingPosition, s"Implicit not found for '${tpe.typeSymbol.name}': ${classOf[Reader[_]].getName}[_, ${tpe.typeSymbol.fullName}]")
      } else None
    }

    @inline private def readBodyConstruct(implicit tpe: Type) =
      if (isSingleton(tpe)) readBodyConstructSingleton
      else readBodyConstructClass

    private def readBodyConstructSingleton(implicit tpe: Type) = {
      val sym = tpe match {
        case SingleType(_, sym) => sym
        case TypeRef(_, sym, _) => sym
        case _                  => c.abort(c.enclosingPosition, s"Something weird is going on with '$tpe'. Should be a singleton but can't parse it")
      }
      val name = TermName(sym.name.toString) //this is ugly but quite stable compared to other attempts
      Ident(name)
    }

    private def readBodyConstructClass(implicit tpe: Type) = {
      val (constructor, _) = matchingApplyUnapply(tpe).getOrElse(
        c.abort(c.enclosingPosition, s"No matching apply/unapply found: $tpe"))

      val tpeArgs: List[c.Type] = tpe match {
        case TypeRef(_, _, args)        => args
        case i @ ClassInfoType(_, _, _) => i.typeArgs
      }
      val boundTypes = constructor.typeParams.zip(tpeArgs).map {
        case (sym, ty) => sym.fullName -> ty
      }.toMap
      val resolve = resolver(boundTypes, "Reader")(readerType)

      val values = constructor.paramLists.head.map { param =>
        val t = param.typeSignature
        val sig = boundTypes.getOrElse(t.typeSymbol.fullName, t)
        val opt = optionTypeParameter(sig)
        //val typ = opt getOrElse sig
        val (reader, _) = resolve(sig)
        val pname = paramName(param)

        if (reader.isEmpty) {
          c.abort(c.enclosingPosition, s"Implicit not found for '$pname': ${classOf[Reader[_]].getName}[_, $sig]")
        }

        opt match {
          case Some(_) => q"document.getAs($pname)($reader)"
          case _       => q"document.getAsTry($pname)($reader).get"
        }
      }

      q"${Ident(companion.name)}.apply(..$values)"
    }

    private def writeBodyFromImplicit(id: Ident, tpe: Type)(r: Type => Implicit): Option[Tree] = {
      val (writer, _) = r(tpe)

      if (!writer.isEmpty) {
        @inline def doc = q"$writer.write($id)"

        Some(classNameTree(tpe).fold(doc) { nameE =>
          val docE = c.Expr[BSONDocument](doc)

          reify {
            docE.splice ++ BSONDocument(Seq(nameE.splice))
          }.tree
        })
      } else if (!hasOption[Macros.Options.AutomaticMaterialization]) {
        c.abort(c.enclosingPosition, s"Implicit not found for '${tpe.typeSymbol.name}': ${classOf[Writer[_]].getName}[_, ${tpe.typeSymbol.fullName}]")

      } else None
    }

    @inline private def writeBodyConstruct(id: Ident, tpe: Type): Tree =
      if (isSingleton(tpe)) writeBodyConstructSingleton(tpe)
      else writeBodyConstructClass(id, tpe)

    private def writeBodyConstructSingleton(tpe: Type): Tree = (
      classNameTree(tpe) map { nameE =>
        reify { BSONDocument(Seq((nameE.splice))) }
      } getOrElse reify { BSONDocument.empty }).tree

    private def writeBodyConstructClass(id: Ident, tpe: Type): Tree = {
      val (constructor, deconstructor) = matchingApplyUnapply(tpe).getOrElse(
        c.abort(c.enclosingPosition, s"No matching apply/unapply found: $tpe"))

      val types = unapplyReturnTypes(deconstructor)
      val constructorParams = constructor.paramLists.head
      val tpeArgs: List[c.Type] = tpe match {
        case TypeRef(_, _, args)        => args
        case i @ ClassInfoType(_, _, _) => i.typeArgs
      }
      val boundTypes = constructor.typeParams.zip(tpeArgs).map {
        case (sym, ty) => sym.fullName -> ty
      }.toMap
      val resolve = resolver(boundTypes, "Writer")(writerType)
      val tuple = Ident(TermName("tuple"))

      val (optional, required) =
        constructorParams.zipWithIndex.zip(types).filterNot {
          case ((sym, _), _) => ignoreField(sym)
        }.partition(t => isOptionalType(t._2))

      val values = required map {
        case ((param, i), sig) =>
          val (writer, _) = resolve(sig)
          val pname = paramName(param)

          if (writer.isEmpty) {
            c.abort(c.enclosingPosition, s"Implicit not found for '$pname': ${classOf[Writer[_]].getName}[${sig.typeSymbol.name}, _]")
          }

          val tuple_i = {
            if (types.length == 1) tuple
            else Select(tuple, TermName("_" + (i + 1)))
          }
          val bs_value = c.Expr[BSONValue](q"$writer.write(${tuple_i})")
          val name = c.Expr[String](q"$pname")
          reify((name.splice, bs_value.splice): (String, BSONValue)).tree
      }

      val appends = optional map {
        case ((param, i), optType) =>
          val sig = optionTypeParameter(optType).get
          val (writer, _) = resolve(sig)
          val pname = paramName(param)

          if (writer.isEmpty) {
            c.abort(c.enclosingPosition, s"Implicit not found for '$pname': ${classOf[Writer[_]].getName}[${sig.typeSymbol.name}, _]")
          }

          val tuple_i = {
            if (types.length == 1) tuple
            else Select(tuple, TermName("_" + (i + 1)))
          }
          val bs_value = c.Expr[BSONValue](q"$writer.write($tuple_i.get)")
          val name = c.Expr[String](q"$pname")

          If(
            Select(tuple_i, TermName("isDefined")),
            Apply(Select(Ident(TermName("buf")), TermName("$plus$colon$eq")), List(reify((name.splice, bs_value.splice)).tree)),
            EmptyTree)

      }

      val mkBSONdoc = Apply(
        bsonDocPath, values ++ classNameTree(tpe).map(_.tree))

      val writer = {
        if (optional.isEmpty) List(mkBSONdoc)
        else List(
          q"val bson = $mkBSONdoc",
          q"var buf = scala.collection.immutable.Stream[(String,reactivemongo.bson.BSONValue)]()") ++ appends :+ q"bson.merge(reactivemongo.bson.BSONDocument(buf))"
      }

      val unapplyTree = Select(Ident(companion(tpe).name), TermName("unapply"))
      val invokeUnapply = Select(Apply(unapplyTree, List(id)), TermName("get"))
      val tupleDef = q"val tuple = $invokeUnapply"

      if (values.length + appends.length > 0) {
        val trees = tupleDef :: writer
        q"{..$trees}"
      } else writer.head
    }

    private def classNameTree(tpe: c.Type): Option[c.Expr[(String, BSONString)]] = {
      val tpeSym = A.typeSymbol.asClass

      if (hasOption[Macros.Options.SaveClassName] ||
        tpeSym.isSealed && tpeSym.isAbstract) Some {
        val name = if (hasOption[Macros.Options.SaveSimpleName]) {
          c.Expr[String](q"${tpe.typeSymbol.name.decodedName.toString}")
        } else c.Expr[String](q"${tpe.typeSymbol.fullName}")

        reify(("className", BSONStringHandler.write(name.splice)))
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

              case _ => c.abort(
                c.enclosingPosition,
                s"Union type parameters expected: $tree")
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

    private def directKnownSubclasses: Option[List[Type]] = {
      // Workaround for SI-7046: https://issues.scala-lang.org/browse/SI-7046
      val tpeSym = A.typeSymbol.asClass

      @annotation.tailrec
      def allSubclasses(path: Traversable[Symbol], subclasses: Set[Type]): Set[Type] = path.headOption match {
        case Some(cls: ClassSymbol) if (
          tpeSym != cls && !cls.isAbstract &&
          cls.selfType.baseClasses.contains(tpeSym)) => {
          val newSub: Set[Type] = if ({
            val tpe = cls.typeSignature
            !applyMethod(tpe).isDefined || !unapplyMethod(tpe).isDefined
          }) {
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
          tpeSym != c && o.typeSignature.baseClasses.contains(tpeSym)) => {
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

        case _       => subclasses
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

    private def ignoreField(param: Symbol): Boolean =
      param.annotations.exists(ann =>
        ann.tree.tpe =:= typeOf[Ignore] || ann.tree.tpe =:= typeOf[transient])

    private def applyMethod(implicit tpe: Type): Option[Symbol] =
      companion(tpe).typeSignature.decl(TermName("apply")) match {
        case NoSymbol => {
          if (hasOption[Macros.Options.Verbose]) {
            c.echo(c.enclosingPosition, s"No apply function found for $tpe")
          }

          None
        }

        case s => Some(s)
      }

    private def unapplyMethod(implicit tpe: Type): Option[MethodSymbol] =
      companion(tpe).typeSignature.decl(TermName("unapply")) match {
        case NoSymbol => {
          if (hasOption[Macros.Options.Verbose]) {
            c.echo(c.enclosingPosition, s"No unapply function found for $tpe")
          }

          None
        }

        case s => Some(s.asMethod)
      }

    /* Deep check for type compatibility */
    @annotation.tailrec
    private def conforms(types: Seq[(Type, Type)]): Boolean =
      types.headOption match {
        case Some((TypeRef(NoPrefix, a, _),
          TypeRef(NoPrefix, b, _))) => { // for generic parameter
          if (a.fullName != b.fullName) {
            c.warning(
              c.enclosingPosition,
              s"Type symbols are not compatible: $a != $b")

            false
          } else conforms(types.tail)
        }

        case Some((a, b)) if (a.typeArgs.size != b.typeArgs.size) => {
          c.warning(
            c.enclosingPosition,
            s"Type parameters are not matching: $a != $b")

          false
        }

        case Some((a, b)) if a.typeArgs.isEmpty =>
          if (a =:= b) conforms(types.tail) else {
            c.warning(
              c.enclosingPosition,
              s"Types are not compatible: $a != $b")

            false
          }

        case Some((a, b)) if (a.baseClasses != b.baseClasses) => {
          c.warning(
            c.enclosingPosition,
            s"Generic types are not compatible: $a != $b")

          false
        }

        case Some((a, b)) =>
          conforms((a.typeArgs, b.typeArgs).zipped ++: types.tail)

        case _ => true
      }

    /**
     * @return (apply symbol, unapply symbol)
     */
    private def matchingApplyUnapply(implicit tpe: Type): Option[(MethodSymbol, MethodSymbol)] = for {
      applySymbol <- applyMethod(tpe)
      unapply <- unapplyMethod(tpe)
      alternatives = applySymbol.asTerm.alternatives.map(_.asMethod)
      u = unapplyReturnTypes(unapply)

      apply <- alternatives.filter { alt =>
        alt.paramLists match {
          case params :: ps if (ps.isEmpty || ps.headOption.flatMap(
            _.headOption).exists(_.isImplicit)) => if (params.size != u.size) false else {
            conforms((params.map(_.typeSignature), u).zipped.toSeq)
          }

          case _ => {
            c.warning(c.enclosingPosition, s"""Constructor with multiple parameter lists is not supported: ${tpe.typeSymbol.name}${alt.typeSignature}""")

            false
          }
        }
      }.headOption
    } yield apply -> unapply

    type Reader[T] = BSONReader[_ <: BSONValue, T]
    type Writer[T] = BSONWriter[T, _ <: BSONValue]

    private def isSingleton(tpe: Type): Boolean = tpe <:< typeOf[Singleton]

    @inline private def companion(implicit tpe: Type): Symbol = tpe.typeSymbol.companion
  }

  sealed trait ImplicitResolver[C <: Context] {
    val c: C

    import c.universe._

    protected def A: Type

    import Macros.Placeholder

    // The placeholder type
    private val PlaceholderType: Type = typeOf[Placeholder]

    /* Refactor the input types, by replacing any type matching the `filter`,
     * by the given `replacement`.
     */
    @annotation.tailrec
    private def refactor(boundTypes: Map[String, Type])(in: List[Type], base: TypeSymbol, out: List[Type], tail: List[(List[Type], TypeSymbol, List[Type])], filter: Type => Boolean, replacement: Type, altered: Boolean): (Type, Boolean) = in match {
      case tpe :: ts =>
        boundTypes.getOrElse(tpe.typeSymbol.fullName, tpe) match {
          case t if (filter(t)) =>
            refactor(boundTypes)(ts, base, (replacement :: out), tail,
              filter, replacement, true)

          case TypeRef(_, sym, as) if as.nonEmpty =>
            refactor(boundTypes)(
              as, sym.asType, List.empty, (ts, base, out) :: tail,
              filter, replacement, altered)

          case t => refactor(boundTypes)(
            ts, base, (t :: out), tail, filter, replacement, altered)
        }

      case _ => {
        val tpe = appliedType(base, out.reverse)

        tail match {
          case (x, y, more) :: ts => refactor(boundTypes)(
            x, y, (tpe :: more), ts, filter, replacement, altered)

          case _ => tpe -> altered
        }
      }
    }

    /**
     * Replaces any reference to the type itself by the Placeholder type.
     * @return the normalized type + whether any self reference has been found
     */
    private def normalized(boundTypes: Map[String, Type])(tpe: Type): (Type, Boolean) = boundTypes.getOrElse(tpe.typeSymbol.fullName, tpe) match {
      case t if (t =:= A) => PlaceholderType -> true

      case TypeRef(_, sym, args) if args.nonEmpty =>
        refactor(boundTypes)(args, sym.asType, List.empty, List.empty,
          _ =:= A, PlaceholderType, false)

      case t => t -> false
    }

    /* Restores reference to the type itself when Placeholder is found. */
    private def denormalized(boundTypes: Map[String, Type])(ptype: Type): Type =
      ptype match {
        case PlaceholderType => A

        case TypeRef(_, sym, args) if args.nonEmpty =>
          refactor(boundTypes)(args, sym.asType, List.empty, List.empty,
            _ == PlaceholderType, A, false)._1

        case _ => ptype
      }

    private class ImplicitTransformer(
      boundTypes: Map[String, Type],
      forwardSuffix: String) extends Transformer {
      private val denorm = denormalized(boundTypes) _
      val forwardName = TermName(s"forward$forwardSuffix")

      override def transform(tree: Tree): Tree = tree match {
        case tt: TypeTree =>
          super.transform(TypeTree(denorm(tt.tpe)))

        case Select(Select(This(TypeName("Macros")), t), sym) if (
          t.toString == "Placeholder" && sym.toString == "Handler") => super.transform(q"$forwardName")

        case _ => super.transform(tree)
      }
    }

    private def createImplicit(boundTypes: Map[String, Type])(tc: Type, ptype: Type, tx: Transformer): Implicit = {
      val tpe = ptype match {
        case TypeRef(_, _, targ :: _) => {
          // Option[_] needs special treatment because we need to use XXXOpt
          if (ptype.typeConstructor <:< typeOf[Option[_]].typeConstructor) targ
          else ptype
        }

        case SingleType(_, _) | TypeRef(_, _, _) => ptype
      }

      val (ntpe, selfRef) = normalized(boundTypes)(tpe)
      val ptpe = boundTypes.get(ntpe.typeSymbol.fullName).getOrElse(ntpe)

      // infers implicit
      val neededImplicitType = appliedType(tc.typeConstructor, ptpe)
      val neededImplicit = if (!selfRef) {
        c.inferImplicitValue(neededImplicitType)
      } else c.untypecheck(
        // Reset the type attributes on the refactored tree for the implicit
        tx.transform(c.inferImplicitValue(neededImplicitType)))

      neededImplicit -> selfRef
    }

    // To print the implicit types in the compiler messages
    private def prettyType(boundTypes: Map[String, Type])(t: Type): String =
      boundTypes.getOrElse(t.typeSymbol.fullName, t) match {
        case TypeRef(_, base, args) if args.nonEmpty => s"""${base.asType.fullName}[${args.map(prettyType(boundTypes)(_)).mkString(", ")}]"""

        case t                                       => t.typeSymbol.fullName
      }

    protected def resolver(boundTypes: Map[String, Type], forwardSuffix: String)(tc: Type): Type => Implicit = {
      val tx = new ImplicitTransformer(boundTypes, forwardSuffix)
      createImplicit(boundTypes)(tc, _: Type, tx)
    }

    type Implicit = (Tree, Boolean)
  }
}
