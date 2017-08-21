package reactivemongo.bson

import scala.collection.immutable.Set

import reactivemongo.bson.Macros.Annotations.{ Flatten, Ignore, Key }
import reactivemongo.bson.Macros.Options.SaveSimpleName

import scala.reflect.macros.Context

private object MacroImpl {
  def reader[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentReader[A]] = c.universe.reify(new BSONDocumentReader[A] {
    private val r: BSONDocument => A = { document =>
      Helper[A, Opts](c).readBody.splice
    }

    lazy val forwardReader = BSONDocumentReader[A](r)

    def read(document: BSONDocument) = forwardReader.read(document)
  })

  def writer[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentWriter[A]] = c.universe.reify(new BSONDocumentWriter[A] {
    private val w: A => BSONDocument = { v =>
      Helper[A, Opts](c).writeBody.splice
    }

    lazy val forwardWriter = BSONDocumentWriter[A](w)

    def write(v: A) = forwardWriter.write(v)
  })

  def handler[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A]] = {
    val helper = Helper[A, Opts](c)
    c.universe.reify(
      new BSONDocumentReader[A] with BSONDocumentWriter[A] with BSONHandler[BSONDocument, A] {
        private val r: BSONDocument => A = { document =>
          Helper[A, Opts](c).readBody.splice
        }

        lazy val forwardReader = BSONDocumentReader[A](r)

        private val w: A => BSONDocument = { v =>
          Helper[A, Opts](c).writeBody.splice
        }

        lazy val forwardWriter = BSONDocumentWriter[A](w)

        def read(document: BSONDocument): A = forwardReader.read(document)
        def write(v: A): BSONDocument = forwardWriter.write(v)
      })
  }

  private def Helper[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context) = new Helper[c.type, A](c) {
    val A = c.weakTypeOf[A]
    val Opts = c.weakTypeOf[Opts]
  }

  private abstract class Helper[C <: Context, A](
    val c: C) extends ImplicitResolver[C] {
    import c.universe._

    protected def A: Type
    protected def Opts: Type

    type Reader[A] = BSONReader[_ <: BSONValue, A]
    type Writer[A] = BSONWriter[A, _ <: BSONValue]

    private val writerType: Type = typeOf[Writer[_]].typeConstructor
    private val docWriterType: Type =
      typeOf[BSONDocumentWriter[_]].typeConstructor

    private val readerType: Type = typeOf[Reader[_]].typeConstructor
    private val docReaderType: Type =
      typeOf[BSONDocumentReader[_]].typeConstructor

    lazy val readBody: c.Expr[A] = {
      val reader = unionTypes.map { types =>
        val resolve = resolver(Map.empty, "Reader")(readerType)
        val cases = types.map { typ =>
          val pattern = if (hasOption[SaveSimpleName])
            Literal(Constant(typ.typeSymbol.name.decodedName.toString))
          else
            Literal(Constant(typ.typeSymbol.fullName)) //todo

          val body = readBodyFromImplicit(typ)(resolve).getOrElse {
            // No existing implicit, but can fallback to automatic mat
            readBodyConstruct(typ)
          }

          CaseDef(pattern, body)
        }

        def className = c.parse("""document.getAs[String]("className").get""")

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
          val n = c.fresh("v")
          val id = Ident(n)

          val pattern = Bind(newTermName(n), {
            if (!isSingleton(typ)) Typed(Ident(nme.WILDCARD), TypeTree(typ))
            else Ident(typ.typeSymbol.name.toString)
          })

          def body = writeBodyFromImplicit(id, typ)(resolve).getOrElse {
            // No existing implicit, but can fallback to automatic mat
            writeBodyConstruct(id, typ)
          }

          CaseDef(pattern, body)
        }

        Match(Ident("v"), cases)
      } getOrElse writeBodyConstruct(Ident("v"), A)

      val result = c.Expr[BSONDocument](writer)

      if (hasOption[Macros.Options.Verbose]) {
        c.echo(c.enclosingPosition, show(writer))
      }

      result
    }

    // For member of a union
    private def readBodyFromImplicit(tpe: Type)(r: Type => Implicit): Option[Tree] = {
      val (reader, _) = r(tpe)

      if (!reader.isEmpty) {
        Some(Apply(Select(reader, "read"), List(Ident("document"))))
      } else if (!hasOption[Macros.Options.AutomaticMaterialization]) {
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

      //this is ugly but quite stable compared to other attempts
      Ident(stringToTermName(sym.name.toString))
    }

    private def readBodyConstructClass(implicit tpe: Type) = {
      val (constructor, _) = matchingApplyUnapply(tpe).getOrElse(
        c.abort(c.enclosingPosition, s"No matching apply/unapply found: $tpe"))

      val tpeArgs: List[Type] = tpe match {
        case TypeRef(_, _, args)    => args
        case ClassInfoType(_, _, _) => Nil
      }
      val boundTypes = constructor.typeParams.zip(tpeArgs).flatMap {
        case (sym, ty) => sym.fullName.split("\\.").reverse.toList match {
          case last :: _ if (last == sym.fullName) =>
            List(sym.fullName -> ty)

          case last :: _ => List(sym.fullName -> ty, last -> ty)
        }
      }.toMap
      val resolve = resolver(boundTypes, "Reader")(readerType)

      val values = constructor.paramss.head.map { param =>
        val t = param.typeSignature
        val x = boundTypes.getOrElse(t.typeSymbol.fullName, t)
        val sig = {
          val tps: List[Type] = x match {
            case TypeRef(_, _, args) => args.map { tp =>
              boundTypes.getOrElse(tp.typeSymbol.fullName, tp)
            }

            case ClassInfoType(_, _, _) => Nil
          }

          appliedType(x, tps)
        }
        val opt = optionTypeParameter(sig)
        val typ = opt getOrElse sig
        val (reader, _) = resolve(typ)
        val pname = paramName(param)

        if (reader.isEmpty) {
          c.abort(c.enclosingPosition, s"Implicit not found for '$pname': ${classOf[Reader[_]].getName}[_, $typ]")
        }

        if (param.annotations.exists(_.tpe =:= typeOf[Flatten])) {
          if (reader.toString == "forwardReader") {
            c.abort(
              c.enclosingPosition,
              s"Cannot flatten reader for '$pname': recursive type")
          }

          if (!(reader.tpe <:< appliedType(docReaderType, List(typ)))) {
            c.abort(c.enclosingPosition, s"Cannot flatten reader '$reader': doesn't conform BSONDocumentReader")
          }

          Apply( // ${reader}.read(document)
            Select(reader, "read"), List(Ident("document")))
        } else opt match {
          case Some(_) => // document.getAs[$typ]($pname)($reader)
            Apply(Apply(
              TypeApply(
                Select(Ident("document"), "getAs"),
                List(TypeTree(typ))),
              List(Literal(Constant(pname)))), List(reader))

          case _ => // document.getAsTry[$typ]($pname)($reader).get
            Select(Apply(Apply(
              TypeApply(
                Select(Ident("document"), "getAsTry"),
                List(TypeTree(typ))),
              List(Literal(Constant(pname)))), List(reader)), "get")
        }
      }

      val constructorTree = Select(Ident(companion.name.toString), "apply")

      Apply(constructorTree, values)
    }

    private def writeBodyFromImplicit(id: Ident, tpe: Type)(r: Type => Implicit): Option[Tree] = {
      val (writer, _) = r(tpe)

      if (!writer.isEmpty) {
        @inline def doc = Apply(Select(writer, "write"), List(id))

        Some(classNameTree(tpe).fold[Tree](doc) { nameE =>
          val docE = c.Expr[BSONDocument](doc)

          reify {
            docE.splice ++ BSONDocument(Seq((nameE.splice)))
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
      classNameTree(tpe).map { nameE =>
        reify { BSONDocument(Seq((nameE.splice))) }
      } getOrElse reify(BSONDocument.empty)).tree

    private def writeBodyConstructClass(id: Ident, tpe: Type): Tree = {
      val (constructor, deconstructor) = matchingApplyUnapply(tpe).getOrElse(
        c.abort(c.enclosingPosition, s"No matching apply/unapply found: $tpe"))

      val types = unapplyReturnTypes(deconstructor)
      val constructorParams = constructor.paramss.head
      val tpeArgs: List[Type] = tpe match {
        case TypeRef(_, _, args)    => args
        case ClassInfoType(_, _, _) => Nil
      }
      val boundTypes = constructor.typeParams.zip(tpeArgs).map {
        case (sym, ty) => sym.fullName -> ty
      }.toMap
      val resolve = resolver(boundTypes, "Writer")(writerType)
      lazy val tupleName = newTermName("tuple")

      val (optional, required) =
        constructorParams.zipWithIndex.zip(types).filterNot {
          case ((sym, _), _) => ignoreField(sym)
        }.partition(t => isOptionalType(t._2))

      def resolveWriter(pname: String, tpe: Type) = {
        val (writer, _) = resolve(tpe)

        if (writer.isEmpty) {
          c.abort(c.enclosingPosition, s"Implicit not found for '$pname': ${classOf[Writer[_]].getName}[$tpe, _]")
        }

        writer
      }

      val tupleElement: Int => Tree = {
        val tuple = Ident(tupleName)

        if (types.length == 1) { _: Int => tuple }
        else { i: Int => Select(tuple, "_" + (i + 1)) }
      }

      // TODO: buf newTermName
      val bufName = newTermName("buf")

      // buf += ${pname} -> ${bsv}
      def append[V <: BSONValue](pname: String, bsv: c.Expr[V]): Tree =
        Apply(Select(Ident(bufName), "$plus$eq"), List(reify(
          (c.literal(pname).splice, bsv.splice)).tree))

      // buf ++= ${bsd}.elements
      def appendFields(bsd: c.Expr[BSONDocument]): Tree =
        Apply(
          Select(Ident(bufName), "$plus$plus$eq"),
          List(reify(bsd.splice.elements).tree))

      def mustFlatten(
        param: Symbol,
        pname: String,
        sig: Type,
        writer: Tree): Boolean = {
        if (param.annotations.exists(_.tpe =:= typeOf[Flatten])) {
          if (writer.toString == "forwardWriter") {
            c.abort(
              c.enclosingPosition,
              s"Cannot flatten writer for '$pname': recursive type")
          }

          if (!(writer.tpe <:< appliedType(docWriterType, List(sig)))) {
            c.abort(c.enclosingPosition, s"Cannot flatten writer '$writer': doesn't conform BSONDocumentWriter")
          }

          true
        } else false
      }

      val values = required.map {
        case ((param, i), sig) =>
          val pname = paramName(param)
          val writer = resolveWriter(pname, sig)

          if (mustFlatten(param, pname, sig, writer)) {
            val bsd = c.Expr[BSONDocument](
              Apply(Select(writer, "write"), List(tupleElement(i))))

            appendFields(bsd)
          } else {
            val bsv = c.Expr[BSONValue](
              Apply(Select(writer, "write"), List(tupleElement(i))))

            append(pname, bsv)
          }
      }

      val extra = optional.flatMap {
        case ((param, i), optType) =>
          val sig = optionTypeParameter(optType).get
          val pname = paramName(param)
          val writer = resolveWriter(pname, sig)
          val vterm = newTermName("v")
          val bsv = c.Expr[BSONValue]( // writer.write(${vterm})
            Apply(Select(writer, "write"), List(Ident(vterm))))

          val ap = append(pname, bsv)

          val z = Apply(Select(tupleElement(i), newTermName("foreach")), List(
            Function(List(
              ValDef(Modifiers(c.universe.Flag.PARAM), vterm,
                TypeTree(sig), EmptyTree)), ap)))

          List(z)
      }

      // List[Tree] corresponding to fields appended to the buffer/builder
      val fields = values ++ extra ++ classNameTree(tpe).map { cn =>
        Apply(Select(Ident(bufName), "$plus$eq"), List(cn.tree))
      }

      val writer = c.parse(s"val ${bufName} = scala.collection.immutable.Stream.newBuilder[reactivemongo.bson.BSONElement]") +: (fields :+ c.parse(s"reactivemongo.bson.BSONDocument($bufName.result())"))

      if (values.isEmpty && extra.isEmpty) { // no class parameter to unapply
        Block(writer: _*)
      } else {
        // Extract/unapply the class instance as ${tupleDef}

        val unapplyTree = Select(Ident(companion(tpe).name.toString), "unapply")
        val invokeUnapply = Select(Apply(unapplyTree, List(id)), "get")
        val tupleDef = ValDef(Modifiers(), tupleName, TypeTree(), invokeUnapply)

        Block((tupleDef :: writer): _*)
      }
    }

    private def classNameTree(tpe: Type): Option[c.Expr[(String, BSONString)]] = {
      val tpeSym = A.typeSymbol.asClass

      if (hasOption[Macros.Options.SaveClassName] ||
        tpeSym.isSealed && tpeSym.isAbstractClass) Some {
        val name = if (hasOption[Macros.Options.SaveSimpleName]) {
          c.literal(tpe.typeSymbol.name.decodedName.toString)
        } else c.literal(tpe.typeSymbol.fullName)

        reify("className" -> BSONStringHandler.write(name.splice))
      }
      else None
    }

    private lazy val unionTypes: Option[List[Type]] =
      parseUnionTypes orElse directKnownSubclasses

    private def parseUnionTypes: Option[List[Type]] = {
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
          tpeSym != cls && !cls.isAbstractClass &&
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
          o.companionSymbol == NoSymbol && // not a companion object
          tpeSym != c && o.typeSignature.baseClasses.contains(tpeSym)) => {
          val newSub: Set[Type] = if (!o.moduleClass.asClass.isCaseClass) {
            c.warning(c.enclosingPosition, s"cannot handle object ${o.fullName}: no case accessor")
            Set.empty
          } else Set(o.typeSignature)

          allSubclasses(path.tail, subclasses ++ newSub)
        }

        case Some(o: ModuleSymbol) if (
          o.companionSymbol == NoSymbol // not a companion object
        ) => allSubclasses(path.tail, subclasses)

        case Some(_) => allSubclasses(path.tail, subclasses)

        case _       => subclasses
      }

      if (tpeSym.isSealed && tpeSym.isAbstractClass) {
        Some(allSubclasses(
          tpeSym.owner.typeSignature.declarations, Set.empty).toList)
      } else None
    }

    private def hasOption[O: TypeTag]: Boolean = Opts <:< typeOf[O]

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
    private def optionTypeParameter(implicit tpe: Type): Option[Type] = {
      if (isOptionalType(tpe)) tpe match {
        case TypeRef(_, _, args) => args.headOption
        case _                   => None
      }
      else None
    }

    private def isOptionalType(implicit tpe: Type): Boolean =
      c.typeOf[Option[_]].typeConstructor == tpe.typeConstructor

    private def paramName(param: Symbol): String = param.annotations.collect {
      case ann if ann.tpe =:= typeOf[Key] => ann.scalaArgs.collect {
        case l: Literal => l.value.value
      }.collect {
        case value: String => value
      }
    }.flatMap(identity).headOption getOrElse param.name.toString

    private def ignoreField(param: Symbol): Boolean =
      param.annotations.exists(ann =>
        ann.tpe =:= typeOf[Ignore] || ann.tpe =:= typeOf[transient])

    private def applyMethod(implicit tpe: Type): Option[Symbol] =
      companion(tpe).typeSignature.declaration(stringToTermName("apply")) match {
        case NoSymbol => {
          if (hasOption[Macros.Options.Verbose]) {
            c.echo(c.enclosingPosition, s"No apply function found for $tpe")
          }

          None
        }

        case s => Some(s)
      }

    private def unapplyMethod(implicit tpe: Type): Option[MethodSymbol] =
      companion(tpe).typeSignature.
        declaration(stringToTermName("unapply")) match {
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

        case Some((x, y)) if (
          x.typeSymbol == NoSymbol || y.typeSymbol == NoSymbol) => x.typeSymbol == y.typeSymbol

        case Some((x, y)) => (x.typeSymbol.asType, y.typeSymbol.asType) match {
          case (a, b) if (a.typeParams.size != b.typeParams.size) => {
            c.warning(
              c.enclosingPosition,
              s"Type parameters are not matching: $a != $b")

            false
          }

          case (a, b) if a.typeParams.isEmpty =>
            if (x =:= y) conforms(types.tail) else {
              c.warning(
                c.enclosingPosition,
                s"Types are not compatible: $a != $b")

              false
            }

          case (a, b) if (x.baseClasses != y.baseClasses) => {
            c.warning(
              c.enclosingPosition,
              s"Generic types are not compatible: $a != $b")

            false
          }

          case (a, b) => {
            val sub = a.typeParams.zip(b.typeParams).map {
              case (z, w) => z.typeSignature -> w.typeSignature
            }
            conforms(sub ++: types.tail)
          }

          case _ => true
        }

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
        alt.paramss match {
          case params :: ps if (ps.isEmpty || ps.headOption.flatMap(
            _.headOption).exists(_.isImplicit)) => if (params.size != u.size) false else {
            conforms(params.map(_.typeSignature).zip(u))
          }

          case _ => {
            c.warning(c.enclosingPosition, s"""Constructor with multiple parameter lists is not supported: ${A.typeSymbol.name}${alt.typeSignature}""")

            false
          }
        }
      }.headOption
    } yield apply -> unapply

    private def isSingleton(t: Type): Boolean = t <:< typeOf[Singleton]

    @inline private def companion(implicit tpe: Type): Symbol =
      tpe.typeSymbol.companionSymbol
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
        val tpe = appliedType(base.typeSignature, out.reverse)

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
      val forwardName = newTermName(s"forward$forwardSuffix")

      override def transform(tree: Tree): Tree = tree match {
        case tt: TypeTree =>
          super.transform(TypeTree(denorm(tt.tpe)))

        case Select(Select(This(n), t), sym) if (
          n.toString == "Macros" && t.toString == "Placeholder" &&
          sym.toString == "Handler") => super.transform(Ident(forwardName))

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
      val neededImplicitType = appliedType(tc.typeConstructor, ptpe :: Nil)
      val neededImplicit = if (!selfRef) {
        c.inferImplicitValue(neededImplicitType)
      } else c.resetAllAttrs(
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
