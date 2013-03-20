package reactivemongo.bson

import collection.mutable.ListBuffer
import scala.reflect.macros.Context

/**
 * User: andraz
 * Date: 2/21/13
 * Time: 6:51 PM
 */
private object MacroImpl {
  def read[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONReader[BSONDocument, A]] = {
    val h = Helper[A,Opts](c)
    val body = h.readBody
    c.universe.reify {
      new BSONReader[BSONDocument, A] {
        def read(document: BSONDocument): A = body.splice
      }
    }
  }

  def write[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONWriter[A, BSONDocument]] = {
    val h = Helper[A,Opts](c)
    val body = h.writeBody
    c.universe.reify (
      new BSONWriter[A, BSONDocument]{
        def write(document: A): BSONDocument = body.splice
      }
    )
  }

  def format[A: c.WeakTypeTag, Opts: c.WeakTypeTag](c: Context): c.Expr[BSONReader[BSONDocument, A] with BSONWriter[A, BSONDocument]] = {
    val h = Helper[A,Opts](c)
    val r = h.readBody
    val w = h.writeBody
    c.universe.reify(
      new BSONReader[BSONDocument,A] with BSONWriter[A, BSONDocument] {
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
          val body = readBodyConstruct(typ)
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
          val body = writeBodyConstruct(typ)
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

    private def readBodyConstruct(implicit A: c.Type) = {
      val (constructor, _) = matchingApplyUnapply

      val values = constructor.paramss.head map {
        param =>
          val sig = param.typeSignature
          val optTyp = optionTypeParameter(sig)
          val typ = optTyp getOrElse sig

          val getter = Apply(
            TypeApply(
              Select(Ident("document"), "getAs"),
              List(TypeTree(typ))

            ),
            List(Literal(Constant(param.name.toString)))
          )

          if (optTyp.isDefined)
            getter
          else
            Select(getter, "get")
      }

      val constructorTree = Select(Ident(companion.name.toString), "apply")
      Apply(constructorTree, values)
    }

    private def writeBodyConstruct(A: c.Type): c.Tree = {
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
          val name = c.literal(param.name.toString)
          reify(
            (name.splice, bs_value.splice)
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
          val name = c.literal(param.name.toString)
          If(
            Select(tuple_i, "isDefined"),
            Apply(Select(Ident("buf"), "$plus$colon$eq"), List(reify((name.splice,bs_value.splice)).tree)),
            EmptyTree
          )
        }
      }

      val className = if (hasOption[Macros.Options.SaveClassName]) Some {
        val name = c.literal(A.typeSymbol.fullName)
        reify {
          ("className", BSONStringHandler.write(name.splice))

        }.tree
      } else None

      val mkBSONdoc = Apply(bsonDocPath, values ++ className)

      val withAppends = List(
        ValDef(Modifiers(), newTermName("bson"), TypeTree(), mkBSONdoc),
        c.parse("var buf = scala.collection.immutable.Stream[(String,reactivemongo.bson.BSONValue)]()")
      ) ++ appends :+ c.parse("bson.add(reactivemongo.bson.BSONDocument(buf))")

      val writer = if(optional.length == 0) List(mkBSONdoc) else withAppends

      val unapplyTree = Select(Ident(companion(A).name.toString), "unapply")
      val document = Ident(newTermName("document"))
      val invokeUnapply = Select(Apply(unapplyTree, List(document)), "get")
      val tupleDef = ValDef(Modifiers(), newTermName("tuple"), TypeTree(), invokeUnapply)

      Block(
        (tupleDef :: writer): _*
      )
    }

    private lazy val unionTypes: Option[List[c.Type]] = {
      val unionOption = c.typeOf[Macros.Options.UnionType[_]]
      val union = c.typeOf[Macros.Options.\/[_,_]]
      def parseUnionTree(tree: Type): List[Type] = {
        if(tree <:< union) {
          tree match {
            case TypeRef(_,_, List(a,b)) => a :: parseUnionTree(b)
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

    private def hasOption[O: c.TypeTag]: Boolean = Opts <:< typeOf[O]

    private def unapplyReturnTypes(deconstructor: c.universe.MethodSymbol): List[c.Type] = {
      val opt = deconstructor.returnType match {
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
      val applys = alternatives filter (_.paramss.head.map(_.typeSignature) == u)

      val apply = applys.headOption getOrElse c.abort(c.enclosingPosition, "No matching apply/unapply found")
      (apply,unapply)
    }

    type Writer[A] = BSONWriter[A, _ <: BSONValue]

    private def writerType: c.Type = typeOf[Writer[_]].typeConstructor

    private def companion(implicit A: c.Type): c.Symbol = A.typeSymbol.companionSymbol
  }
}
