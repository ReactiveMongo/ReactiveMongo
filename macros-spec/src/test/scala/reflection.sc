object reflection {
import scala.reflect.runtime.universe._

val p = Person("james", "doe")                    //> p  : Person = Person(james,doe)
p.lastName                                        //> res0: String = doe

def weakParamInfo[T](x: T)(implicit tag: WeakTypeTag[T]): Unit = {
  val targs = tag.tpe match { case TypeRef(_, _, args) => args }
  //println(s"type of $x has type arguments $targs")
  println(tag.tpe.members)
  
  
}                                                 //> weakParamInfo: [T](x: T)(implicit tag: reflect.runtime.universe.WeakTypeTag[
                                                  //| T])Unit
val a : Person => String = x => x.lastName        //> a  : Person => String = <function1>

def foo[T] = weakParamInfo(a)                     //> foo: [T]=> Unit

foo[Int]                                          //> Scope{
                                                  //|   override def toString(): String;
                                                  //|   def andThen[A <: <?>](g: <?>): T1 => A;
                                                  //|   def compose[A <: <?>](g: <?>): A => R;
                                                  //|   def $init$: <?>;
                                                  //|   final def $asInstanceOf[T0](): T0;
                                                  //|   final def $isInstanceOf[T0](): Boolean;
                                                  //|   final def synchronized[T0](x$1: T0): T0;
                                                  //|   final def ##(): Int;
                                                  //|   final def !=(x$1: AnyRef): Boolean;
                                                  //|   final def ==(x$1: AnyRef): Boolean;
                                                  //|   final def ne(x$1: AnyRef): Boolean;
                                                  //|   final def eq(x$1: AnyRef): Boolean;
                                                  //|   def <init>(): java.lang.Object;
                                                  //|   final def notifyAll(): Unit;
                                                  //|   final def notify(): Unit;
                                                  //|   protected[package lang] def clone(): java.lang.Object;
                                                  //|   final def getClass(): java.lang.Class[_];
                                                  //|   def hashCode(): Int;
                                                  //|   def equals(x$1: Any): Boolean;
                                                  //|   final def wait(): Unit;
                                                  //|   final def wait(x$1: Long): Unit;
                                                  //|   final def wait(x$1: Long,x$2: Int): Unit;
                                                  //|   protected[package lang] def finalize(): Unit;
                                                  //|   final def asInstanceOf[T0]: T0;
                                                  //|   final def isInst
                                                  //| Output exceeds cutoff limit.



  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
}