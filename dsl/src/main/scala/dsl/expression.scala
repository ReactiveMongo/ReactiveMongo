/*
 * Copyright 2013 Steve Vickers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.dsl

import scala.language.{
  dynamics,
  implicitConversions
  }

import reactivemongo.bson._


/**
 * The '''Expression''' type defines a recursive propositional abstract
 * syntax tree central to the MongoDB EDSL.
 */
case class Expression (name : String, value : BSONValue)
{
  def toElement : BSONElement = (name, value);
  
  def && (rhs : Expression) : Expression = combine ("$and", rhs);
  
  def !&& (rhs : Expression) : Expression = combine ("$nor", rhs);
  
  def || (rhs : Expression) : Expression = combine ("$or", rhs);
  
  private def combine (op : String, rhs : Expression) : Expression =
    (name, value) match {
      case (`op`, arr : BSONArray) =>
	    Expression (name, arr ++ BSONArray (BSONDocument (rhs.toElement)))

      case _ =>
        Expression (
          op,
          BSONArray (BSONDocument (toElement), BSONDocument (rhs.toElement))
          );
    }
}


object Expression
{
  /// Implicit Conversions
  implicit object ExpressionWriter extends BSONWriter[Expression, BSONDocument]
  {
    override def write (expr : Expression) : BSONDocument = toBSONDocument (expr);
  }
  
  implicit def toBSONDocument (expr : Expression) : BSONDocument = BSONDocument (expr.toElement);
  
  implicit def toBSONElement (expr : Expression) : BSONElement = expr.toElement;
}


/**
 * The '''ValueBuilder'' type is a model of the ''type class'' pattern used to
 * produce a ''T''-specific [[reactivemongo.bson.BSONValue]] instance.
 */
sealed trait ValueBuilder[T]
{
  def bson (v : T) : BSONValue;
}


object ValueBuilder
{
  implicit object DateTimeValue
  	extends ValueBuilder[java.util.Date]
  {
    override def bson (v : java.util.Date) : BSONValue = BSONDateTime (v.getTime);
  }
  
  implicit object BooleanValue
  	extends ValueBuilder[Boolean]
  {
    override def bson (v : Boolean) : BSONValue = BSONBoolean (v);
  }
  
  implicit object DoubleValue
  	extends ValueBuilder[Double]
  {
    override def bson (v : Double) : BSONValue = BSONDouble (v);
  }
  
  implicit object IntValue
  	extends ValueBuilder[Int]
  {
    override def bson (v : Int) : BSONValue = BSONInteger (v);
  }
  
  implicit object LongValue
  	extends ValueBuilder[Long]
  {
    override def bson (v : Long) : BSONValue = BSONLong (v);
  }
  
  implicit object StringValue
  	extends ValueBuilder[String]
  {
    override def bson (v : String) : BSONValue = BSONString (v);
  }
  
  implicit object SymbolValue
  	extends ValueBuilder[Symbol]
  {
    override def bson (v : Symbol) : BSONValue = BSONSymbol (v.name);
  }
  
  implicit object TimestampValue
  	extends ValueBuilder[java.sql.Timestamp]
  {
    override def bson (v : java.sql.Timestamp) : BSONValue = BSONTimestamp (v.getTime);
  }
}

/**
 * A '''Term'' instance reifies the use of a MongoDB document field, both
 * top-level or nested.  Operators common to all ''T'' types are defined here
 * with type-specific ones provided in the companion object below.
 */
case class Term[T] (name : String)
	extends Dynamic
{
  def ===[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (name, implicitly[ValueBuilder[U]].bson (rhs));
  
  def @==[U <: T : ValueBuilder] (rhs : U) : Expression = ===[U] (rhs);
  
  def <>[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (name, BSONDocument ("$ne" -> implicitly[ValueBuilder[U]].bson (rhs)));
  
  def =/=[U <: T : ValueBuilder] (rhs : U) : Expression = <>[U] (rhs);
  
  def <[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (name, BSONDocument ("$lt" -> implicitly[ValueBuilder[U]].bson (rhs)));
  
  def <=[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (name, BSONDocument ("$lte" -> implicitly[ValueBuilder[U]].bson (rhs)));
  
  def >[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (name, BSONDocument ("$gt" -> implicitly[ValueBuilder[U]].bson (rhs)));
  
  def >=[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (name, BSONDocument ("$gte" -> implicitly[ValueBuilder[U]].bson (rhs)));
  
  def selectDynamic(field : String) : Term[Any] = Term[Any] (name + "." + field);
}


object Term
{
  implicit class CollectionTermOps[T] (val term : Term[Seq[T]]) extends AnyVal
  {
    def all (values : Seq[T]) (implicit B : ValueBuilder[T]) : Expression =
      Expression (term.name, BSONDocument ("$all" -> BSONArray (values map (B.bson))));
    
    def in (values : Seq[T]) (implicit B : ValueBuilder[T]) : Expression =
      Expression (term.name, BSONDocument ("$in" -> BSONArray (values map (B.bson))));
  }
  
  implicit class StringTermOps[T >: String] (val term : Term[T]) extends AnyVal
  {
    def =~ (re : String) : Expression =
      Expression (term.name, BSONDocument ("$regex" -> BSONRegex (re, "")));
    
    def !~ (re : String) : Expression =
      Expression (term.name, BSONDocument ("$not" -> BSONDocument ("$regex" -> BSONRegex (re, ""))));
  }
}

