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
case class Expression (name : Option[String], element : BSONElement)
{
  import Expression._
  
  /**
   * The logical negation operator attempts to invert this '''Expression'''
   * by using complimentary operators if possible, falling back to the
   * general-case wrapping in a `$not` operator.
   */
  def unary_! : Expression =
    this match {
      case Expression (term, ("$in", vals)) =>
        Expression (term, ("$nin", vals));
        
      case Expression (term, ("$nin", vals)) =>
        Expression (term, ("$in", vals));
        
      case Expression (None, ("$nor", vals)) =>
        Expression (None, ("$or" -> vals));
        
      case Expression (None, ("$or", vals)) =>
        Expression (None, ("$nor" -> vals));
        
      case Expression (Some ("$not"), el) =>
        Expression (None, el);
        
      case Expression (Some (n), _) =>
        Expression (Some ("$not"), (n -> BSONDocument (element)));
        
      case Expression (None, el) =>
        Expression (Some ("$not"), el);
      }
  
  def && (rhs : Expression) : Expression = combine ("$and", rhs);
  
  def !&& (rhs : Expression) : Expression = combine ("$nor", rhs);
  
  def || (rhs : Expression) : Expression = combine ("$or", rhs);
  
  def isEmpty : Boolean = name.isEmpty && element._1.isEmpty;

  private def combine (op : String, rhs : Expression) : Expression =
    if (rhs.isEmpty)
        this;
    else
      element match {
        case (`op`, arr : BSONArray) =>
	      Expression (None, (op, arr ++ BSONArray (toBSONDocument (rhs))));

        case ("", _) =>
          rhs;

        case _ =>
          Expression (
            None,
            (op -> BSONArray (toBSONDocument (this), toBSONDocument (rhs)))
            );
    }
}


object Expression
{
  /**
   * The empty property is provided so that ''monoid'' definitions for '''Expression''' can
   * be easily provided.
   */
  val empty = new Expression (None, "" -> BSONDocument.empty);


  def apply (name : String, element : BSONElement) : Expression =
    new Expression (Some (name), element);
  
  /// Implicit Conversions
  implicit object ExpressionWriter extends BSONDocumentWriter[Expression]
  {
    override def write (expr : Expression) : BSONDocument = toBSONDocument (expr);
  }
  
  implicit def toBSONDocument (expr : Expression) : BSONDocument =
    expr match {
      case Expression (Some (name), element) =>
        BSONDocument (name -> BSONDocument (element));
        
      case Expression (None, ("", _)) =>
        BSONDocument.empty;

      case Expression (None, element) =>
        BSONDocument (element);
    }
  
  implicit def toBSONElement (expr : Expression) : BSONElement = expr.element;
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
  implicit def bsonValueIdentityValue[T <: BSONValue] : ValueBuilder[T] =
  	new ValueBuilder[T] {
      override def bson (v : T) : T = v;
      }
  
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
case class Term[T] (`_term$name` : String)
	extends Dynamic
{
  def ===[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (`_term$name`, "$eq" -> implicitly[ValueBuilder[U]].bson (rhs));
  
  def @==[U <: T : ValueBuilder] (rhs : U) : Expression = ===[U] (rhs);
  
  def <>[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (`_term$name`, "$ne" -> implicitly[ValueBuilder[U]].bson (rhs));
  
  def =/=[U <: T : ValueBuilder] (rhs : U) : Expression = <>[U] (rhs);
  
  def <[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (`_term$name`, "$lt" -> implicitly[ValueBuilder[U]].bson (rhs));
  
  def <=[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (`_term$name`, "$lte" -> implicitly[ValueBuilder[U]].bson (rhs));
  
  def >[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (`_term$name`, "$gt" -> implicitly[ValueBuilder[U]].bson (rhs));
  
  def >=[U <: T : ValueBuilder] (rhs : U) : Expression =
    Expression (`_term$name`, "$gte" -> implicitly[ValueBuilder[U]].bson (rhs));
    
  def exists : Expression =
    Expression (`_term$name`, "$exists" -> BSONBoolean (true));
    
  def in[U <: T : ValueBuilder] (values : Traversable[U]) (implicit B : ValueBuilder[U]) : Expression =
    Expression (`_term$name`, "$in" -> BSONArray (values map (B.bson)));
  
  def in[U <: T : ValueBuilder] (head : U, tail : U *) (implicit B : ValueBuilder[U]) : Expression =
    Expression (`_term$name`, "$in" -> BSONArray (Seq (B.bson (head)) ++ tail.map (B.bson)));
  
  def selectDynamic(field : String) : Term[Any] = Term[Any] (`_term$name` + "." + field);
}


object Term
{
  implicit class CollectionTermOps[T] (val term : Term[Seq[T]]) extends AnyVal
  {
    def all (values : Traversable[T]) (implicit B : ValueBuilder[T]) : Expression =
      Expression (term.`_term$name`, "$all" -> BSONArray (values map (B.bson)));
  }
  
  implicit class StringTermOps[T >: String] (val term : Term[T]) extends AnyVal
  {
    def =~ (re : String) : Expression =
      Expression (term.`_term$name`, "$regex" -> BSONRegex (re, ""));
    
    def !~ (re : String) : Expression =
      Expression (term.`_term$name`, "$not" -> BSONDocument ("$regex" -> BSONRegex (re, "")));
  }
}

