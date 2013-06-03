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
 * 
 * Created on: Jun 2, 2013
 */
package reactivemongo.dsl

import org.specs2.mutable._

import reactivemongo.bson._


/**
 * The '''UntypedCriteriaSpec''' type unit tests the
 * [[reactivemongo.dsl.UntypedCriteria]] EDSL functionality and serves both
 * to verify fitness as well as an exemplar.
 *
 * @author svickers
 *
 */
class UntypedCriteriaSpec extends Specification
{
  import Untyped._
  
  
  "An Untyped criteria" should {
    "support simple filtering" in {
      val q = criteria.myField === "a value";
      
      BSONDocument.pretty (q) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "myField" ->
            BSONDocument ("$eq" -> BSONString ("a value")
            )
          )
        )
      );
    }
    
    "support nested object selectors" in {
      val q = criteria.outer.inner === 99;
      
      BSONDocument.pretty (q) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "outer.inner" ->
            BSONDocument ("$eq" -> BSONInteger (99)
            )
          )
        )
      );
    }
    
    "support String operations" in {
      val q = criteria.str =~ "^test|re";
      
      BSONDocument.pretty (q) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "str" ->
            BSONDocument (
              "$regex" -> BSONRegex ("^test|re", "")
            )
          )
        )
      );
    }
    
    "support conjunctions" in {
      val q = criteria.first < 10 && criteria.second >= 20.0;
      
      BSONDocument.pretty (BSONDocument (q.element)) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$and" ->
            BSONArray (
              BSONDocument (
                "first" -> BSONDocument ("$lt" -> BSONInteger (10))
              ),
              BSONDocument (
                "second" -> BSONDocument ("$gte" -> BSONDouble (20.0))
              )
            )
          )
        )
      );
    }
    
    "support disjunctions" in {
      val q = criteria.first < 10 || criteria.second >= 20.0;
      
      BSONDocument.pretty (BSONDocument (q.element)) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$or" ->
            BSONArray (
              BSONDocument (
                "first" -> BSONDocument ("$lt" -> BSONInteger (10))
              ),
              BSONDocument (
                "second" -> BSONDocument ("$gte" -> BSONDouble (20.0))
              )
            )
          )
        )
      );
    }
    
    "combine adjacent conjunctions" in {
      val q = criteria.first < 10 && criteria.second >= 20.0 && criteria.third < 0.0;
      
      BSONDocument.pretty (BSONDocument (q.element)) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$and" ->
            BSONArray (
              BSONDocument (
                "first" -> BSONDocument ("$lt" -> BSONInteger (10))
              ),
              BSONDocument (
                "second" -> BSONDocument ("$gte" -> BSONDouble (20.0))
              ),
              BSONDocument (
                "third" -> BSONDocument ("$lt" -> BSONDouble (0.0))
              )
            )
          )
        )
      );
    }
    
    "combine adjacent disjunctions" in {
      val q = criteria.first < 10 || criteria.second >= 20.0 || criteria.third < 0.0;
      
      BSONDocument.pretty (BSONDocument (q.element)) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$or" ->
            BSONArray (
              BSONDocument (
                "first" -> BSONDocument ("$lt" -> BSONInteger (10))
              ),
              BSONDocument (
                "second" -> BSONDocument ("$gte" -> BSONDouble (20.0))
              ),
              BSONDocument (
                "third" -> BSONDocument ("$lt" -> BSONDouble (0.0))
              )
            )
          )
        )
      );
    }
    
    "support compound filtering" in {
      val q = criteria.first < 10 && (criteria.second >= 20.0 || criteria.second.in (0.0, 1.0));
      
      BSONDocument.pretty (q) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$and" ->
            BSONArray (
              BSONDocument (
                "first" -> BSONDocument ("$lt" -> BSONInteger (10))
              ),
              BSONDocument (
                "$or" ->
                BSONArray (
                  BSONDocument (
                    "second" -> BSONDocument ("$gte" -> BSONDouble (20.0))
                  ),
                  BSONDocument (
                    "second" ->
                    BSONDocument (
                      "$in" ->
                      BSONArray (BSONDouble (0.0), BSONDouble (1.0))
                    )
                  )
                )
              )
            )
          )
        )
      );
    }
    
    "support alternating logical operators" in {
      val q = criteria.first < 10 && criteria.second >= 20.0 || criteria.third < 0.0 && criteria.fourth =~ "some regex";
      
      BSONDocument.pretty (BSONDocument (q.element)) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$or" ->
            BSONArray (
              BSONDocument (
                "$and" ->
                BSONArray (
                  BSONDocument (
                    "first" -> BSONDocument ("$lt" -> BSONInteger (10))
                  ),
                  BSONDocument (
                    "second" -> BSONDocument ("$gte" -> BSONDouble (20.0))
                  )
                )
              ),
              BSONDocument (
                "$and" ->
                BSONArray (
                  BSONDocument (
                    "third" -> BSONDocument ("$lt" -> BSONDouble (0.0))
                  ),
                  BSONDocument (
                    "fourth" -> BSONDocument ("$regex" -> BSONRegex ("some regex", ""))
                  )
                )
              )
            )
          )
        )
      );
    }
    
    "support logical negation" in {
      BSONDocument.pretty (!(criteria.a =~ "regex(p)?")) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$not" ->
            BSONDocument (
              "a" ->
              BSONDocument (
                "$regex" -> BSONRegex ("regex(p)?", "")
              )
            )
          )
        )
      );
      
      BSONDocument.pretty (!(criteria.xyz === 1 || criteria.xyz === 2)) should_== (
        BSONDocument.pretty (
          BSONDocument (
            "$nor" ->
            BSONArray (
              BSONDocument (
                "xyz" ->
                BSONDocument (
                  "$eq" -> BSONInteger (1)
                )
              ),
              BSONDocument (
                "xyz" ->
                BSONDocument (
                  "$eq" -> BSONInteger (2)
                )
              )
            )
          )
        )
      );
    }
  }
}
