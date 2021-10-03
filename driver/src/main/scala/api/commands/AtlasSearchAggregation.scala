package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * @define queryParam the value or values to search for
 * @define pathParam the indexed field or fields to search
 * @define scoreParam the optional score modifier (default: `None`)
 */
private[commands] trait AtlasSearchAggregation[P <: SerializationPack] {
  aggregation: AggregationFramework[P] =>

  /**
   * '''EXPERIMENTAL:''' See [[AtlasSearch$]]
   */
  final class AtlasSearch private[api] (
    val operator: AtlasSearch.Operator) extends PipelineOperator {

    def makePipe: pack.Document =
      pipe(f"$$search", pipe(operator.name, operator.document))

    @inline override def hashCode: Int = operator.hashCode

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (this.operator == null && other.operator == null) || (
          this.operator != null && this.operator == other.operator)

      case _ =>
        false
    }

    override def toString = s"AtlasSearch(${operator.toString})"
  }

  /**
   * '''EXPERIMENTAL:'''
   * One or at least one string with optional alternate analyzer specified in multi field
   */
  final class SearchString private[api] (
    val head: String,
    val next: Seq[String],
    val multi: Option[String]) {

    lazy val values: Seq[String] = head +: next

    private[api] def value: pack.Value = {
      import builder.{ array, string, elementProducer => elm }

      (next.headOption, multi) match {
        case (Some(_), None) =>
          array(string(head) +: next.map(string))

        case (Some(_), Some(alterAnalyzer)) => array(
          builder.document(Seq(
            elm("value", string(head)),
            elm("multi", string(alterAnalyzer)))) +: next.map(string))

        case (None, Some(alterAnalyzer)) => array(Seq(
          builder.document(Seq(
            elm("value", string(head)),
            elm("multi", string(alterAnalyzer))))))

        case _ =>
          string(head)
      }
    }

    @inline override def hashCode: Int = values.hashCode

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.values == other.values

      case _ =>
        false
    }

    override def toString: String =
      s"""SearchString${values.mkString("[ ", ", ", " ]")}"""
  }

  /**
   * '''EXPERIMENTAL:''' Search string utilities
   * @see [[https://docs.atlas.mongodb.com/reference/atlas-search/path-construction/#usage]]
   */
  object SearchString {
    import scala.language.implicitConversions

    /**
     * Returns a single search string.
     *
     * {{{
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def prepare(
     *   coll: BSONCollection): coll.AggregationFramework.SearchString =
     *   "foo"
     * }}}
     */
    implicit def apply(single: String): SearchString =
      new SearchString(single, Seq.empty, None)

    /**
     * Returns a search string from more than one strings.
     *
     * {{{
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def prepare(
     *   coll: BSONCollection): coll.AggregationFramework.SearchString =
     *   "foo" -> Seq("bar", "lorem")
     * }}}
     */
    implicit def apply(strings: (String, Seq[String])): SearchString =
      new SearchString(strings._1, strings._2, None)

    /**
     * Returns a single search string with alternate analyzer specified.
     *
     * {{{
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def prepare(
     *   coll: BSONCollection): coll.AggregationFramework.SearchString =
     *   coll.AggregationFramework.SearchString("foo", "mySecondaryAnalyzer")
     * }}}
     */
    def apply(single: String, multi: String): SearchString =
      new SearchString(single, Seq.empty, Some(multi))

    /**
     * Returns a search string from more than one strings with alternate analyzer specified for first term.
     *
     * {{{
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def prepare(
     *   coll: BSONCollection): coll.AggregationFramework.SearchString =
     *   coll.AggregationFramework.SearchString(
     *     "foo", "mySecondaryAnalyzer", Seq("bar", "lorem"))
     * }}}
     */
    def apply(
      single: String,
      alternateAnalyzer: String,
      next: Seq[String]): SearchString =
      new SearchString(single, next, Some(alternateAnalyzer))

  }

  /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/tutorial/ Atlas Search]] (only on MongoDB Atlas) */
  object AtlasSearch {
    /**
     * @param operator the Atlas Search top [[https://docs.atlas.mongodb.com/reference/atlas-search/query-syntax/#fts-operators operator]]
     *
     * {{{
     * import reactivemongo.api.bson.collection.BSONCollection
     *
     * def prepare(
     *   coll: BSONCollection): coll.AggregationFramework.AtlasSearch =
     *   coll.AggregationFramework.AtlasSearch(
     *     coll.AggregationFramework.AtlasSearch.Term(
     *       query = "foo",
     *       path = "field1" // or "field1" -> Seq("field2", ...)
     *     )
     *   )
     * }}}
     */
    def apply(operator: Operator): AtlasSearch = new AtlasSearch(operator)

    // ---

    /** '''EXPERIMENTAL:''' See [[Operator$]] */
    sealed trait Operator {
      def name: String

      private[api] def document: pack.Document
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/query-syntax/#fts-operators Operator]] for [[]] */
    object Operator {
      /** Creates a search operator with given options. */
      def apply(name: String, options: pack.Document): Operator =
        new DefaultOp(name, options)

      private final class DefaultOp(
        val name: String,
        val document: pack.Document) extends Operator {
      }
    }

    /** '''EXPERIMENTAL:''' Score option for term operator */
    sealed trait Score {
      def value: Double

      private[api] def document: pack.Document
    }

    /** '''EXPERIMENTAL:''' Multiplies the result score by the given number. */
    final class BoostScore private[api] (
      val value: Double) extends Score {

      def document = builder.document(Seq(builder.elementProducer(
        "boost", builder.document(Seq(builder.elementProducer(
          "value", builder.double(value)))))))

      @inline override def hashCode: Int = value.toInt

      @SuppressWarnings(Array("ComparingFloatingPointTypes"))
      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.value == other.value

        case _ =>
          false
      }

      override def toString = s"BoostScore($value)"
    }

    /** '''EXPERIMENTAL:''' Replaces the result score with the given number */
    final class ConstantScore private[api] (
      val value: Double) extends Score {

      def document = builder.document(Seq(builder.elementProducer(
        "constant", builder.document(Seq(builder.elementProducer(
          "value", builder.double(value)))))))

      @inline override def hashCode: Int = value.toInt

      @SuppressWarnings(Array("ComparingFloatingPointTypes"))
      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.value == other.value

        case _ =>
          false
      }

      override def toString = s"ConstantScore($value)"
    }

    object Score {
      /**
       * '''EXPERIMENTAL:''' Multiplies the result score by the given number.
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(
       *   coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Score =
       *   coll.AggregationFramework.AtlasSearch.Score.boost(1.23D)
       * }}}
       */
      def boost(multiplier: Double): BoostScore = new BoostScore(multiplier)

      /**
       * '''EXPERIMENTAL:''' Replaces the result score with the given number.
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(
       *   coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Score =
       *   coll.AggregationFramework.AtlasSearch.Score.constant(2.1D)
       * }}}
       */
      def constant(value: Double): ConstantScore = new ConstantScore(value)
    }

    // ---

    /**
     * '''EXPERIMENTAL:''' See [[Term$]]
     *
     * @param query $queryParam
     * @param path $pathParam
     * @param score $scoreParam
     */
    @deprecated("https://docs.atlas.mongodb.com/reference/atlas-search/term/", "")
    final class Term private[api] (
      val query: SearchString,
      val path: SearchString,
      val modifier: Option[Term.Modifier],
      val score: Option[Score]) extends Operator {

      val name = "term"

      def document: pack.Document = {
        import builder.{ boolean, elementProducer => elm }

        val opts = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elm("query", query.value),
          elm("path", path.value))

        modifier.foreach {
          case Term.Wildcard =>
            opts += elm("wildcard", boolean(true))

          case Term.Regex =>
            opts += elm("regex", boolean(true))

          case Term.Prefix =>
            opts += elm("prefix", boolean(true))

          case fuzzy: Term.Fuzzy =>
            opts += elm("fuzzy", builder.document(Seq(
              elm("maxEdits", builder.int(fuzzy.maxEdits)),
              elm("prefixLength", builder.int(fuzzy.prefixLength)))))
        }

        score.foreach { sc =>
          opts += elm("score", sc.document)
        }

        builder.document(opts.result())
      }

      private lazy val tupled = Tuple4(
        query, path, modifier, score)

      @inline override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchTerm${tupled.toString}"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/term/#term-ref Term]] operator for [[]]. */
    object Term {
      /**
       * @param query $queryParam
       * @param path $pathParam
       * @param modifier the optional modifier for the term query execution (`wildcard` or `regex` or `prefix` or `fuzzy`)
       * @param score $scoreParam
       */
      def apply(
        query: SearchString,
        path: SearchString,
        modifier: Option[Modifier] = None,
        score: Option[Score] = None): Term =
        new Term(query, path, modifier, score)

      // ---

      /** '''EXPERIMENTAL:''' Term query mode */
      sealed trait Modifier

      /** '''EXPERIMENTAL:''' Wildcard mode */
      object Wildcard extends Modifier

      /** '''EXPERIMENTAL:''' Regular expression mode */
      object Regex extends Modifier

      /** '''EXPERIMENTAL:''' Prefix mode */
      object Prefix extends Modifier

      /** '''EXPERIMENTAL:''' Fuzzy search options */
      final class Fuzzy private[api] (
        val maxEdits: Int,
        val prefixLength: Int) extends Modifier {
        private lazy val tupled = maxEdits -> prefixLength

        override def hashCode = tupled.hashCode

        override def equals(that: Any): Boolean = that match {
          case other: this.type =>
            this.tupled == other.tupled

          case _ =>
            false
        }

        override def toString =
          s"Fuzzy(maxEdits = $maxEdits, prefixLength = $prefixLength)"
      }

      /** '''EXPERIMENTAL:''' Fuzzy search options */
      object Fuzzy {
        /**
         * @param maxEdits the maximum number of single-character edits required to match the specified search term
         * @param prefixLength the number of characters at the beginning of the result that must exactly match the search term
         */
        def apply(
          maxEdits: Int = 2,
          prefixLength: Int = 0): Fuzzy = new Fuzzy(maxEdits, prefixLength)
      }
    }

    // ---

    /**
     * '''EXPERIMENTAL:''' See [[Text$]]
     *
     * @param query $queryParam
     * @param path $pathParam
     * @param score $scoreParam
     */
    final class Text private[api] (
      val query: SearchString,
      val path: SearchString,
      val fuzzy: Option[Text.Fuzzy],
      val score: Option[Score]) extends Operator {

      val name = "text"

      def document: pack.Document = {
        import builder.{ elementProducer => elm }

        val elms = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elm("query", query.value),
          elm("path", path.value))

        fuzzy.foreach { mod =>
          elms += elm("fuzzy", builder.document(Seq(
            elm("maxEdits", builder.int(mod.maxEdits)),
            elm("prefixLength", builder.int(mod.prefixLength)),
            elm("maxExpansions", builder.int(mod.maxExpansions)))))
        }

        score.foreach { sc =>
          elms += elm("score", sc.document)
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple4(query, path, fuzzy, score)

      override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchText${tupled.toString}"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/text/#text-ref Text]] operator for Atlas Search */
    object Text {
      /**
       * @param query $queryParam
       * @param path $pathParam
       * @param fuzzy enables the fuzzy search (default: `None`)
       * @param score $scoreParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Text = coll.AggregationFramework.AtlasSearch.Text(
       *   query = "foo",
       *   path = "field1", // or "field1" -> Seq("field2", ...)
       *   score = Some(
       *     coll.AggregationFramework.AtlasSearch.Score.boost(1.23D))
       * )
       * }}}
       */
      def apply(
        query: SearchString,
        path: SearchString,
        fuzzy: Option[Text.Fuzzy] = None,
        score: Option[Score] = None): Text =
        new Text(query, path, fuzzy, score)

      // ---

      /** '''EXPERIMENTAL:''' Fuzzy search options */
      final class Fuzzy private[api] (
        val maxEdits: Int,
        val prefixLength: Int,
        val maxExpansions: Int) {
        private lazy val tupled = Tuple3(maxEdits, prefixLength, maxExpansions)

        override def hashCode = tupled.hashCode

        override def equals(that: Any): Boolean = that match {
          case other: this.type =>
            this.tupled == other.tupled

          case _ =>
            false
        }

        override def toString =
          s"Fuzzy(maxEdits = $maxEdits, prefixLength = $prefixLength, maxExpansions = $maxExpansions)"
      }

      /**
       * '''EXPERIMENTAL:''' Fuzzy search options.
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Text.Fuzzy = coll.AggregationFramework.AtlasSearch.Text.Fuzzy(
       *   maxEdits = 1, prefixLength = 2
       * )
       * }}}
       */
      object Fuzzy {
        /**
         * @param maxEdits the maximum number of single-character edits required to match the specified search term
         * @param prefixLength the number of characters at the beginning of the result that must exactly match the search term
         * @param maxExpansions the maximum number of variations to generate and search for
         */
        def apply(
          maxEdits: Int = 2,
          prefixLength: Int = 0,
          maxExpansions: Int = 50): Fuzzy = new Fuzzy(maxEdits, prefixLength, maxExpansions)
      }
    }

    /**
     * '''EXPERIMENTAL:''' See [[Phrase$]]
     *
     * @param query $queryParam
     * @param path $pathParam
     * @param score $scoreParam
     */
    final class Phrase private[api] (
      val query: SearchString,
      val path: SearchString,
      val slop: Int,
      val score: Option[Score]) extends Operator {

      val name = "phrase"

      def document: pack.Document = {
        import builder.{ elementProducer => elm }

        val elms = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elm("query", query.value),
          elm("path", path.value),
          elm("slop", builder.int(slop)))

        score.foreach { sc =>
          elms += elm("score", sc.document)
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple4(query, path, slop, score)

      @inline override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchPhrase${tupled.toString}"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/phrase/#phrase-ref Phrase]] operator for Atlas Search */
    object Phrase {
      /**
       * @param query $queryParam
       * @param path $pathParam
       * @param slop the allowable distance between words in the query phrase (default: 0)
       * @param score $scoreParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(
       *   coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Phrase =
       *   coll.AggregationFramework.AtlasSearch.Phrase(
       *     query = "foo" -> Seq("bar"),
       *     path = "title",
       *     slop = 5)
       * }}}
       */
      def apply(
        query: SearchString,
        path: SearchString,
        slop: Int = 0,
        score: Option[Score] = None): Phrase =
        new Phrase(query, path, slop, score)
    }

    /** '''EXPERIMENTAL:''' See [[Compound$]] */
    final class Compound private[api] (
      val head: Compound.Clause,
      val next: Seq[Compound.Clause],
      val minimumShouldMatch: Option[Int]) extends Operator {
      val name = "compound"

      def document: pack.Document = {
        import builder.{ elementProducer => elm }

        def docOp(op: Operator): pack.Document =
          builder.document(Seq(elm(op.name, op.document)))

        val elms = Seq.newBuilder[pack.ElementProducer]

        def clauseElm(c: Compound.Clause): Unit = {
          import c._1.{ toString => clauseType }, c._2.{
            _1 => firstOp,
            _2 => ops
          }

          elms += elm(clauseType, builder.array(
            docOp(firstOp) +: ops.map(docOp)))

          ()
        }

        clauseElm(head)

        next.map(clauseElm)

        minimumShouldMatch.foreach { mini =>
          elms += elm("minimumShouldMatch", builder.int(mini))
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple3(head, next, minimumShouldMatch)

      override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchCompound${tupled.toString}"
    }

    /**
     * '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/compound/ Compound]] operator for Atlas Search.
     *
     * @define newBuilderBrief Returns a compound builder with the first clause
     * @define clauseTypeParam the type of the clause
     * @define opParam the search operator
     * @define nextParam more search operators
     * @define operators the non empty list of operators
     * @define minimumShouldMatchParam the option to specify a minimum number of clauses which must match to return a result
     */
    object Compound {
      /**
       * $newBuilderBrief and no `minimumShouldMatch` setting.
       *
       * @param clauseType $clauseTypeParam
       * @param head $opParam to define the first/mandatory clause
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
       *   import coll.AggregationFramework.AtlasSearch.{ Compound, Term
       *   }
       *
       *   val term = Term(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   Compound.newBuilder(Compound.must, term).result()
       * }
       * }}}
       */
      def newBuilder(
        clauseType: ClauseType,
        head: Operator): Builder = new Builder(
        (clauseType -> (head -> Seq.empty)), Seq.empty, None)

      /**
       * $newBuilderBrief and with given `minimumShouldMatch` setting.
       *
       * @param clauseType $clauseTypeParam
       * @param head $opParam to define the first/mandatory clause
       * @param minimumShouldMatch $minimumShouldMatchParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
       *   import coll.AggregationFramework.AtlasSearch.{ Compound, Term }
       *
       *   val term = Term(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   Compound.newBuilder(
       *     Compound.must, term, minimumShouldMatch = 2).result()
       * }
       * }}}
       */
      def newBuilder(
        clauseType: ClauseType,
        head: Operator,
        minimumShouldMatch: Int): Builder = new Builder(
        (clauseType -> (head -> Seq.empty)), Seq.empty, Some(minimumShouldMatch))

      /**
       * $newBuilderBrief (with multiple operators)
       * and no `minimumShouldMatch` setting.
       *
       * @param clauseType $clauseTypeParam
       * @param head $opParam to define the first/mandatory clause
       * @param next $nextParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
       *   import coll.AggregationFramework.AtlasSearch.{ Compound, Term, Text }
       *
       *   val term = Term(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   val text = Text(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   Compound.newBuilder(
       *     Compound.must, term, Seq(text)).result()
       * }
       * }}}
       */
      def newBuilder(
        clauseType: ClauseType,
        head: Operator,
        next: Seq[Operator]): Builder = new Builder(
        (clauseType -> (head -> next)), Seq.empty, None)

      /**
       * $newBuilderBrief (with multiple operators)
       * and with given `minimumShouldMatch` setting.
       *
       * @param clauseType $clauseTypeParam
       * @param head $opParam to define the first/mandatory clause
       * @param next $nextParam
       * @param minimumShouldMatch $minimumShouldMatchParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
       *   import coll.AggregationFramework.AtlasSearch.{ Compound, Term, Text }
       *
       *   val term = Term(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   val text = Text(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   Compound.newBuilder(
       *     Compound.must, term, Seq(text),
       *     minimumShouldMatch = 5).result()
       * }
       * }}}
       */
      def newBuilder(
        clauseType: ClauseType,
        head: Operator,
        next: Seq[Operator],
        minimumShouldMatch: Int): Builder = new Builder(
        (clauseType -> (head -> next)), Seq.empty, Some(minimumShouldMatch))

      /**
       * $newBuilderBrief (with multiple operators)
       * and no `minimumShouldMatch` setting.
       *
       * @param clauseType $clauseTypeParam
       * @param operators $operators
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
       *   import coll.AggregationFramework.AtlasSearch.{ Compound, Term, Text }
       *
       *   val term = Term(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   val text = Text(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   Compound.newBuilder(
       *     Compound.must, term -> Seq(text)).result()
       * }
       * }}}
       */
      def newBuilder(
        clauseType: ClauseType,
        operators: Operators): Builder = new Builder(
        (clauseType -> operators), Seq.empty, None)

      /**
       * $newBuilderBrief (with multiple operators)
       * and with given `minimumShouldMatch` setting.
       *
       * @param clauseType $clauseTypeParam
       * @param operators $operators
       * @param minimumShouldMatch $minimumShouldMatchParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
       *   import coll.AggregationFramework.AtlasSearch.{ Compound, Term, Text }
       *
       *   val term = Term(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   val text = Text(
       *     query = "foo" -> Seq.empty,
       *     path = "title" -> Seq("description", "tags"))
       *
       *   Compound.newBuilder(
       *     Compound.must, term -> Seq(text),
       *     minimumShouldMatch = 5).result()
       * }
       * }}}
       */
      def newBuilder(
        clauseType: ClauseType,
        operators: Operators,
        minimumShouldMatch: Int): Builder = new Builder(
        (clauseType -> operators), Seq.empty, Some(minimumShouldMatch))

      /**
       * '''EXPERIMENTAL:''' Type of [[Compound]] clause;
       * Actually either [[must]], [[mustNot]], [[should]] or [[filter]].
       */
      final class ClauseType private[api] (
        override val toString: String) {

        @inline override def hashCode: Int = toString.hashCode

        override def equals(that: Any): Boolean = that match {
          case other: this.type =>
            this.toString == other.toString

          case _ =>
            false
        }
      }

      val must: ClauseType = new ClauseType("must")
      val mustNot: ClauseType = new ClauseType("mustNot")
      val should: ClauseType = new ClauseType("should")
      val filter: ClauseType = new ClauseType("filter")

      type Operators = (Operator, Seq[Operator])
      type Clause = (ClauseType, Operators)
      type Clauses = (Clause, Seq[Clause])

      /**
       * '''EXPERIMENTAL:''' Compound search builder.
       *
       * @define appendBrief Appends a clause
       * @define appendWarning Override any clause previously defined for the same type
       */
      final class Builder private[api] (
        head: Clause,
        next: Seq[Clause],
        minimumShouldMatch: Option[Int]) {
        def result(): Compound = new Compound(
          head, next.reverse, minimumShouldMatch)

        /**
         * $appendBrief with a single operator. $appendWarning.
         *
         * @param clauseType $clauseTypeParam
         * @param op $opParam
         *
         * {{{
         * import reactivemongo.api.bson.collection.BSONCollection
         *
         * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
         *   import coll.AggregationFramework.AtlasSearch.{
         *     Compound, Term, Text
         *   }
         *
         *   val term = Term(
         *     query = "foo" -> Seq.empty,
         *     path = "title" -> Seq("description", "tags"))
         *
         *   val text = Text(
         *     query = "foo" -> Seq.empty,
         *     path = "title" -> Seq("description", "tags"))
         *
         *   Compound.newBuilder(Compound.must, term).
         *     append(Compound.mustNot, text). // <--- HERE
         *     result()
         * }
         * }}}
         */
        def append(
          clauseType: ClauseType,
          op: Operator): Builder = new Builder(
          head, (clauseType -> (op -> Seq.empty)) +: next, minimumShouldMatch)

        /**
         * $appendBrief with multiple operators. $appendWarning.
         *
         * @param clauseType $clauseTypeParam
         * @param op $opParam
         * @param ops $nextParam
         *
         * {{{
         * import reactivemongo.api.bson.collection.BSONCollection
         *
         * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
         *   import coll.AggregationFramework.AtlasSearch.{
         *     Compound, Term, Text
         *   }
         *
         *   val term = Term(
         *     query = "foo" -> Seq.empty,
         *     path = "title" -> Seq("description", "tags"))
         *
         *   val text1 = Text(
         *     query = "foo" -> Seq.empty,
         *     path = "title" -> Seq("description", "tags"))
         *
         *   val text2 = Text(
         *     query = "bar" -> Seq.empty,
         *     path = "title" -> Seq.empty)
         *
         *   Compound.newBuilder(Compound.must, term).
         *     append(Compound.mustNot, text1, Seq(text2)). // <--- HERE
         *     result()
         * }
         * }}}
         */
        def append(
          clauseType: ClauseType,
          op: Operator,
          ops: Seq[Operator]): Builder = new Builder(
          head, (clauseType -> (op -> ops)) +: next, minimumShouldMatch)

        /**
         * $appendBrief with multiple operators. $appendWarning.
         *
         * @param clauseType $clauseTypeParam
         * @param operators $operators
         *
         * {{{
         * import reactivemongo.api.bson.collection.BSONCollection
         *
         * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
         *   import coll.AggregationFramework.AtlasSearch.{
         *     Compound, Term, Text
         *   }
         *
         *   val term = Term(
         *     query = "foo" -> Seq.empty,
         *     path = "title" -> Seq("description", "tags"))
         *
         *   val text1 = Text(
         *     query = "foo" -> Seq.empty,
         *     path = "title" -> Seq("description", "tags"))
         *
         *   val text2 = Text(
         *     query = "bar" -> Seq.empty,
         *     path = "title" -> Seq.empty)
         *
         *   Compound.newBuilder(Compound.must, term).
         *     append(Compound.mustNot, text1 -> Seq(text2)). // <--- HERE
         *     result()
         * }
         * }}}
         */
        def append(
          clauseType: ClauseType,
          operators: Operators): Builder = new Builder(
          head, (clauseType -> operators) +: next, minimumShouldMatch)

        /**
         * Updates the `minimumShouldMatch` setting for the compound search.
         *
         * {{{
         * import reactivemongo.api.bson.collection.BSONCollection
         *
         * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Compound = {
         *   import coll.AggregationFramework.AtlasSearch.{
         *     Compound, Term
         *   }
         *
         *   val term = Term(
         *     query = "foo" -> Seq.empty,
         *     path = "title" -> Seq("description", "tags"))
         *
         *   Compound.newBuilder(Compound.must, term).
         *     minimumShouldMatch(10). // <--- HERE
         *     result()
         * }
         * }}}
         */
        def minimumShouldMatch(minimum: Int): Builder =
          new Builder(head, next, Some(minimum))
      }
    }

    /**
     * '''EXPERIMENTAL:''' See [[Exists$]]
     *
     * @param path $pathParam
     */
    final class Exists private[api] (
      val path: SearchString) extends Operator {

      val name = "exists"

      def document: pack.Document =
        builder.document(Seq(
          builder.elementProducer("path", path.value)))

      @inline override def hashCode: Int = path.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.path == other.path

        case _ =>
          false
      }

      override def toString = s"SearchExists($path)"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/exists/#exists-ref Exists]] operator for Atlas Search */
    object Exists {
      /**
       * @param path $pathParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection) =
       *   coll.AggregationFramework.AtlasSearch.Exists("field")
       * }}}
       */
      def apply(path: SearchString): Exists =
        new Exists(path)
    }

    /**
     * '''EXPERIMENTAL:''' See [[Near$]]
     *
     * @see [[https://docs.atlas.mongodb.com/reference/atlas-search/near/ near]] operator
     *
     * @param origin $originParam
     * @param path $pathParam
     * @param score $scoreParam
     */
    final class Near private[api] (
      val origin: Near.Origin,
      val path: SearchString,
      val pivot: Option[Double],
      val score: Option[Score]) extends Operator {

      val name = "near"

      def document: pack.Document = {
        import builder.{ elementProducer => elm }

        val elms = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elm("origin", origin.value),
          elm("path", path.value))

        pivot.foreach { pv =>
          elms += elm("pivot", builder.double(pv))
        }

        score.foreach { sc =>
          elms += elm("score", sc.document)
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple4(origin, path, pivot, score)

      @inline override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchNear${tupled.toString}"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/near/#near-ref Near]] operator for Atlas Search */
    object Near {
      /**
       * @param origin $originParam
       * @param path $pathParam
       * @param pivot the value to use to calculate scores of Atlas Search result documents
       * @param score $scoreParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Near = coll.AggregationFramework.AtlasSearch.Near(
       *   origin = coll.AggregationFramework.AtlasSearch.Near.int(1),
       *   path = "field",
       *   pivot = Some(0.5D)
       * )
       * }}}
       */
      def apply(
        origin: Near.Origin,
        path: SearchString,
        pivot: Option[Double] = None,
        score: Option[Score] = None): Near =
        new Near(origin, path, pivot, score)

      final class Origin private[api] (
        private[api] val value: pack.Value)

      /**
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection) = coll.AggregationFramework.
       *   AtlasSearch.Near.date(java.time.Instant.now())
       * }}}
       */
      def date(origin: java.time.Instant): Origin =
        new Origin(builder dateTime origin.toEpochMilli)

      /**
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection) =
       *   coll.AggregationFramework.AtlasSearch.Near.int(1)
       * }}}
       */
      def int(value: Int): Origin =
        new Origin(builder.int(value))

      def short(value: Short): Origin =
        new Origin(builder.int(value.toInt))

      def long(value: Long): Origin =
        new Origin(builder.long(value))

      def float(value: Float): Origin =
        new Origin(builder.double(value.toDouble))

      def double(value: Double): Origin =
        new Origin(builder.double(value))
    }

    /**
     * '''EXPERIMENTAL:''' See [[Range$]]
     *
     * @param path $pathParam
     * @param score $scoreParam
     */
    final class Range(
      val path: SearchString,
      val start: Range.Start,
      val end: Range.End,
      val score: Option[Score]) extends Operator {
      val name = "range"

      def document: pack.Document = {
        import builder.{ elementProducer => elm }

        val elms = Seq.newBuilder[pack.ElementProducer]

        elms ++= Seq(
          elm("path", path.value),
          elm(start.tpe, start.value),
          elm(end.tpe, end.value))

        score.foreach { sc =>
          elms += elm("score", sc.document)
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple4(path, start, end, score)

      override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/range/ Range]] operator for Atlas Search */
    object Range {
      /**
       * @param path $pathParam
       * @param start the start condition of the range
       * @param end the end condition of the range
       * @param score $scoreParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Range = {
       *   import coll.AggregationFramework.AtlasSearch.Range
       *
       *   Range(
       *     path = "age" -> Seq("duration"),
       *     start = Range.greaterThan(4),
       *     end = Range.lessThanOrEqual(10)
       *   )
       * }
       * }}}
       */
      def apply(
        path: SearchString,
        start: Start,
        end: End,
        score: Option[Score] = None): Range =
        new Range(path, start, end, score)

      // ---

      /**
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection) =
       *   coll.AggregationFramework.AtlasSearch.Range.greaterThan(2)
       * }}}
       */
      def greaterThan[T](value: T)(implicit w: Writer[T]): Start =
        new Start("gt", w.write(value))

      /**
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection) =
       *   coll.AggregationFramework.AtlasSearch.Range.greaterThanOrEqual(1.23D)
       * }}}
       */
      def greaterThanOrEqual[T](value: T)(implicit w: Writer[T]): Start =
        new Start("gte", w.write(value))

      /**
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection) =
       *   coll.AggregationFramework.AtlasSearch.Range.lessThan(10)
       * }}}
       */
      def lessThan[T](value: T)(implicit w: Writer[T]): End =
        new End("lt", w.write(value))

      /**
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection) =
       *   coll.AggregationFramework.AtlasSearch.Range.lessThanOrEqual(7.5D)
       * }}}
       */
      def lessThanOrEqual[T](value: T)(implicit w: Writer[T]): End =
        new End("lte", w.write(value))

      final class Start private[api] (
        val tpe: String,
        val value: pack.Value) {
        @inline override def hashCode: Int = toString.hashCode

        override def equals(that: Any): Boolean = that match {
          case other: this.type =>
            this.tpe == other.tpe

          case _ =>
            false
        }

        override def toString = tpe
      }

      final class End private[api] (
        val tpe: String,
        val value: pack.Value) {
        @inline override def hashCode: Int = toString.hashCode

        override def equals(that: Any): Boolean = that match {
          case other: this.type =>
            this.tpe == other.tpe

          case _ =>
            false
        }

        override def toString = tpe
      }

      sealed trait Writer[T] {
        /** Returns the serialized representation for the input value `v` */
        def write(v: T): pack.Value
      }

      object Writer {
        import java.time.Instant

        def apply[T](f: T => pack.Value): Writer[T] = new FunctionalWriter(f)

        implicit def intWriter: Writer[Int] = Writer[Int](builder.int)

        implicit def floatWriter: Writer[Float] = Writer[Float] { f =>
          builder.double(f.toDouble)
        }

        implicit def longWriter: Writer[Long] = Writer[Long](builder.long)

        implicit def doubleWriter: Writer[Double] = Writer[Double](builder.double)

        implicit def instantWriter: Writer[Instant] =
          Writer[Instant] { i => builder.dateTime(i.toEpochMilli) }

        // ---

        private final class FunctionalWriter[T](
          f: T => pack.Value) extends Writer[T] {

          def write(v: T): pack.Value = f(v)
        }
      }
    }

    /**
     * '''EXPERIMENTAL:''' See [[Wildcard$]]
     *
     * @param query $queryParam
     * @param path $pathParam
     * @param score $scoreParam
     */
    final class Wildcard private[api] (
      val query: SearchString,
      val path: SearchString,
      val allowAnalyzedField: Boolean,
      val score: Option[Score]) extends Operator {

      val name = "wildcard"

      def document: pack.Document = {
        import builder.{ elementProducer => elm }

        val elms = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elm("query", query.value),
          elm("path", path.value),
          elm("allowAnalyzedField", builder.boolean(allowAnalyzedField)))

        score.foreach { sc =>
          elms += elm("score", sc.document)
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple4(query, path, allowAnalyzedField, score)

      @inline override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchWildcard${tupled.toString}"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/wildcard/#wildcard-ref Wildcard]] operator for Atlas Search */
    object Wildcard {
      /**
       * @param query $queryParam
       * @param path $pathParam
       * @param allowAnalyzedField the value to use to calculate scores of Atlas  result documents
       * @param score $scoreParam
       */
      def apply(
        query: SearchString,
        path: SearchString,
        allowAnalyzedField: Boolean = false,
        score: Option[Score] = None): Wildcard =
        new Wildcard(query, path, allowAnalyzedField, score)
    }

    /** '''EXPERIMENTAL:''' See [[Regex$]] */
    final class Regex private[api] (
      val query: SearchString,
      val path: SearchString,
      val allowAnalyzedField: Boolean,
      val score: Option[Score]) extends Operator {

      val name = "regex"

      def document: pack.Document = {
        import builder.{ elementProducer => elm }

        val elms = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elm("query", query.value),
          elm("path", path.value),
          elm("allowAnalyzedField", builder.boolean(allowAnalyzedField)))

        score.foreach { sc =>
          elms += elm("score", sc.document)
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple4(query, path, allowAnalyzedField, score)

      @inline override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchRegex${tupled.toString}"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/regex/#regex-ref Regex]] operator for Atlas Search */
    object Regex {
      /**
       * @param query $queryParam
       * @param path $pathParam
       * @param allowAnalyzedField the value to use to calculate scores of Atlas  result documents
       * @param score $scoreParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(
       *   coll: BSONCollection): coll.AggregationFramework.AtlasSearch.Regex =
       *   coll.AggregationFramework.AtlasSearch.Regex(
       *     query = "foo.*",
       *     path = "field1" // or "field1" -> Seq("field2", ...)
       *   )
       * }}}
       */
      def apply(
        query: SearchString,
        path: SearchString,
        allowAnalyzedField: Boolean = false,
        score: Option[Score] = None): Regex =
        new Regex(query, path, allowAnalyzedField, score)
    }

    /**
     * '''EXPERIMENTAL:''' See [[QueryString$]].
     *
     * @param defaultPath the indexed field to search by default
     * @param query the [[https://docs.atlas.mongodb.com/reference/atlas-search/queryString/#description query string]]
     * @param score $scoreParam
     */
    final class QueryString private[api] (
      val defaultPath: String,
      val query: String,
      val score: Option[Score]) extends Operator {

      val name = "queryString"

      def document: pack.Document = {
        import builder.{ elementProducer => elm, string }

        val elms = Seq.newBuilder[pack.ElementProducer] ++= Seq(
          elm("defaultPath", string(defaultPath)),
          elm("query", string(query)))

        score.foreach { sc =>
          elms += elm("score", sc.document)
        }

        builder.document(elms.result())
      }

      private lazy val tupled = Tuple3(defaultPath, query, score)

      override def hashCode: Int = tupled.hashCode

      override def equals(that: Any): Boolean = that match {
        case other: this.type =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def toString = s"SearchQueryString${tupled.toString}"
    }

    /** '''EXPERIMENTAL:''' [[https://docs.atlas.mongodb.com/reference/atlas-search/query-syntax/#query-syntax-ref Query string]] operator for Atlas Search. */
    object QueryString {
      /**
       * @param defaultPath the indexed field to search by default
       * @param query the [[https://docs.atlas.mongodb.com/reference/atlas-search/queryString/#description query string]]
       * @param score $scoreParam
       *
       * {{{
       * import reactivemongo.api.bson.collection.BSONCollection
       *
       * def prepare(coll: BSONCollection): coll.AggregationFramework.AtlasSearch.QueryString = coll.AggregationFramework.AtlasSearch.QueryString(
       *   defaultPath = "title",
       *   query = "Rocky AND (IV OR 4 OR Four)"
       * )
       * }}}
       */
      def apply(
        defaultPath: String,
        query: String,
        score: Option[Score] = None): QueryString =
        new QueryString(defaultPath, query, score)
    }
  }
}
