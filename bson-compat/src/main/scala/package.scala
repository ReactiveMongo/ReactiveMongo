package reactivemongo.api.bson

/**
 * Implicit conversions for handler & values types
 * between `reactivemongo.bson` and `reactivemongo.api.bson` .
 *
 * {{{
 * import reactivemongo.api.bson.compat._
 * }}}
 *
 * For more specific imports, see [[ValueConverters]]
 * and [[HandlerConverters]] .
 */
package object compat extends ValueConverters with HandlerConverters
