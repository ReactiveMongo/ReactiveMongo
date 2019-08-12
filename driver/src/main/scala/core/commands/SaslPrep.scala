package reactivemongo.core.commands

import java.nio.CharBuffer
import java.text.Normalizer

/**
 * SASLPrep utility (inspired by https://github.com/mongodb/mongo-java-driver/blob/master/driver-core/src/main/com/mongodb/internal/authentication/SaslPrep.java).
 *
 * @define rfc3454Url https://tools.ietf.org/html/rfc3454
 * @define rfc4013Url https://tools.ietf.org/html/rfc4013
 */
private[core] object SaslPrep {
  /**
   * Return the SASLPrep-canonicalised version of the given `str` for use as a stored string.
   * This implements the SASLPrep algorithm defined in [[${rfc4013Url} RFC 4013]].
   *
   * @param str the string to canonicalise
   * @return the canonicalised string
   * @see [[${rfc3454Url}#section-7 RFC 3454, Section 7]] for discussion of what a stored string is
   */
  def apply(str: String, allowUnassigned: Boolean): Either[String, String] = {
    // 1. Map

    val sz1 = str.size
    val buf = CharBuffer.allocate(sz1)

    @annotation.tailrec def mapChars(i: Int): Unit = {
      if (i < sz1) {
        val orig = str.charAt(i)
        val ch = {
          if (nonAsciiSpace(orig)) ' '
          else orig
        }

        if (mappedToNothing(ch)) {
          mapChars(i + 1)
        } else {
          buf.put(ch)

          mapChars(i + 1)
        }
      }
    }

    mapChars(i = 0)

    // 2. Normalize

    val normalized = Normalizer.normalize(
      { buf.rewind(); buf }, Normalizer.Form.NFKC)

    val sz2 = normalized.size

    @annotation.tailrec
    def check(
      i: Int,
      containsRandALCat: Boolean,
      containsLCat: Boolean,
      initialRandALCat: Boolean): Either[String, String] = {
      if (i == sz2) {
        Right(normalized)
      } else if (containsRandALCat && containsLCat) {
        Left("Contains both RandALCat characters and LCat characters")
      } else {
        val codepoint = normalized.codePointAt(i)

        // 3. Prohibit
        if (prohibited(codepoint)) {
          Left(s"Prohibited character at position $i")
        } else if (!allowUnassigned && !Character.isDefined(codepoint)) {
          Left(s"Character at position $i is unassigned")
        } else {
          // 4. Check bidi

          val dir = Character.getDirectionality(codepoint)
          val isRandALcat = (
            dir == Character.DIRECTIONALITY_RIGHT_TO_LEFT
            || dir == Character.DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC)

          val updRandALCat = initialRandALCat | (i == 0 && isRandALcat)
          val ni = i + Character.charCount(codepoint)

          if (updRandALCat && ni >= sz2 && !isRandALcat) {
            Left("First character is RandALCat, but last character is not");
          } else {
            check(
              i = ni,
              containsRandALCat = containsRandALCat | isRandALcat,
              containsLCat = containsLCat | (
                dir == Character.DIRECTIONALITY_LEFT_TO_RIGHT),
              initialRandALCat = updRandALCat)
          }
        }
      }
    }

    check(
      i = 0,
      containsRandALCat = false,
      containsLCat = false,
      initialRandALCat = false)

  }

  // ---

  /**
   * Return true if the given `codepoint` is a prohibited character
   * as defined by [[${rfc4013Url}#section-2.3 RFC 4013, Section 2.3]].
   */
  @inline def prohibited(codepoint: Int): Boolean = {
    val ch = codepoint.toChar

    nonAsciiSpace(ch) ||
      asciiControl(ch) ||
      nonAsciiControl(codepoint) ||
      privateUse(codepoint) ||
      nonCharacterCodePoint(codepoint) ||
      surrogateCodePoint(codepoint) ||
      inappropriateForPlainText(codepoint) ||
      inappropriateForCanonical(codepoint) ||
      changeDisplayProperties(codepoint) ||
      tagging(codepoint);
  }

  /**
   * Return true if the given `codepoint` is a tagging character
   * as defined by [[${rfc3454Url}#appendix-C.9 RFC 3454, Appendix C.9]].
   */
  @inline def tagging(codepoint: Int): Boolean =
    codepoint == 0xE0001 || 0xE0020 <= codepoint && codepoint <= 0xE007F

  /**
   * Return true if the given `codepoint` is change display properties
   * or deprecated characters as defined by
   * [[${rfc3454Url}#appendix-C.8 RFC 3454, Appendix C.8]].
   */
  @inline private def changeDisplayProperties(codepoint: Int): Boolean = (
    codepoint == 0x0340
    || codepoint == 0x0341
    || codepoint == 0x200E
    || codepoint == 0x200F
    || codepoint == 0x202A
    || codepoint == 0x202B
    || codepoint == 0x202C
    || codepoint == 0x202D
    || codepoint == 0x202E
    || codepoint == 0x206A
    || codepoint == 0x206B
    || codepoint == 0x206C
    || codepoint == 0x206D
    || codepoint == 0x206E
    || codepoint == 0x206F)

  /**
   * Return true if the given `codepoint` is inappropriate
   * for canonical representation characters as defined by
   * [[${rfc3454Url}#appendix-C.7 RFC 3454, Appendix C.7]].
   */
  @inline private def inappropriateForCanonical(codepoint: Int): Boolean =
    0x2FF0 <= codepoint && codepoint <= 0x2FFB

  /**
   * Return true if the given `codepoint` is inappropriate
   * for plain text characters as defined by
   * [[${rfc3454Url}#appendix-C.6 RFC 3454, Appendix C.6]].
   */
  @inline private def inappropriateForPlainText(codepoint: Int): Boolean = (
    codepoint == 0xFFF9
    || codepoint == 0xFFFA
    || codepoint == 0xFFFB
    || codepoint == 0xFFFC
    || codepoint == 0xFFFD)

  /**
   * Return true if the given `codepoint` is a surrogate code point
   * as defined by [[${rfc3454Url}#appendix-C.5 RFC 3454, Appendix C.5]].
   */
  @inline private def surrogateCodePoint(codepoint: Int): Boolean =
    0xD800 <= codepoint && codepoint <= 0xDFFF

  /**
   * Return true if the given `codepoint` is a non-character code point
   * as defined by [[${rfc3454Url}#appendix-C.4 RFC 3454, Appendix C.4]].
   */
  @inline private def nonCharacterCodePoint(codepoint: Int): Boolean = (
    0xFDD0 <= codepoint && codepoint <= 0xFDEF
    || 0xFFFE <= codepoint && codepoint <= 0xFFFF
    || 0x1FFFE <= codepoint && codepoint <= 0x1FFFF
    || 0x2FFFE <= codepoint && codepoint <= 0x2FFFF
    || 0x3FFFE <= codepoint && codepoint <= 0x3FFFF
    || 0x4FFFE <= codepoint && codepoint <= 0x4FFFF
    || 0x5FFFE <= codepoint && codepoint <= 0x5FFFF
    || 0x6FFFE <= codepoint && codepoint <= 0x6FFFF
    || 0x7FFFE <= codepoint && codepoint <= 0x7FFFF
    || 0x8FFFE <= codepoint && codepoint <= 0x8FFFF
    || 0x9FFFE <= codepoint && codepoint <= 0x9FFFF
    || 0xAFFFE <= codepoint && codepoint <= 0xAFFFF
    || 0xBFFFE <= codepoint && codepoint <= 0xBFFFF
    || 0xCFFFE <= codepoint && codepoint <= 0xCFFFF
    || 0xDFFFE <= codepoint && codepoint <= 0xDFFFF
    || 0xEFFFE <= codepoint && codepoint <= 0xEFFFF
    || 0xFFFFE <= codepoint && codepoint <= 0xFFFFF
    || 0x10FFFE <= codepoint && codepoint <= 0x10FFFF)

  /**
   * Return true if the given `codepoint` is a private use character
   * as defined by [[${rfc3454Url}#appendix-C.3 RFC 3454, Appendix C.3]].
   */
  @inline private def privateUse(codepoint: Int): Boolean = (
    0xE000 <= codepoint && codepoint <= 0xF8FF
    || 0xF000 <= codepoint && codepoint <= 0xFFFFD
    || 0x100000 <= codepoint && codepoint <= 0x10FFFD)

  /**
   * Return true if the given {@code ch} is a non-ASCII control character
   * as defined by [[${rfc3454Url}#appendix-C.2.2 RFC 3454, Appendix C.2.2]].
   */
  @inline private def nonAsciiControl(codepoint: Int): Boolean = (
    0x0080 <= codepoint && codepoint <= 0x009F
    || codepoint == 0x06DD
    || codepoint == 0x070F
    || codepoint == 0x180E
    || codepoint == 0x200C
    || codepoint == 0x200D
    || codepoint == 0x2028
    || codepoint == 0x2029
    || codepoint == 0x2060
    || codepoint == 0x2061
    || codepoint == 0x2062
    || codepoint == 0x2063
    || 0x206A <= codepoint && codepoint <= 0x206F
    || codepoint == 0xFEFF
    || 0xFFF9 <= codepoint && codepoint <= 0xFFFC
    || 0x1D173 <= codepoint && codepoint <= 0x1D17A)

  /**
   * Return true if the given {@code ch} is an ASCII control character
   * as defined by [[${rfc3454Url}#appendix-C.2.1 RFC 3454, Appendix C.2.1]].
   */
  @inline private def asciiControl(ch: Char): Boolean =
    ch <= '\u001F' || ch == '\u007F'

  /**
   * Return true if the given `ch` is a non-ASCII space character, as defined
   * by [[${rfc3454Url}#appendix-C.1.2 RFC 3454, Appendix C.1.2]].
   */
  @inline private def nonAsciiSpace(ch: Char): Boolean = (
    ch == '\u00A0'
    || ch == '\u1680'
    || '\u2000' <= ch && ch <= '\u200B'
    || ch == '\u202F'
    || ch == '\u205F'
    || ch == '\u3000')

  /**
   * Return true if the given `ch` is a "commonly mapped to nothing" character
   * as defined by [[${rfc3454Url}#appendix-B.1 RFC 3454, Appendix B.1]].
   */
  private def mappedToNothing(ch: Char): Boolean = (
    ch == '\u00AD'
    || ch == '\u034F'
    || ch == '\u1806'
    || ch == '\u180B'
    || ch == '\u180C'
    || ch == '\u180D'
    || ch == '\u200B'
    || ch == '\u200C'
    || ch == '\u200D'
    || ch == '\u2060'
    || '\uFE00' <= ch && ch <= '\uFE0F'
    || ch == '\uFEFF')

}
