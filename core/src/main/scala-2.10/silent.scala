package com.github.ghik.silencer

/** No-op annotation as silencer not available for 2.10 */
final class silent(msg: String = "") extends scala.annotation.StaticAnnotation
