package com.github.ghik.silencer
import annotation.unused

class silent(@unused s: String = "") extends scala.annotation.StaticAnnotation
