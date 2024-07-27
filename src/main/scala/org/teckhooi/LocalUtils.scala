package org.teckhooi

import scala.annotation.tailrec

object LocalUtils:
  
  def indexOf(s: String, c: Char): Int =
    @tailrec
    def _indexOf(ndx: Int, len: Int): Int =
      if (ndx >= len) -1
      else if (s(ndx) == c) ndx
      else _indexOf(ndx + 1, len)

    if (s.isBlank) -1
    else _indexOf(0, s.length)
