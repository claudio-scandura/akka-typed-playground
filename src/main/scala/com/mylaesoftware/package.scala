package com.mylaesoftware

import scala.concurrent.duration.FiniteDuration

package object utils {
  def busy(duration: FiniteDuration): Unit = pi(System.nanoTime() + duration.toNanos)
  private def pi(endNanos: Long) = {
    def gregoryLeibnitz(n: Long) = 4.0 * (1 - (n % 2) * 2) / (n * 2 + 1)
    var n                        = 0
    var acc                      = BigDecimal(0.0)
    while (System.nanoTime() < endNanos) {
      acc += gregoryLeibnitz(n)
      n += 1
    }
    acc
  }
}
