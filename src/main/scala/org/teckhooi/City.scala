package org.teckhooi

case class City(name: String, max: Double, min: Double, total: Double, count: Int) {
  override def toString = f"$name=$min/${total / count}%.1f/$max"
}