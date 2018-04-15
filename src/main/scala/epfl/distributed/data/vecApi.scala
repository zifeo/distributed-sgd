import math._

trait VecAPI {

  def elementWiseOp(other: Vec, op: (Float, Float) => Float): Vec

  def mapValues(op: Float => Float): Vec

  def foldLeft[B](init: B)(op: (B, Float) => B): B

  def +(other: Vec): Vec = elementWiseOp(other, _ + _)

  def +(scalar: Float): Vec = mapValues(_ + scalar)

  def -(other: Vec): Vec = elementWiseOp(other, _ - _)

  def -(scalar: Float): Vec = mapValues(_ - scalar)

  def *(other: Vec): Vec = elementWiseOp(other, _ * _)

  def *(scalar: Float): Vec = mapValues(_ * scalar)

  def /(scalar: Float): Vec = mapValues(_ / scalar)

  def pow(scalar: Float): Vec = mapValues(math.pow(_, scalar).toFloat)

  def sum: Float = foldLeft(0.toFloat)(_ + _)

  def normSquared: Float = math.pow(foldLeft(0.toFloat)(_ + _), 2).toFloat
  def norm: Float        = (math.pow(normSquared, 0.5)).toFloat

  def dot(other: Vec): Float = {
    (this * other).sum
  }

}
