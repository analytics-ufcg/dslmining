package api

/**
 * Created by andryw on 17/03/15.
 */
object Implicits {
  implicit def cpuInt(i: Int) = new CpuInt(i)


}
