import api.Machine
import api.Implicits._

/**
 * Created by andryw on 17/03/15.
 */
object main {
  def main(args: Array[String]) {
    val machine = new Machine having(8 cpus "64bit") with_os "Linux" at_spot_price 30 with_price_strategy{(x) => 1.15*x}
    println(machine cpu)
    println(machine os)
    println(machine spotPrice)
    println(machine threshold)

  }
}
