package api

/**
 * Created by andryw on 17/03/15.
 */
class CpuInt (qty:Int){
  def cpus(arch:String): Cpu ={
    Cpu(qty,arch)
  }
}

