package api

/**
 * Created by andryw on 17/03/15.
 */
class Machine {

  var cpu:Cpu = null
  var os:String = null
  var spotPrice:Int = 0
  var threshold:Double = 0

  def having(_cpu:Cpu): Machine ={
    cpu = _cpu
    this
  }

  def with_os(_os:String):Machine= {
    os = _os
    this
  }

  def at_spot_price(_spotPrice: Int)={
    spotPrice = _spotPrice
    this
  }

  def with_price_strategy(_pricing: (Int)=>Double)={
    threshold = _pricing(spotPrice)
    this
  }
}
