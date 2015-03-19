package api

import dsl.Implicits._
/**
 * Created by andryw on 17/03/15.
 */
case object train {
  def on_dataset(path: String) : WithPath= {
    new WithPath(path)
  }

}
