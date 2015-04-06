package api

import dsl.Implicits._

case object train {
  def on_dataset(path: String) : WithPath= {
    new WithPath(path)
  }

}
