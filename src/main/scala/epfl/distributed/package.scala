package epfl

import java.util.logging.LogManager

import com.typesafe.config.ConfigFactory
import epfl.distributed.Utils.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

package object distributed {

  LogManager.getLogManager.readConfiguration()

  val config: Config = ConfigFactory.load().as[Config]("distributed")

}
