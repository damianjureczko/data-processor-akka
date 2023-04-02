package dataprocessor.akka

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar


abstract class BaseAkkaSpec extends AnyWordSpec
  with BeforeAndAfterAll
  with Matchers
  with MockitoSugar {

  val testKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()
}
