package com.mylaesoftware

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter.{TypedSchedulerOps, _}
import akka.persistence.journal.inmem.InmemJournal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.mylaesoftware.BottleEntity._
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.util.Random

object TestConfig {
  val inMemoryJournalConfig = ConfigFactory.parseString(s"""
                                                           |akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
                                                           |akka.persistence.journal.inmem.test-serialization = on
                                                           |akka.actor.allow-java-serialization = on
                                                           |""".stripMargin)
}

class BottleEntitySpec extends ScalaTestWithActorTestKit(TestConfig.inMemoryJournalConfig) with AnyWordSpecLike {
  //  implicit val dispatcher = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(64))
  val logger = LoggerFactory.getLogger(this.getClass)

  object WithExpectedEvents {
    def none(testBody: => Unit): Unit = apply(List.empty[BottleEvent]: _*)(testBody)

    def apply(expectedEvents: BottleEvent*)(testBody: => Unit): Unit = {
      val eventProbe = createTestProbe[InmemJournal.Operation]()
      system.eventStream ! EventStream.Subscribe(eventProbe.ref)
      testBody
      expectedEvents.foreach(eventProbe.expectMessageType[InmemJournal.Write].event shouldBe _)
      eventProbe.expectNoMessage()
    }
  }

  "BottleBehaviour" should {

    "Open bottle" in WithExpectedEvents(TapToggled) {
      val probe = createTestProbe[CommandResponse]()

      val underTest = spawn(BottleEntity())

      underTest ! Open(probe.ref)

      probe.expectMessageType[CommandResponse].bottleState should have(
        'isClosed (false)
      )

      //idempotence check
      underTest ! Open(probe.ref)

    }

    "Pour amount" in {
      val amountToPour = 0.3
      WithExpectedEvents(TapToggled, PouredAmount(amountToPour)) {
        val probe = createTestProbe[CommandResponse]()

        val underTest = spawn(BottleEntity())

        underTest ! Open(probe.ref)

        val amountLeft = probe.expectMessageType[CommandResponse].bottleState.amountLeft

        underTest ! Pour(amountToPour, probe.ref)

        probe.expectMessageType[CommandResponse].bottleState should have(
          'amountLeft (amountLeft - amountToPour)
        )
      }
    }

    "Close bottle" in WithExpectedEvents(TapToggled, TapToggled) {
      val probe = createTestProbe[CommandResponse]()

      val underTest = spawn(BottleEntity())

      underTest ! Open(probe.ref)

      probe.expectMessageType[CommandResponse]

      underTest ! Close(probe.ref)

      probe.expectMessageType[CommandResponse].bottleState should have(
        'isClosed (true)
      )
    }

    s"reject ${classOf[Pour].getSimpleName} command if bottle is closed" in WithExpectedEvents.none {
      val probe = createTestProbe[CommandResponse]()

      val underTest = spawn(BottleEntity())

      underTest ! Pour(0.2, probe.ref)

      probe.expectMessageType[UnexpectedCommand]
    }

    "return empty bottle" in {
      val amount = 10.0
      WithExpectedEvents(TapToggled, PouredAmount(amount)) {
        val probe = createTestProbe[CommandResponse]()

        val underTest = spawn(BottleEntity(capacity = amount))

        underTest ! Open(probe.ref)

        probe.expectMessageType[CommandResponse].bottleState.amountLeft shouldBe amount

        underTest ! Pour(amount, probe.ref)

        probe.expectMessageType[CommandResponse].bottleState should have(
          'isEmpty (true),
          'amountLeft (0.0)
        )

        //idempotence check
        underTest ! Pour(amount, probe.ref)

      }
    }

  }

  "Load testing" should {
    def runLoadTest(
        numOfEntities: Int,
        flow: Flow[(ActorRef[BottleCommand], ActorRef[CommandResponse] => BottleCommand), Done, NotUsed]
    ): Unit = {

      import scala.concurrent.duration._

      implicit val timeout = Timeout(5.seconds)

      implicit val mat = ActorMaterializer()(system.toClassic)
      val capacity     = 1.0

      // Allocate entities
      val entities = (1 to numOfEntities).map { _ =>
        val entityId = UUID.randomUUID()
        (entityId, spawn(BottleEntity(entityId, capacity)))
      }.toMap

      // Create commands
      val commands = Random
        .shuffle(entities.keys.toList.map { id =>
          List((id, ref => Open(ref)), (id, ref => Pour(capacity, ref)))
        })
        .flatten

      val start = Instant.now()
      val responsesF = Source
        .fromIterator(() => commands.iterator)
        .map {
          case (id, command) => (entities(id), command)
        }
        .via(flow)
        .runWith(Sink.seq)

      val responses = Await.result(responsesF, 5.minutes)

      val end           = Instant.now()
      val elapsedMillis = ChronoUnit.MILLIS.between(start, end)
      //Verify that eventually all commands are processed correctly
      responses should have size (2 * numOfEntities)
      responses foreach {
        _ should not be a[UnexpectedCommand]
      }

      logger.info(
        s"Processed ${numOfEntities * 2} commands in $elapsedMillis milliseconds. ${numOfEntities * 2 / (elapsedMillis / 1000)} commands/second"
      )

    }

    "Run three load tests" in {
      // Naive flow where sequentially each command is sent to the entity and then another async task is called - all within the same mapAsync
      val simpleFlow = Flow[(ActorRef[BottleCommand], ActorRef[CommandResponse] => BottleCommand)].mapAsync(1) {
        case (entity, command) => askCommandToEntity(entity, command).flatMap(_ => doOtherExpensiveTask())
      }

      // As the simpleFlow but the second async task is run in a subsequent flow which is separated using async boundaries and thus enabling pipelining
      val flowWithPipelining = Flow[(ActorRef[BottleCommand], ActorRef[CommandResponse] => BottleCommand)]
        .mapAsync(1) {
          case (entity, command) => askCommandToEntity(entity, command)
        }
        .async
        .via(Flow[CommandResponse].mapAsync(1)(_ => doOtherExpensiveTask()))
        .async

      // As flowWithPipelining but in the first flow, where commands are sent to entity the parallelism in the mapAsync is increased
      val flowWithHigherParallelismAndPipelining =
        Flow[(ActorRef[BottleCommand], ActorRef[CommandResponse] => BottleCommand)]
          .mapAsync(Runtime.getRuntime.availableProcessors()) {
            case (entity, command) => askCommandToEntity(entity, command)
          }
          .async
          .via(Flow[CommandResponse].mapAsync(1)(_ => doOtherExpensiveTask()))
          .async

      runLoadTest(100, simpleFlow)
      runLoadTest(100, flowWithPipelining)
      runLoadTest(100, flowWithHigherParallelismAndPipelining)
    }

  }

  private def askCommandToEntity(entity: ActorRef[BottleCommand], command: ActorRef[CommandResponse] => BottleCommand) =
    withRetries(0.milliseconds)(() => entity.ask[CommandResponse](command(_)))

  private def doOtherExpensiveTask(): Future[Done] = Future {
    utils.busy(20.milliseconds)
    Done
  }

  // Run the async call after the given initDelay and recurse when the result of the call is an UnexpectedCommand, meaning that the
  // entity rejected the command
  def withRetries(initDelay: FiniteDuration)(call: () => Future[CommandResponse]): Future[CommandResponse] =
    akka.pattern.after(initDelay, system.scheduler.toClassic)(call()).flatMap {
      case _: UnexpectedCommand =>
        logger.info("UnexpectedCommand response received... retrying")
        withRetries(2.milliseconds)(call)
      case resp => Future.successful(resp)
    }

}
