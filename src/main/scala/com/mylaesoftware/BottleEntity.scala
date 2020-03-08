package com.mylaesoftware

import java.util.UUID

import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}


/**
 * Represents a Bottle entity with a capacity and can either be Open or Closed. When Open the entity accepts
 * Pour command until there is any volume left, after that Pour commands are rejected just as they are when sent
 * to a Closed bottle.
 */
object BottleEntity {

  val DefaultCapacity = 1.5

  sealed trait BottleCommand {
    def replyTo: ActorRef[CommandResponse]
  }

  case class Open(replyTo: ActorRef[CommandResponse]) extends BottleCommand

  case class Close(replyTo: ActorRef[CommandResponse]) extends BottleCommand

  case class Pour(amount: Double, replyTo: ActorRef[CommandResponse]) extends BottleCommand

  trait CommandResponse {
    def bottleState: BottleState
  }

  object CommandResponse {
    def apply(state: BottleState): CommandResponse = new CommandResponse {
      override def bottleState: BottleState = state
    }
  }

  case class UnexpectedCommand(bottleState: BottleState) extends CommandResponse

  sealed trait BottleEvent

  case object TapToggled extends BottleEvent

  case class PouredAmount(amount: Double) extends BottleEvent

  final case class BottleState(amountLeft: Double, isClosed: Boolean) {
    def isEmpty: Boolean = amountLeft <= 0.0
  }

  def apply(id: UUID = UUID.randomUUID(), capacity: Double = DefaultCapacity): Behavior[BottleCommand] = EventSourcedBehavior[BottleCommand, BottleEvent, BottleState](
    persistenceId = PersistenceId.ofUniqueId(id.toString),
    emptyState = BottleState(capacity, isClosed = true),
    commandHandler = handleCommands,
    eventHandler = handleEvents
  )

  val handleCommands: (BottleState, BottleCommand) => Effect[BottleEvent, BottleState] = {
    // Unexpected transitions need re-attempting at a later stage
    case (s@BottleState(_, true), Pour(_, replyTo)) => Effect.reply(replyTo)(UnexpectedCommand(s))

    // Expected transitions
    case (BottleState(_, true), Open(replyTo)) => toggleTapAndReplyTo(replyTo)
    case (BottleState(_, false), Close(replyTo)) => toggleTapAndReplyTo(replyTo)
    case (s@BottleState(_, false), Pour(amount, replyTo)) if !s.isEmpty => Effect.persist(PouredAmount(amount)).thenReply(replyTo)(CommandResponse(_))

    // Anything else return current state right away (command idempotence)
    case (state, command: BottleCommand) => Effect.reply(command.replyTo)(CommandResponse(state))

  }

  private def toggleTapAndReplyTo(replyTo: ActorRef[CommandResponse]) =
    Effect.persist[BottleEvent, BottleState](TapToggled).thenReply[CommandResponse](replyTo)(CommandResponse(_))

  val handleEvents: (BottleState, BottleEvent) => BottleState = {
    case (s@BottleState(_, isCurrentlyClosed), TapToggled) => s.copy(isClosed = !isCurrentlyClosed)
    case (s@BottleState(amountLeft, false), PouredAmount(amount)) => s.copy(amountLeft = amountLeft - amount)
  }

}
