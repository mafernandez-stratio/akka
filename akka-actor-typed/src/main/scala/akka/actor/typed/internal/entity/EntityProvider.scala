/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.entity

import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future

import akka.actor.ActorRefProvider
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Entity
import akka.actor.typed.EntityEnvelope
import akka.actor.typed.EntityRef
import akka.actor.typed.EntityTypeKey
import akka.actor.typed.EntityTypeKeyImpl
import akka.actor.typed.internal.InternalRecipientRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.annotation.InternalApi
import akka.pattern.StatusReply
import akka.util.ByteString
import akka.util.Timeout

/**
 * Marker interface to use with dynamic access
 *
 * INTERNAL API
 */
@InternalApi
private[akka] trait EntityProvider {

  def initEntity[M, E](entity: Entity[M, E]): ActorRef[E]

  def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M]
}

private[akka] class LocalEntityProvider(system: ActorSystem[_]) extends EntityProvider {

  private val typeNames: ConcurrentHashMap[String, String] = new ConcurrentHashMap
  private val entityManagers: ConcurrentHashMap[String, ActorRef[_]] = new ConcurrentHashMap
  private val hasExtractor: ConcurrentHashMap[String, Boolean] = new ConcurrentHashMap()

  override def initEntity[M, E](entity: Entity[M, E]): ActorRef[E] = {

    val typeKey = entity.typeKey
    val messageClassName = typeKey.asInstanceOf[EntityTypeKeyImpl[M]].messageClassName

    typeNames.putIfAbsent(typeKey.name, messageClassName) match {
      case existingMessageClassName: String if messageClassName != existingMessageClassName =>
        throw new IllegalArgumentException(s"[${typeKey.name}] already initialized for [$existingMessageClassName]")
      case _ => ()
    }

    hasExtractor.putIfAbsent(typeKey.name, entity.messageExtractor.isDefined)

    val entityManager =
      entityManagers.computeIfAbsent(
        entity.typeKey.name,
        new java.util.function.Function[String, ActorRef[_]] {
          override def apply(t: String): ActorRef[_] =
            system.systemActorOf(
              EntityManager.behavior(entity),
              "entity-manager-" + URLEncoder.encode(entity.typeKey.name, ByteString.UTF_8))
        })
    entityManager.asInstanceOf[ActorRef[E]]
  }

  override def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M] = {
    val managerRef = entityManagers.get(typeKey.name).asInstanceOf[ActorRef[Any]]
    EntityRefImpl[M](entityId, typeKey, managerRef)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] case class EntityRefImpl[M](
      entityId: String,
      typeKey: EntityTypeKey[M],
      entityManagerRef: ActorRef[Any])
      extends EntityRef[M]
      with InternalRecipientRef[M] {

    /**
     * Converts incoming message to what the EntityManager expects.
     * It may be returned as is or wrapped in an EntityEnvelope.
     */
    private def toEntityMessage(msg: M): Any =
      if (hasExtractor.get(typeKey.name)) msg
      else EntityEnvelope(entityId, msg)

    override def tell(msg: M): Unit =
      entityManagerRef ! toEntityMessage(msg)

    override def ask[Res](func: ActorRef[Res] => M)(implicit timeout: Timeout): Future[Res] = {
      implicit val scheduler = system.scheduler
      entityManagerRef.ask { replyTo: ActorRef[Res] =>
        toEntityMessage(func(replyTo))
      }
    }

    override def askWithStatus[Res](func: ActorRef[StatusReply[Res]] => M)(implicit timeout: Timeout): Future[Res] =
      StatusReply.flattenStatusFuture(ask[StatusReply[Res]](func))

    override def provider: ActorRefProvider =
      entityManagerRef.asInstanceOf[InternalRecipientRef[_]].provider

    override def isTerminated: Boolean =
      entityManagerRef.asInstanceOf[InternalRecipientRef[_]].isTerminated

    override def hashCode(): Int =
      // 3 and 5 chosen as primes which are +/- 1 from a power-of-two
      ((entityId.hashCode * 3) + typeKey.hashCode) * 5

    override def equals(other: Any): Boolean =
      other match {
        case eri: EntityRefImpl[_] => (eri.entityId == entityId) && (eri.typeKey == typeKey)
        case _                     => false
      }

    override def toString: String = s"EntityRef($typeKey, $entityId)"

  }
}
