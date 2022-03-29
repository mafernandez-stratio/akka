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
              URLEncoder.encode(entity.typeKey.name, ByteString.UTF_8) + "-EntityManager")
        })
    entityManager.asInstanceOf[ActorRef[E]]
  }

  override def entityRefFor[M](key: EntityTypeKey[M], id: String): EntityRef[M] = {

    val managerRef = entityManagers.get(key.name).asInstanceOf[ActorRef[Any]]

    new EntityRef[M] with InternalRecipientRef[M] {

      override val entityId: String = id
      override val typeKey: EntityTypeKey[M] = key

      override def tell(msg: M): Unit =
        if (hasExtractor.get(key.name)) {
          managerRef ! msg
        } else {
          managerRef ! EntityEnvelope(entityId, msg)
        }

      override def ask[Res](f: ActorRef[Res] => M)(implicit timeout: Timeout): Future[Res] = {
        implicit val scheduler = system.scheduler
        managerRef.ask { replyTo: ActorRef[Res] =>
          f(replyTo)
        }
      }

      override def askWithStatus[Res](f: ActorRef[StatusReply[Res]] => M)(implicit timeout: Timeout): Future[Res] = ???

      override def provider: ActorRefProvider =
        managerRef.asInstanceOf[InternalRecipientRef[_]].provider

      override def isTerminated: Boolean =
        managerRef.asInstanceOf[InternalRecipientRef[_]].isTerminated
    }
  }
}
