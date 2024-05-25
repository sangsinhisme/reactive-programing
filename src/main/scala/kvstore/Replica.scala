package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import akka.actor.Terminated

import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply
  case class Check()

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with akka.actor.ActorLogging{
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  val persister = context.actorOf(persistenceProps)

  context.system.scheduler.scheduleWithFixedDelay(100.millisecond, 100.millisecond, self, Check())

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var ackSn = Map.empty[Long, (ActorRef, Replicate, Int)]
  var ackRp = Map.empty[Long, (ActorRef, Set[ActorRef], Int)]

  override val preStart: Unit = {
    arbiter ! Join
  }

  override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  def receive: Receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key: String, value: String, id: Long) =>
      ackSn += (id -> (sender, Replicate(key, Some(value), id), 0))
      persister ! Persist(key, Some(value), id)
      if(replicators.nonEmpty) {
        replicators.foreach(repl => repl ! Replicate(key, Some(value), id))
        ackRp += (id ->(sender, replicators, 0))
      }
      kv += (key -> value)
    case Remove(key: String, id: Long) =>
      ackSn += (id -> (sender, Replicate(key, None, id), 0))
      persister ! Persist(key, None, id)
      if(replicators.nonEmpty) {
        replicators.foreach(repl => repl ! Replicate(key, None, id))
        ackRp += (id ->(sender, replicators, 0))
      }
      kv -= key
    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv get key, id)
    case Persisted(key, seq) =>
      ackSn get seq match {
        case Some(x) =>
          ackSn -= seq
          if(ackRp.nonEmpty) {
            ackRp get seq match {
              case Some(x) =>
              case None =>
                x._1 ! OperationAck(seq)
            }
          } else {
            x._1 ! OperationAck(seq)
          }
        case None =>
      }
    case Replicated(key, id) =>
      ackRp get id match {
        case Some(x) =>
          ackRp += (id -> (x._1, x._2 - sender, x._3))
        case None =>
      }
      ackRp = ackRp.filter { case (seq, t) =>
        if(t._2 isEmpty) {
          ackSn get seq match {
            case Some(x) =>
            case None =>
              t._1 ! OperationAck(seq)
          }
        }
        t._2.nonEmpty
      }
    case Replicas(replicas) =>
      val deleted = secondaries.keySet &~ replicas.tail
      deleted.foreach( r  => {
        secondaries get r match {
          case Some(repl) =>
            replicators -= repl
            secondaries -= repl
            context.stop(repl)
            ackRp = ackRp.map { case (seq, t) =>
              seq ->(t._1, t._2 - repl, t._3)
            }.filter { case (seq, t) =>
              if (t._2 isEmpty) ackSn get seq match {
                case Some(x) =>
                case None =>
                  t._1 ! OperationAck(seq)
              }
              t._2.nonEmpty
            }
          case None =>
        }
      })
      replicas.tail.foreach( r  => {
        secondaries get r match {
          case Some(_) =>
          case None =>
            val repl = context.actorOf(Replicator.props(r))
            secondaries += r -> repl
            replicators += repl
            kv.zipWithIndex.foreach{case ((id, value), i) => repl ! Replicate(id, Some(value), -kv.size + i)}
        }
      })
    case Check() =>
      ackSn = ackSn.map { case (seq, t) =>
        persister ! Persist(t._2.key, t._2.valueOption, seq)
        seq ->(t._1, t._2, t._3 + 1)
      }.filter { case (seq, t) =>
        if (t._3 > 10) {
          t._1 ! OperationFailed(seq)
        }
        t._3 <= 10
      }
      ackRp = ackRp.map { case (seq, t) =>
        seq ->(t._1, t._2, t._3 + 1)
      }.filter { case (seq, t) =>
        if (t._3 > 10) {
          t._1 ! OperationFailed(seq)
        }
        t._3 <= 10
      }
  }

  private var rSeq:Long = 0


  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv get key, id)
    case Snapshot(key, valueOption, seq) =>
      if(rSeq == seq) {
        rSeq += 1
        ackSn += (seq -> (sender, Replicate(key, valueOption, seq), 0))
        persister ! Persist(key, valueOption, seq)
        valueOption match {
          case Some(value) =>
            kv += (key -> value)
          case None =>
            kv -= key
        }
      } else if(rSeq > seq) {
        sender ! SnapshotAck(key, seq)
      }
    case Persisted(key, seq) =>
      ackSn get seq match {
        case Some(x) =>
          ackSn -= seq
          x._1 ! SnapshotAck(key, seq)
        case None =>
      }
    case Check() =>
      ackSn = ackSn.map { case (seq, t) =>
        persister ! Persist(t._2.key, t._2.valueOption, seq)
        seq ->(t._1, t._2, t._3 + 1)
      }.filter { case (seq, t) =>
        if (t._3 >= 10) {
          t._1 ! OperationFailed(seq)
        }
        t._3 < 10
      }
  }
}