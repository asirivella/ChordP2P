package com.dos.chord

import java.math.BigInteger
import java.security.MessageDigest
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.serialization.Serialization
import scala.concurrent.Await
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.Breaks._
/**
 * @author Ananda Kishore Sirivella
 */

sealed trait ChordMessage
case object Start extends ChordMessage
case object CreateRing extends ChordMessage
case class JoinRing (helperNode: ActorRef) extends ChordMessage
case class FindClosestPrecFinger (peerId: BigInteger) extends ChordMessage
case class FindPredecessor (peerId: BigInteger) extends ChordMessage
case class FindSuccessor (peerId: BigInteger) extends ChordMessage
case object ReturnSuccessor extends ChordMessage
case object ReturnPredecessor extends ChordMessage
case class FindClosestPrecFingerReply(actorRef: ActorRef, actorId: BigInteger, successorRef: ActorRef, successorId: BigInteger) extends ChordMessage
case class FindSuccessorReply(actorRef: ActorRef, actorId: BigInteger) extends ChordMessage
case class FindPredecessorReply(actorRef: ActorRef) extends ChordMessage
case class ReturnSuccessorReply(actorRef: ActorRef, successorId: BigInteger) extends ChordMessage
case class ReturnPredecessorReply(actorRef: ActorRef) extends ChordMessage
case class Join(actorRef: ActorRef) extends ChordMessage
case class InitFingerTable(actorRef: ActorRef) extends ChordMessage
case object UpdateOthers extends ChordMessage
case class UpdateFingerTable(actorRef: ActorRef, actorId: BigInteger, fingerI: Integer) extends ChordMessage
case class UpdatePredecessor(actorRef: ActorRef, actorId: BigInteger) extends ChordMessage


object ChordImpl extends App {

    if(args.isEmpty || args(0).isEmpty() || args(1).isEmpty()){
        println("Enter arguments for NumberOfNodes NumberOfRequests")
    }else{
        InitializeAndInitiate(args(0).toInt, args(1).toInt)
    }

    def InitializeAndInitiate(numOfNodes: Int, numOfRequests: Int){

        val system = ActorSystem("ChordImplSystem")
                val master = system.actorOf(Props(new Master(numOfNodes, numOfRequests)), name = "Master")
                master ! Start
    }
}

class Master(numofNodes: Int, numOfRequests:Int) extends Actor {

    var AllActors = new ArrayBuffer[ActorRef]()
            var identifierBits: Int = 16
            var helperNode: ActorRef = null
            final val AWAIT_DURATION = Duration(50000, MILLISECONDS)
            var nodeJoinCount:Integer = 0

            def receive = {

            case Start =>
            for(i <- 0 until numofNodes){
                AllActors += context.actorOf(Props(new PeerNode(numOfRequests, identifierBits, i)), name = "PeerNode" + (i + 1))
                        if(!AllActors.isEmpty){
                            if(AllActors.length == 1){

                                helperNode = AllActors(0)
                                        implicit val timeout = Timeout(AWAIT_DURATION)
                                        val Future = AllActors(0) ? CreateRing
                                                val result = Await.result(Future, timeout.duration).asInstanceOf[String]
                                                        Future onSuccess {
                                                        case "NodeJoinCompleted" =>
                                                        println("Node join completed for Node " + i)
                                                        nodeJoinCount += 1

                                                        if(nodeJoinCount == numofNodes)
                                                            println("All " +numofNodes + " nodes have joined the system")
                                }
                            }else {
                                implicit val timeout = Timeout(AWAIT_DURATION)
                                        val Future = AllActors(i) ? Join(helperNode)
                                                val result = Await.result(Future,timeout.duration).asInstanceOf[String]
                                                        Future onSuccess {
                                                        case "NodeJoinCompleted" =>
                                                        println("Node join completed for Node " + i)
                                                        nodeJoinCount += 1

                                                        if(nodeJoinCount == numofNodes)
                                                            println("All " +numofNodes + " nodes have joined the system")

                                }
                            }                                    
                        }    
            }    

            case _ =>
            println("Undefined Operation for the Master Node")
            }
}

class finger{

    var startId: BigInteger = null
            var endId: BigInteger = null
            var successor: ActorRef = null
            var successorId: BigInteger = null

}


class PeerNode(numOfRequests: Int, identifierBits: Int, index: Int) extends Actor{

    final var HASH_TYPE: String = "SHA1"
            final val BOTH_OPEN: String = "OpenIntervals"
            final val LEFT_OPEN_RIGHT_CLOSED: String = "RightClosedInterval"
            final val RIGHT_OPEN_LEFT_CLOSED: String ="LeftClosedInterval"  
            final val AWAIT_DURATION = Duration(30000, MILLISECONDS)
            val byteLimit: Integer = 2
            val identifier: String = Serialization.serializedActorPath(self)             
            var peerId: BigInteger = hashIdentifier(identifier, byteLimit)
            var moduloValue: BigInteger = BigInteger.valueOf(math.pow(2, identifierBits.toDouble).toLong)
            var fingerTable: ArrayList[finger] = new ArrayList[finger]()
            var successor: ActorRef = null
            var successorId: BigInteger = null
            var predecessorId: BigInteger = null
            var predecessor: ActorRef = null

            println("Identifier is " + identifier + " and peer Id is " + peerId)

            def receive = {

            case CreateRing =>
            StartAndInitializeFingerTable()
            successor = self
            predecessor = self
            successorId = peerId
            predecessorId = peerId
            for(i <- 0 until fingerTable.size()){
                fingerTable.get(i).successor = self
                        fingerTable.get(i).successorId = peerId
            }
            printFingerTable()
            sender ! "NodeJoinCompleted"

            case JoinRing(helperNode: ActorRef) =>
            StartAndInitializeFingerTable()

            case FindSuccessor(searchId: BigInteger) =>
            val(replyRef, replyId) = returnSuccessor(searchId)
            sender ! FindSuccessorReply(replyRef, replyId)

            case FindPredecessor(searchId: BigInteger) =>
            sender ! FindPredecessorReply(returnPredecessor(searchId))

            case FindClosestPrecFinger(searchId: BigInteger) =>
            val (actorRef, actorId, successorRef, successorId) = returnClosestPrecFinger(searchId)
            sender ! FindClosestPrecFingerReply(actorRef, actorId, successorRef, successorId)

            case ReturnSuccessor =>
            sender ! ReturnSuccessorReply(successor, successorId)

            case ReturnPredecessor =>
            sender ! ReturnPredecessorReply(predecessor)

            case Join(actorRef: ActorRef) =>
            var mainSender: ActorRef = sender
            StartAndInitializeFingerTable()
            println("Sender for Join is " + sender)
            implicit var timeout = Timeout(AWAIT_DURATION)
            val Future = self ? InitFingerTable(actorRef)
                    val result = Await.result(Future, timeout.duration).asInstanceOf[String]
                            Future onSuccess {

                            case "FingerTableInitializationCompleted" =>
                            println("Finger Table Initialization Completed for the node")
                            val updateFuture = self ? UpdateOthers
                                    val result = Await.result(updateFuture, AWAIT_DURATION).asInstanceOf[String]
                                            updateFuture onSuccess {

                                            case "FingerTableUpdated" =>
                                            mainSender ! "NodeJoinCompleted"
                            }
            }

            case UpdateFingerTable(actorRef: ActorRef, actorId: BigInteger, fingerI: Integer) =>

            println(" Remote Invocation of Update Finger Table")

            var actualSender: ActorRef = sender
            if(belongsTo(peerId, fingerTable.get(fingerI.toInt).successorId, actorId, BOTH_OPEN)){
                println("Its predecessor is " + predecessor + " and self is " + self)
                println(" Compared " + peerId +" " +fingerTable.get(fingerI.toInt).successorId + " " +actorId)
                fingerTable.get(fingerI.toInt).successor = actorRef
                fingerTable.get(fingerI.toInt).successorId = actorId
                if(fingerI == 0){
                    successor = actorRef
                            successorId = actorId
                }
                implicit val timeout = Timeout(AWAIT_DURATION)
                        val Future = predecessor ? UpdateFingerTable(actorRef, actorId, fingerI)
                                val result = Await.result(Future, timeout.duration).asInstanceOf[String]
                                        Future onSuccess{

                                        case "FingerTableUpdated" =>
                                        println("Update completed for updateFingerTable routine with actorId " +actorId + " and fingerI " + fingerI)
                                        actualSender ! "FingerTableUpdated"

                }
            }else{
                actualSender ! "FingerTableUpdated"  
            }


            case InitFingerTable(actorRef: ActorRef) =>

            println(" Invoked")
            var mainSender: ActorRef = sender
            implicit val timeout = Timeout(AWAIT_DURATION)
            println("Finding Successor for searchId " + fingerTable.get(0).startId)
            val Future = actorRef ? FindSuccessor(fingerTable.get(0).startId)
                    val result = Await.result(Future, timeout.duration).asInstanceOf[FindSuccessorReply]
                            Future onSuccess {

                            case FindSuccessorReply(successorRef: ActorRef, successorRefId: BigInteger) =>
                            println("Got ReturnSuccessorReply , successorId " + successorRefId)
                            fingerTable.get(0).successor = successorRef
                            fingerTable.get(0).successorId = successorRefId
                            successor = fingerTable.get(0).successor
                            successorId = fingerTable.get(0).successorId

                            updatePredecessorNotifySuccessor(actorRef)

                            // Check this
                            for( i <- 0 to fingerTable.size() - 2){
                                println("Loop iterations Count")
                                if(belongsTo(peerId, fingerTable.get(i).successorId, fingerTable.get(i + 1).startId,RIGHT_OPEN_LEFT_CLOSED)){
                                    fingerTable.get(i + 1).successor = fingerTable.get(i).successor
                                            fingerTable.get(i + 1).successorId = fingerTable.get(i).successorId
                                }else{
                                    val futureOne: Future[FindSuccessorReply] = ask(actorRef,FindSuccessor(fingerTable.get(i + 1).startId)).mapTo[FindSuccessorReply]
                                            futureOne.onSuccess {

                                            case FindSuccessorReply(actorRef: ActorRef, actorRefId: BigInteger) =>
                                            fingerTable.get(i + 1).successor = actorRef
                                            fingerTable.get(i + 1).successorId = actorRefId

                                }
                                } 
                            }
                            mainSender ! "FingerTableInitializationCompleted"
            }

            case UpdatePredecessor(actorRef: ActorRef, actorId: BigInteger) =>
            predecessor = actorRef
            predecessorId = actorId

            case UpdateOthers =>
            var senderRef: ActorRef = sender
            var comparisonId: BigInteger = null
            var updateCounter: Integer = 0

            for(i <- 0 to fingerTable.size() - 1){

                println(" Total Loop Iterations")

                comparisonId = peerId.subtract(BigInteger.valueOf(math.pow(2,i).toLong))
                if(comparisonId.compareTo(new BigInteger("0")) < 0)
                    comparisonId = comparisonId.add(moduloValue)

                    var nodeP: ActorRef = null
                    implicit val futureTimeout = Timeout(AWAIT_DURATION)
                    val PredecessorFuture = self ? FindPredecessor(comparisonId)
                            val result = Await.result(PredecessorFuture, futureTimeout.duration).asInstanceOf[FindPredecessorReply]
                                    PredecessorFuture onSuccess {

                                    case FindPredecessorReply(predecessorRef) =>
                                    println(" Predecessor found" + predecessorRef)
                                    nodeP = predecessorRef
                                    val Future = nodeP ? UpdateFingerTable(self, peerId, i)
                                            val result = Await.result(Future, futureTimeout.duration).asInstanceOf[String]
                                                    Future onSuccess {
                                                    case "FingerTableUpdated" =>
                                                    println(" One Success")
                                                    updateCounter += 1
                                                    if(updateCounter == fingerTable.size()){
                                                        println(" Completed all fingers update")
                                                        senderRef ! "FingerTableUpdated"
                                                    }

                                    }
                    }                 

            }



            case _ =>
            println("Undefined Operation for the Peer Node")
            }

            def printFingerTable() = {
                for(i <- 0 until fingerTable.size()){
                    println("Successor ID = " + fingerTable.get(i).successorId + " startId = " + fingerTable.get(i).startId + " endId = " + fingerTable.get(i).endId)
                }
            }

            def hashIdentifier(identifier: String, byteLimit: Int): BigInteger =  {

                var mDigest = MessageDigest.getInstance(HASH_TYPE)
                        var sb = new StringBuffer();
                mDigest.reset()
                mDigest.update(identifier.getBytes)
                var digestResult = mDigest.digest()
                for (i <- 0 until byteLimit) {
                    sb.append(Integer.toString((digestResult(i) & 0xff) + 0x100, 16).substring(1))
                }
                var bigInteger = new BigInteger(sb.toString(), 16);
                return bigInteger

            }

            // Defer usage based on changes 
            def InitializeFingerTable(actorRef: ActorRef) = {

                implicit val timeout = Timeout(AWAIT_DURATION)
                        println("Finding Successor for searchId " + fingerTable.get(0).startId)
                        val Future = actorRef ? FindSuccessor(fingerTable.get(0).startId)
                                val result = Await.result(Future, timeout.duration).asInstanceOf[FindSuccessorReply]
                                        Future onSuccess {

                                        case FindSuccessorReply(successorRef: ActorRef, successorRefId: BigInteger) =>
                                        println("Got ReturnSuccessorReply , successorId " + successorRefId)
                                        fingerTable.get(0).successor = successorRef
                                        fingerTable.get(0).successorId = successorRefId
                                        successor = fingerTable.get(0).successor
                                        successorId = fingerTable.get(0).successorId

                                        updatePredecessorNotifySuccessor(actorRef)

                                        // Check this
                                        for( i <- 0 to fingerTable.size() - 2){
                                            if(belongsTo(peerId, fingerTable.get(i).successorId, fingerTable.get(i + 1).startId,RIGHT_OPEN_LEFT_CLOSED)){
                                                fingerTable.get(i + 1).successor = fingerTable.get(i).successor
                                                        fingerTable.get(i + 1).successorId = fingerTable.get(i).successorId
                                            }else{
                                                val futureOne: Future[FindSuccessorReply] = ask(actorRef,FindSuccessor(fingerTable.get(i + 1).startId)).mapTo[FindSuccessorReply]
                                                        futureOne.onSuccess {

                                                        case FindSuccessorReply(actorRef: ActorRef, actorRefId: BigInteger) =>
                                                        fingerTable.get(i + 1).successor = actorRef
                                                        fingerTable.get(i + 1).successorId = actorRefId

                                            }
                                            Thread.sleep(500)
                                            } 
                                        }
                                        Thread.sleep(5000)
                }
                println("out of InitializeFingerTable")
            }

            def updatePredecessorNotifySuccessor(actorRef: ActorRef) = {

                println("Enter routine updatePredecessorNotifySuccessor")

                implicit val timeout = Timeout(AWAIT_DURATION)
                val Future = successor ? ReturnPredecessor
                        val result = Await.result(Future, timeout.duration).asInstanceOf[ReturnPredecessorReply]
                                /*    val futureOne: Future[ReturnPredecessorReply] = ask(successor, ReturnPredecessor).mapTo[ReturnPredecessorReply]*/
                                Future onSuccess {
                                case ReturnPredecessorReply(predecessorRef: ActorRef) =>
                                println(predecessorRef)
                                predecessor = predecessorRef
                                successor ! UpdatePredecessor(self, peerId)

                                case _ =>
                                println("Failure during requesting predecessor through futures operation from INITIALIZE FINGER TABLE")
                }
                Thread.sleep(200)
                println("Out of updatePredecessorNotifySuccessor")

            }

            def updateOthers(senderRef: ActorRef) = {


                var comparisonId: BigInteger = null
                        var updateCounter: Integer = 0

                        for(i <- 0 to fingerTable.size() - 1){

                            comparisonId = peerId.subtract(BigInteger.valueOf(math.pow(2,i).toLong))
                                    if(comparisonId.compareTo(new BigInteger("0")) < 0)
                                        comparisonId = comparisonId.add(moduloValue)

                                        var nodeP: ActorRef = null
                                        implicit val futureTimeout = Timeout(AWAIT_DURATION)
                                        val PredecessorFuture = self ? FindPredecessor(comparisonId)
                                                val result = Await.result(PredecessorFuture, futureTimeout.duration).asInstanceOf[FindPredecessorReply]
                                                        PredecessorFuture onSuccess {

                                                        case FindPredecessorReply(predecessorRef) =>
                                                        nodeP = predecessorRef
                                                        val Future = nodeP ? UpdateFingerTable(self, peerId, i)
                                                                val result = Await.result(Future, futureTimeout.duration).asInstanceOf[String]
                                                                        Future onSuccess {
                                                                        case "FingerTableUpdated" =>
                                                                        println(" One Success")
                                                                        updateCounter += 1
                                                                        if(updateCounter == fingerTable.size()){
                                                                            println(" Completed all fingers update")
                                                                            senderRef ! "FingerTableUpdated"
                                                                        }

                                                        }
                                        }                                    

                        }
            }


            def returnSuccessor(searchId: BigInteger) = {

                println("Entered routine returnSuccessor")

                if(peerId.equals(searchId))
                {
                    (successor, successorId)    
                }else{
                    var refHolder = returnPredecessor(searchId)
                            if(self.equals(refHolder)){
                                (successor, successorId)
                            }else{
                                var refId: BigInteger = null
                                        implicit val timeout = Timeout(AWAIT_DURATION)
                                        val Future = refHolder ? ReturnSuccessor
                                                val result = Await.result(Future, timeout.duration).asInstanceOf[ReturnSuccessorReply]
                                                        /*  val future: Future[ReturnSuccessorReply] = ask(refHolder, ReturnSuccessor).mapTo[ReturnSuccessorReply]*/
                                                        Future onSuccess {

                                                        case ReturnSuccessorReply(actorRef: ActorRef, actorId: BigInteger) =>     
                                                        println("Success")
                                                        refHolder = actorRef
                                                        refId = actorId
                                                        println("completed returnSuccessor routine with refId " + actorId)
                                                        case _ =>
                                                        println("Unexpected error during execution of Future returnSuccessor")

                            }
                    (refHolder, refId)
                            }

                }

            }

            def updateFingerTable(actorRef: ActorRef, actorId: BigInteger, fingerI: Integer) = {

                var actualSender: ActorRef = sender
                        if(belongsTo(peerId, fingerTable.get(fingerI.toInt).successorId, actorId, RIGHT_OPEN_LEFT_CLOSED)){
                            println("Its predecessor is " + predecessor + " and self is " + self)
                            fingerTable.get(fingerI.toInt).successor = actorRef
                            fingerTable.get(fingerI.toInt).successorId = actorId
                            implicit val timeout = Timeout(AWAIT_DURATION)
                            val Future = predecessor ? UpdateFingerTable(actorRef, actorId, fingerI)
                                    val result = Await.result(Future, timeout.duration).asInstanceOf[String]
                                            Future onSuccess{

                                            case "FingerTableUpdated" =>
                                            println("Update completed for updateFingerTable routine with actorId " +actorId + " and fingerI " + fingerI)
                                            actualSender ! "FingerTableUpdated"

                            }
                        }
            }

            def returnPredecessor(searchId: BigInteger): ActorRef = {

                println("Entered routine returnPredecessor")
                var refHolder: ActorRef = self
                var successorHolder: ActorRef = successor
                var refId: BigInteger = peerId
                var successorHolderId: BigInteger = successorId
                var breakCondition: Boolean = false



                while(!belongsTo(refId, successorHolderId, searchId,LEFT_OPEN_RIGHT_CLOSED) && !breakCondition)
                {
                    if(refHolder.equals(self)){
                        println("Invoking Method call")
                        val (actorRef, actorId, successorRef, successorRefId) = returnClosestPrecFinger(searchId)
                        refHolder = actorRef
                        refId = actorId
                        successorHolder = successorRef
                        successorHolderId = successorRefId
                        if(actorRef.equals(self)){
                            breakCondition = true
                                    Thread.sleep(2200)
                        }

                    }else{
                        implicit val timeout = Timeout(AWAIT_DURATION)
                                val Future = refHolder ? FindClosestPrecFinger(searchId)
                                        val result = Await.result(Future,timeout.duration).asInstanceOf[FindClosestPrecFingerReply]
                                                Future onSuccess {

                                                case FindClosestPrecFingerReply(actorRef: ActorRef, actorId: BigInteger, successorRef: ActorRef, successorRefId: BigInteger) =>
                                                refHolder = actorRef
                                                refId = actorId
                                                successorHolder = successorRef
                                                successorHolderId = successorRefId
                                                if(actorRef.equals(refHolder)){
                                                    breakCondition = true
                                                }

                                                case _ =>
                                                println("Unexpectiong error during futures execution for returnPredecesso")
                        }

                    }
                }  
                println(" Out of predecessor method")
                return refHolder
            } 

            def returnClosestPrecFinger(searchId: BigInteger) = {


                var sentInformation: Boolean = false
                        var nodeRef: ActorRef = null
                        var nodeId: BigInteger = null
                        var nodeSuccessorRef: ActorRef = null
                        var nodeSuccessorId: BigInteger = null
                        var i: Integer = 0
                        var breakCondition: Boolean = false

                        for(j <- 0 to fingerTable.size() - 1){
                            i = fingerTable.size() - 1 - j

                                    if(belongsTo(peerId, searchId, fingerTable.get(i).successorId,BOTH_OPEN) && !breakCondition)
                                    {
                                        nodeRef = fingerTable.get(i).successor
                                                nodeId = fingerTable.get(i).successorId
                                                implicit val timeout = Timeout(AWAIT_DURATION)
                                                val Future = nodeRef ? ReturnSuccessor
                                                        val result = Await.result(Future, timeout.duration).asInstanceOf[ReturnSuccessorReply]
                                                                Future onSuccess {

                                                                case ReturnSuccessorReply(actorRef: ActorRef, actorId: BigInteger) =>
                                                                nodeSuccessorRef = actorRef
                                                                nodeSuccessorId = actorId
                                                                sentInformation = true
                                                                breakCondition = true
                                        }
                                    }
                        }
            if(!sentInformation)
                (self, peerId,successor, successorId)
                else
                    (nodeRef, nodeId, nodeSuccessorRef, nodeSuccessorId)
            }

            def returnModuloId (id: BigInteger): BigInteger = {
                return id.mod(moduloValue)
            }

            def belongsTo(id1: BigInteger, id2: BigInteger, compareId: BigInteger, comparisonType: String): Boolean = {

                var belongsTo: Boolean = false

                        comparisonType match {

                        case BOTH_OPEN =>
                        belongsTo = (compareId.compareTo(id1) >0 && compareId.compareTo(id2) < 0) || 
                        (((compareId.compareTo(id1) >0 && compareId.compareTo(id2.add(moduloValue)) < 0) ||
                                (compareId.add(moduloValue).compareTo(id1) >0 && compareId.add(moduloValue).compareTo(id2.add(moduloValue)) < 0)) && (id1.compareTo(id2) > 0)) ||
                                (id1.equals(id2) && (!compareId.equals(id1)))

                        case LEFT_OPEN_RIGHT_CLOSED =>
                        belongsTo = (compareId.compareTo(id1) >0 && compareId.compareTo(id2) <= 0) ||
                        (((compareId.compareTo(id1) >0 && compareId.compareTo(id2.add(moduloValue)) <= 0) ||
                                (compareId.add(moduloValue).compareTo(id1) >0 && compareId.add(moduloValue).compareTo(id2.add(moduloValue)) <= 0)) && (id1.compareTo(id2) > 0)) ||
                                (id1.equals(id2))

                        case RIGHT_OPEN_LEFT_CLOSED =>
                        belongsTo = (compareId.compareTo(id1) >=0 && compareId.compareTo(id2) < 0) ||
                        (((compareId.compareTo(id1) >=0 && compareId.compareTo(id2.add(moduloValue)) < 0) ||
                                (compareId.add(moduloValue).compareTo(id1) >=0 && compareId.add(moduloValue).compareTo(id2.add(moduloValue)) < 0)) && (id1.compareTo(id2) > 0)) ||
                                (id1.equals(id2))
            }
            return belongsTo
            }

            def StartAndInitializeFingerTable() = {

                for(i <- 1 to identifierBits){
                    var fingerEntry: finger = new finger()
                fingerEntry.startId = returnModuloId(peerId.add(BigInteger.valueOf(math.pow(2, i-1).toLong)))
                fingerEntry.endId = returnModuloId(peerId.add(BigInteger.valueOf(math.pow(2, i).toLong)))
                fingerTable.add(fingerEntry)  
                }
            }
}