package com.lightbend.akka.sample
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }
import scala.io.StdIn
import java.io._
import java.util.Calendar
import java.util.Calendar
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import scala.concurrent.duration._

object PATH{
  val path = System.getProperty("user.dir");
  val dir = "/actorDirs/"
}

object DatabaseNode {
  def props(actor: ActorRef) = Props(new DatabaseNode(actor))

  //tell the actor that he is master now -> use only once, while creating new ActorSystem
  case object InitMaster

  //tell the actor to do the log
  case object MakeLog

  //tell the actor to start updating database procedure
  case object SendUpdate

  //sending changes to all nodes
  final case class UpdateFile(toAppend: String)

  //claiming crown from master
  case object GetMasterReference

  //node who is master
  case object WhoIsMaster

  //reply for GetMaster/WhoIs request, if it returns reference to sender then he becomes new master, otherwise it will return reference to node that he believes is a current master
  final case class TellWhoIsMaster(actor: ActorRef)
}

class DatabaseNode(var masterReference: ActorRef) extends Actor with ActorLogging {
  import DatabaseNode._

  val name: String = self.path.name
  val src: String = PATH.path + PATH.dir + name
  var logging_message: String = ""
  var processing_message: Boolean = false

  override def preStart = {
    new File(src).mkdir()
  }

  def copy(content: String){
    val file = new File(src+"/log")
    file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file,true))
    bw.write(content)
    bw.close()
	}

  def makeLog{
    logging_message += Calendar.getInstance.getTime + " " + name + "\n"
  }

  def receive = {
      case InitMaster => {
        log.info("Initializing Master")
        masterReference = self
      }
      case MakeLog => makeLog

      case WhoIsMaster => sender ! TellWhoIsMaster(masterReference)

      case UpdateFile(content: String) =>{
        log.info("Commiting changes to file")
        copy(content)
      }

      case TellWhoIsMaster(ref: ActorRef) => {
        if( masterReference != ref && ref == self){
          self ! SendUpdate
          masterReference = ref
          log.info("received crown from " + sender.path.name)
        }else{
          masterReference = ref
          log.info(ref.path.name+ " became new master")
          if(processing_message)
            self ! SendUpdate
        }
      }
      case GetMasterReference =>{
        if(masterReference == self){
          log.info("giving crown to " + sender.path.name)
          context.system.actorSelection("/user/*") ! TellWhoIsMaster(sender)

        }else{
          log.info(sender.path.name + " asked wrong actor for crown")
          sender ! TellWhoIsMaster(masterReference)
        }
      }
      case SendUpdate =>  {
        if(masterReference == self){
          log.info("Requesting to commit changes")
          context.system.actorSelection("/user/*") ! UpdateFile(logging_message)
          logging_message = ""
          processing_message = false
        }else{
          log.info("Asking " + masterReference.path.name +" for crown")
          masterReference ! GetMasterReference
          processing_message = true
        }
      }
  }
}


object AkkaQuickstart extends App {
  import DatabaseNode._

  val system: ActorSystem = ActorSystem("database")

  import system.dispatcher

  //Frequency of sampling
  val cancellable = system.scheduler.schedule(0 milliseconds, 500 milliseconds){
    system.actorSelection("/user/*") ! MakeLog
  }

  val actor: ActorRef =
    system.actorOf(DatabaseNode.props(null), "actor")


  val actor1: ActorRef =
    system.actorOf(DatabaseNode.props(actor), "actor1")

  val actor2: ActorRef =
    system.actorOf(DatabaseNode.props(actor1), "actor2")

  val actor3: ActorRef =
    system.actorOf(DatabaseNode.props(actor), "actor3")


  actor ! InitMaster

  Thread.sleep(2000)

  actor2 ! SendUpdate

  Thread.sleep(2000)

  actor1 ! SendUpdate

  StdIn.readLine()

  system.terminate()

}
