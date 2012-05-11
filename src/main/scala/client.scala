package org.asyncmongo

import akka.actor.{Actor, ActorRef}
import akka.actor.ActorSystem
import akka.actor.Props
import protocol._
import actors._
import akka.actor.ActorContext
import akka.routing.RoundRobinRouter
import java.net.InetSocketAddress
import akka.routing.Broadcast



object Client {
  import protocol.messages._
  import akka.util.Timeout
  import akka.dispatch.Future
  import akka.util.duration._

  implicit val timeout = Timeout(5 seconds)

  def test = {
    val connection = MongoConnection( List( "localhost" -> 27017 ) )
    //connection.ask(WritableMessage(KillCursors(Set(900))))
    def p(name: String) :Either[Throwable, ReadReply] => Unit = {
      case Right(reply) => {
        println(name + ": \n" + bson.DefaultBSONIterator.pretty(bson.DefaultBSONIterator(reply.documents)))
      }
      case Left(error) => throw error
    }
    connection.ask(messages.IsMaster.makeWritableMessage("plugin")).onComplete(p("IsMaster"))
    connection.ask(messages.Status.makeWritableMessage("plugin")).onComplete(p("Status"))
    connection.ask(messages.ReplStatus.makeWritableMessage("plugin")).onComplete(p("ReplStatus"))
    //connection.ask(WritableMessage(KillCursors(Set(900)), Array[Byte](0)))
    /*connection send list
    connection send insert
    connection ask(insert, GetLastError())
    val future = connection ask list
    println(future)
    future.onComplete({
      case Right(reply) => {
        println("future is complete! got this response: " + reply)
        connection.stop
      }
      case Left(error) => throw error
    })*/
  }
  /*val system = ActorSystem("mongodb")
  def makeDBSystem(nodes: List[(String, Int)]) = system.actorOf(Props(new MongoDBSystem(nodes)), name="db1")
  
  def test = {
    val act = makeDBSystem(List("localhost" -> 27017))
    act ! "toto"
    val elector = system.actorFor(act.path.child("elector"))
    println("elector is " + elector)
    act ! "toto"
    elector ! LookForMaster
    elector ! "hello"
  }*/
  import java.io._
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._

  def insert = {
    val factory = new BsonFactory()
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeStringField("name", "Jack")
    gen.writeEndObject()
    gen.close()
    
    val random = (new java.util.Random()).nextInt(Integer.MAX_VALUE)
    println("generated list request #" + random)

    WritableMessage(random, 0, Insert(0, "plugin.acoll"), baos.toByteArray)
  }

  def list = {
    val factory = new BsonFactory()
 
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeEndObject()
    gen.close()

    val random = (new java.util.Random()).nextInt(Integer.MAX_VALUE)
    println("generated list request #" + random)

    WritableMessage(random, 0, Query(0, "plugin.acoll", 0, 0), baos.toByteArray)
  }
}
