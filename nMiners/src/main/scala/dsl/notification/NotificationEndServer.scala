package dsl.notification

import org.apache.camel.builder.RouteBuilder
import org.apache.camel.main.Main
import org.apache.camel.{Exchange, Processor}
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer

object NotificationEndServer {
  var started = false

  val main = initServer

  def initServer = {
    val m = new Main
    m.enableHangupSupport
    m.addRouteBuilder(Router)
    m
  }

  val jobsToNotify = ArrayBuffer[String]()

  def addJobToNotification(jobId: String) = jobsToNotify += jobId

  def start = {
    if (!started) {
      main.start
      started = true
    }
  }

  def stop = {
    if (started) {
      main.stop
      started = false
    }
  }

  def notifyJob(jobId: String, status: String) = {
    println("\n\n\n\n\n\n\n\n\n\n\n\n")
    println(s"$jobId is finished with status $status")
    println("\n\n\n\n\n\n\n\n\n\n\n\n")
  }

  def configureServer (conf: Configuration) = {
    conf.set("job.end.notification.url", "http://localhost:9090/notification?jobId=$jobId&status=$jobStatus")
    conf.setInt("job.end.retry.attempts", 3)
    conf.setInt("job.end.retry.interval", 1000)
  }

  def handler = (exchange: Exchange) => {
    val jobId = exchange.getIn().getHeader("jobId").toString
    val status = exchange.getIn().getHeader("status").toString

    println("\n\n\n\n\n\n\n\n\n\n\n\n")
    println("job ended = " + jobId)
    println("\n\n\n\n\n\n\n\n\n\n\n\n")
    jobsToNotify.find(_ equals jobId) match {
      case Some(id) => notifyJob(id, status)
      case None => {}
    }
  }
}

object Router extends RouteBuilder {
  override def configure() = {
    from("jetty:http://localhost:9090/notification?jobId={id}&status={status}").process(NotificationEndServer.handler)
  }

  private implicit def function2Processor(f: (Exchange) => Unit): Processor = new Processor {
    override def process(exchange: Exchange) = f(exchange)
  }
}