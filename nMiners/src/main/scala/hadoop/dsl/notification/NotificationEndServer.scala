package hadoop.dsl.notification

import org.apache.camel.builder.RouteBuilder
import org.apache.camel.main.Main
import org.apache.camel.{Exchange, Processor}
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer

object NotificationEndServer {
  var started = false

  val main = initServer

  val jobsToNotify = ArrayBuffer[String]()

  val notifiersFunctions = ArrayBuffer[(String, String) => Unit]()

  def initServer = {
    val m = new Main
    m.enableHangupSupport
    m.addRouteBuilder(Router)
    m
  }

  def addNotificationFunction(func: (String, String) => Unit) = notifiersFunctions += func


  def addJobToNotification(jobId: String) = jobsToNotify += jobId

  def addJobsToNotification(jobsId: List[String]) = jobsToNotify ++= jobsId

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
    notifiersFunctions foreach {_ apply (jobId, status)}
  }

  def configureServer(conf: Configuration) = {
    conf.set("job.end.notification.url", "http://localhost:9090/notification?jobId=$jobId&status=$jobStatus")
    conf.setInt("job.end.retry.attempts", 3)
    conf.setInt("job.end.retry.interval", 1000)
  }

  def handler = (exchange: Exchange) => {
    val jobId = exchange.getIn().getHeader("jobId").toString
    val status = exchange.getIn().getHeader("status").toString

    jobsToNotify.find(_ equals jobId) match {
      case Some(id) => notifyJob(id, status)
      case None => {}
    }
  }
}

object Router extends RouteBuilder {
  override def configure() = {
    from("jetty:http://localhost:9090/notification?jobId={id}&status={status}").
      process(NotificationEndServer.handler)
  }

  private implicit def function2Processor(f: (Exchange) => Unit): Processor = new Processor {
    override def process(exchange: Exchange) = f(exchange)
  }
}