package org.aaron.scala.netty.proxy

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.socket.ClientSocketChannelFactory
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender
import org.jboss.netty.handler.codec.string.StringDecoder
import org.jboss.netty.handler.codec.string.StringEncoder
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.InternalLogLevel
import org.jboss.netty.logging.InternalLoggerFactory
import org.jboss.netty.logging.Slf4JLoggerFactory
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.util.Timeout
import org.jboss.netty.util.TimerTask

import com.weiglewilczek.slf4s.Logger

class ScalaTestClient(
  val clientSocketChannelFactory: ClientSocketChannelFactory,
  val timer: HashedWheelTimer,
  val serverAddressPortString: String,
  val reconnectDelaySeconds: Int = 1) {

  private val log = Logger(getClass)

  private val maxFrameLengthBytes = 1 * 1024 * 1024

  private val headerLengthBytes = 4

  private val clientBootstrap = new ClientBootstrap(
    clientSocketChannelFactory)

  private class ClientHandler extends SimpleChannelUpstreamHandler {

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("channelOpen " + e.getChannel)
    }

    override def channelConnected(ctx: ChannelHandlerContext,
      e: ChannelStateEvent) {
      log.info("channelConnected " + e.getChannel)
      for (i <- 0 until 10) {
        e.getChannel().write(
          "port "
            + (e.getChannel()
              .getLocalAddress().asInstanceOf[InetSocketAddress]).getPort
              + " message " + i)
      }
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("channelClosed " + e.getChannel())
      timer.newTimeout(new TimerTask {
        override def run(timeout: Timeout) {
          log.info("before connect")
          clientBootstrap.connect()
        }
      }, reconnectDelaySeconds, TimeUnit.SECONDS)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.warn("exceptionCaught " + e.getChannel, e.getCause)
      e.getChannel().close()
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      log.info("messageReceived from " + e.getChannel + " message = '"
        + e.getMessage + "'")
    }

  }

  private class ClientPipelineFactory extends ChannelPipelineFactory {

    override def getPipeline: ChannelPipeline =
      Channels.pipeline(

        new LoggingHandler(InternalLogLevel.DEBUG),

        new LengthFieldPrepender(headerLengthBytes),

        new LengthFieldBasedFrameDecoder(maxFrameLengthBytes, 0,
          headerLengthBytes, 0, headerLengthBytes),

        new StringEncoder, new StringDecoder,

        new ClientHandler)

  }

  def start() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)

    clientBootstrap.setPipelineFactory(new ClientPipelineFactory)

    clientBootstrap.setOption("remoteAddress",
      NettyUtil.parseAddressPortString(serverAddressPortString))

    log.info("before connect")
    clientBootstrap.connect
  }

}

object ScalaTestClientMain {

  private val log = Logger(getClass)

  def main(args: Array[String]) {
    args match {
      case args if args.size == 1 =>
        val executor = Executors.newCachedThreadPool
        val timer = new HashedWheelTimer
        val clientSocketChannelFactory = new NioClientSocketChannelFactory(
          executor, executor)
        new ScalaTestClient(
          timer = timer,
          clientSocketChannelFactory = clientSocketChannelFactory,
          serverAddressPortString = args(0)).start

      case _ =>
        log.warn("Usage: <server address>")
        exit(1)
    }
  }

}