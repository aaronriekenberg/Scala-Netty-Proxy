package org.aaron.scala.netty.proxy

import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.socket.ServerSocketChannelFactory
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
import com.weiglewilczek.slf4s.Logger
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory

/**
 * Simple TCP server using Netty.
 */
class ScalaTestServer(
  val serverSocketChannelFactory: ServerSocketChannelFactory,
  val serverAddressPortString: String) {

  private val log = Logger(getClass)

  private val maxFrameLengthBytes = 1 * 1024 * 1024

  private val headerLengthBytes = 4

  private val serverBootstrap = new ServerBootstrap(
    serverSocketChannelFactory)

  private class ClientHandler extends SimpleChannelUpstreamHandler {

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("channelOpen " + e.getChannel)
    }

    override def channelConnected(ctx: ChannelHandlerContext,
      e: ChannelStateEvent) {
      log.info("channelConnected " + e.getChannel)
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("channelClosed " + e.getChannel)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.warn("exceptionCaught " + e.getChannel, e.getCause)
      e.getChannel.close
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      log.info("messageReceived from " + e.getChannel + " message = '"
        + e.getMessage + "'")
      e.getChannel.write(e.getMessage)
    }

  }

  def start() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)

    serverBootstrap.setPipelineFactory(
      new TestProtocol.TestProtocolPipelineFactory(
        () => new ClientHandler))
    serverBootstrap.setOption("reuseAddress", true)

    val serverChannel = serverBootstrap.bind(
      NettyUtil.parseAddressPortString(serverAddressPortString))

    log.info("listening on " + serverChannel)
  }

}

object ScalaTestServerMain {

  private val log = Logger(getClass)

  def main(args: Array[String]) {
    args match {
      case args if args.size == 1 =>
        val executor = Executors.newCachedThreadPool
        val serverSocketChannelFactory = new NioServerSocketChannelFactory(
          executor, executor)
        new ScalaTestServer(
          serverSocketChannelFactory = serverSocketChannelFactory,
          serverAddressPortString = args(0)).start

      case _ =>
        log.warn("Usage: <server address>")
        exit(1)
    }
  }

}