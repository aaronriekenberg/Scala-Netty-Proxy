package org.aaron.scala.netty.proxy.testclient

import java.util.concurrent.Executors

import org.aaron.scala.netty.proxy.NettyUtil
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.channel.socket.ServerSocketChannelFactory
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.logging.InternalLoggerFactory
import org.jboss.netty.logging.Slf4JLoggerFactory

import com.weiglewilczek.slf4s.Logger

/**
 * Simple TCP server using Netty.
 */
class ScalaTestServer(
  serverSocketChannelFactory: ServerSocketChannelFactory,
  serverAddressPortString: String) {

  private val log = Logger(getClass)

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
        new ClientHandler))
    serverBootstrap.setOption("reuseAddress", true)

    val serverChannel = serverBootstrap.bind(
      NettyUtil.parseAddressPortString(serverAddressPortString))

    log.info("listening on " + serverChannel.getLocalAddress)
  }

}

object ScalaTestServerMain extends App {

  private val log = Logger(getClass)

  if (args.size == 1) {
    val executor = Executors.newCachedThreadPool
    val serverSocketChannelFactory = new NioServerSocketChannelFactory(
      executor, executor)
    new ScalaTestServer(
      serverSocketChannelFactory = serverSocketChannelFactory,
      serverAddressPortString = args(0)).start
  } else {
    log.warn("Usage: <server address>")
    exit(1)
  }

}