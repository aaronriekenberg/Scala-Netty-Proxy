package org.aaron.scala.netty.proxy

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.channel.socket.ClientSocketChannelFactory
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.logging.InternalLoggerFactory
import org.jboss.netty.logging.Slf4JLoggerFactory
import org.jboss.netty.util.Version

import com.weiglewilczek.slf4s.Logger

/**
 * Simple TCP proxy using Netty.
 */
class ScalaNettyProxy(
  localAddressPortStrings: Seq[String],
  remoteAddressPortString: String) {

  private val log = Logger(getClass)

  private class RemoteChannelHandler(
    val clientChannel: Channel)
    extends SimpleChannelUpstreamHandler {

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("remote channel open " + e.getChannel)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.warn("remote channel exception caught " + e.getChannel, e.getCause)
      e.getChannel.close
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("remote channel closed " + e.getChannel)
      NettyUtil.closeOnFlush(clientChannel)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      clientChannel.write(e.getMessage)
    }

  }

  private class ClientChannelHandler(
    remoteAddress: InetSocketAddress,
    clientSocketChannelFactory: ClientSocketChannelFactory)
    extends SimpleChannelUpstreamHandler {

    @volatile
    private var remoteChannel: Channel = null

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      val clientChannel = e.getChannel
      log.info("client channel open " + clientChannel)
      clientChannel.setReadable(false)

      val clientBootstrap = new ClientBootstrap(
        clientSocketChannelFactory)
      clientBootstrap.setOption("connectTimeoutMillis", 1000)
      clientBootstrap.getPipeline.addLast("handler",
        new RemoteChannelHandler(clientChannel))

      val connectFuture = clientBootstrap
        .connect(remoteAddress)
      remoteChannel = connectFuture.getChannel
      connectFuture.addListener(new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            log.info("remote channel connect success "
              + remoteChannel)
            clientChannel.setReadable(true)
          } else {
            log.info("remote channel connect failure "
              + remoteChannel)
            clientChannel.close
          }
        }
      })
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.info("client channel exception caught " + e.getChannel,
        e.getCause)
      e.getChannel.close
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("client channel closed " + e.getChannel)
      val remoteChannelCopy = remoteChannel
      if (remoteChannelCopy != null) {
        NettyUtil.closeOnFlush(remoteChannelCopy)
      }
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val remoteChannelCopy = remoteChannel
      if (remoteChannelCopy != null) {
        remoteChannelCopy.write(e.getMessage)
      }
    }

  }

  private class ProxyPipelineFactory(
    val remoteAddress: InetSocketAddress,
    val clientSocketChannelFactory: ClientSocketChannelFactory)
    extends ChannelPipelineFactory {

    override def getPipeline: ChannelPipeline =
      Channels.pipeline(new ClientChannelHandler(remoteAddress,
        clientSocketChannelFactory))

  }

  def start {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)

    val remoteAddress = NettyUtil.parseAddressPortString(remoteAddressPortString)

    val executor = Executors.newCachedThreadPool

    val serverBootstrap = new ServerBootstrap(
      new NioServerSocketChannelFactory(
        executor, executor))

    val clientSocketChannelFactory = new NioClientSocketChannelFactory(
      executor, executor)

    serverBootstrap.setPipelineFactory(new ProxyPipelineFactory(
      remoteAddress, clientSocketChannelFactory))

    serverBootstrap.setOption("reuseAddress", true)

    log.info("Netty version " + Version.ID)

    localAddressPortStrings.map(
      localAddrPortString =>
        serverBootstrap.bind(NettyUtil.parseAddressPortString(localAddrPortString))).
      foreach(
        serverChannel => log.info("listening on " + serverChannel.getLocalAddress))

    log.info("remote address " + remoteAddress);
  }

}

object ScalaNettyProxyMain extends App {

  private val log = Logger(getClass)

  if (args.size >= 2) {
    new ScalaNettyProxy(
      localAddressPortStrings = args.init,
      remoteAddressPortString = args.last).start
  } else {
    log.warn("Usage: <local address> [<local address>...] <remote address>")
    sys.exit(1)
  }

}