package org.aaron.scala.netty.proxy.testclient

import org.jboss.netty.channel.ChannelHandler
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender
import org.jboss.netty.handler.codec.string.StringDecoder
import org.jboss.netty.handler.codec.string.StringEncoder
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.InternalLogLevel

/**
 * Protocol definition used for sending messages between ScalaTestClient and
 * ScalaTestServer.  Each message consists of a 4 byte header followed by a
 * string.  Maximum length of a messages is 1 MB.
 */
object TestProtocol {

  val maxFrameLengthBytes = 1 * 1024 * 1024

  val headerLengthBytes = 4

  class TestProtocolPipelineFactory(
    lastHandler: => ChannelHandler)
    extends ChannelPipelineFactory {

    override def getPipeline: ChannelPipeline =
      Channels.pipeline(

        new LoggingHandler(InternalLogLevel.DEBUG),

        new LengthFieldPrepender(headerLengthBytes),

        new LengthFieldBasedFrameDecoder(maxFrameLengthBytes, 0,
          headerLengthBytes, 0, headerLengthBytes),

        new StringEncoder, new StringDecoder,

        lastHandler)

  }

}