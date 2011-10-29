package org.aaron.scala.netty.proxy

import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.Channels
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.InternalLogLevel
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import org.jboss.netty.handler.codec.string.StringEncoder
import org.jboss.netty.handler.codec.string.StringDecoder
import org.jboss.netty.channel.ChannelHandler

// Protocol definition used for sending messages between
// ScalaTestClient and ScalaTestServer.  Each message consists
// of a 4 byte header followed by a string up to 1 MB in length.

object TestProtocol {

  val maxFrameLengthBytes = 1 * 1024 * 1024

  val headerLengthBytes = 4

  class TestProtocolPipelineFactory(
    val createLastHandler: () => ChannelHandler)
    extends ChannelPipelineFactory {

    override def getPipeline: ChannelPipeline =
      Channels.pipeline(

        new LoggingHandler(InternalLogLevel.DEBUG),

        new LengthFieldPrepender(headerLengthBytes),

        new LengthFieldBasedFrameDecoder(maxFrameLengthBytes, 0,
          headerLengthBytes, 0, headerLengthBytes),

        new StringEncoder, new StringDecoder,

        createLastHandler())

  }

}