
package com.facebook.thrift.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ClosedChannelException;

/**
 * Wrapper around ServerSocketChannel
 */
public class TNonblockingServerSocket extends TNonblockingServerTransport {

  /**
   * This channel is where all the nonblocking magic happens.
   */
  private ServerSocketChannel serverSocketChannel = null;

  /**
   * Underlying serversocket object
   */
  private ServerSocket serverSocket_ = null;

  /**
   * Port to listen on
   */
  private int port_ = 0;

  /**
   * Timeout for client sockets from accept
   */
  private int clientTimeout_ = 0;

  /**
   * Creates a server socket from underlying socket object
   */
  // public TNonblockingServerSocket(ServerSocket serverSocket) {
  //   this(serverSocket, 0);
  // }

  /**
   * Creates a server socket from underlying socket object
   */
  // public TNonblockingServerSocket(ServerSocket serverSocket, int clientTimeout) {
  //   serverSocket_ = serverSocket;
  //   clientTimeout_ = clientTimeout;
  // }

  /**
   * Creates just a port listening server socket
   */
  public TNonblockingServerSocket(int port) throws TTransportException {
    this(port, 0);
  }

  /**
   * Creates just a port listening server socket
   */
  public TNonblockingServerSocket(int port, int clientTimeout) throws TTransportException {
    port_ = port;
    clientTimeout_ = clientTimeout;
    try {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);

      // Make server socket
      serverSocket_ = serverSocketChannel.socket();
      // Prevent 2MSL delay problem on server restarts
      serverSocket_.setReuseAddress(true);
      // Bind to listening port
      serverSocket_.bind(new InetSocketAddress(port_));
    } catch (IOException ioe) {
      serverSocket_ = null;
      throw new TTransportException("Could not create ServerSocket on port " + port + ".");
    }
  }

  public void listen() throws TTransportException {
    // Make sure not to block on accept
    if (serverSocket_ != null) {
      try {
        serverSocket_.setSoTimeout(0);
      } catch (SocketException sx) {
        sx.printStackTrace();
      }
    }
  }

  protected TNonblockingSocket acceptImpl() throws TTransportException {
    if (serverSocket_ == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
    }
    try {
      SocketChannel socketChannel = serverSocketChannel.accept();
      if (socketChannel == null) {
        return null;
      }

      TNonblockingSocket tsocket = new TNonblockingSocket(socketChannel);
      tsocket.setTimeout(clientTimeout_);
      return tsocket;
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  public void registerSelector(Selector selector) {
    try {
      // Register the server socket channel, indicating an interest in
      // accepting new connections
      serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    } catch (ClosedChannelException e) {
      // this shouldn't happen, ideally...
      // TODO: decide what to do with this.
    }
  }

  public void close() {
    if (serverSocket_ != null) {
      try {
        serverSocket_.close();
      } catch (IOException iox) {
        System.err.println("WARNING: Could not close server socket: " +
                           iox.getMessage());
      }
      serverSocket_ = null;
    }
  }

  public void interrupt() {
    // The thread-safeness of this is dubious, but Java documentation suggests
    // that it is safe to do this from a different thread context
    close();
  }

}
