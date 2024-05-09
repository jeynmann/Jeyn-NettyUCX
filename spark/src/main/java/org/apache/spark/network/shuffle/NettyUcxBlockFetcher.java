package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.NettyUcxTransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenBlocks;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.network.util.TransportConf;

/**
 * Simple wrapper on top of a NettyUcxTransportClient which interprets each chunk as a whole block, and
 * invokes the BlockFetchingListener appropriately. This class is agnostic to the actual RPC
 * handler, as long as there is a single "open blocks" message which returns a ShuffleStreamHandle,
 * and Java serialization is used.
 *
 * Note that this typically corresponds to a
 * {@link org.apache.spark.network.server.OneForOneStreamManager} on the server side.
 */
public class NettyUcxBlockFetcher {
  private static final Logger logger = LoggerFactory.getLogger(NettyUcxBlockFetcher.class);

  private final NettyUcxTransportClient client;
  private final OpenBlocks openMessage;
  private final String[] blockIds;
  private final BlockFetchingListener listener;
  private final ChunkReceivedCallback chunkCallback;
  private final TransportConf transportConf;
  private final DownloadFileManager downloadFileManager;

  private StreamHandle streamHandle = null;

  public NettyUcxBlockFetcher(
    NettyUcxTransportClient client,
    String appId,
    String execId,
    String[] blockIds,
    BlockFetchingListener listener,
    TransportConf transportConf) {
    this(client, appId, execId, blockIds, listener, transportConf, null);
  }

  public NettyUcxBlockFetcher(
    NettyUcxTransportClient client,
    String appId,
    String execId,
    String[] blockIds,
    BlockFetchingListener listener,
    TransportConf transportConf,
    DownloadFileManager downloadFileManager) {
    this.client = client;
    this.openMessage = new OpenBlocks(appId, execId, blockIds);
    this.blockIds = blockIds;
    this.listener = listener;
    this.chunkCallback = new ChunkCallback();
    this.transportConf = transportConf;
    this.downloadFileManager = downloadFileManager;
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
  class ChunkCallback implements ChunkReceivedCallback {
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, e);
    }
  }

  /**
   * Begins the fetching process, calling the listener with every block fetched.
   * The given message will be serialized with the Java serializer, and the RPC must return a
   * {@link StreamHandle}. We will send all fetch requests immediately, without throttling.
   */
  public void start() {
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }

    client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          if (downloadFileManager != null) {
            for (int i = 0; i < streamHandle.numChunks; i++) {
              client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
              new DownloadCallback(i));
            }
          } else {
            client.fetchBatch(streamHandle.streamId, streamHandle.numChunks, chunkCallback);
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          failRemainingBlocks(blockIds, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(blockIds, e);
      }
    });
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }

  class DownloadCallback implements StreamCallback {

    private DownloadFileWritableChannel channel = null;
    private DownloadFile targetFile = null;
    private int chunkIndex;

    DownloadCallback(int chunkIndex) throws IOException {
      this.targetFile = downloadFileManager.createTempFile(transportConf);
      this.channel = targetFile.openForWriting();
      this.chunkIndex = chunkIndex;
    }

    @Override
    public void onData(String streamId, ByteBuffer buf) throws IOException {
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
    }

    @Override
    public void onComplete(String streamId) throws IOException {
      listener.onBlockFetchSuccess(blockIds[chunkIndex], channel.closeAndRead());
      if (!downloadFileManager.registerTempFileToClean(targetFile)) {
        targetFile.delete();
      }
    }

    @Override
    public void onFailure(String streamId, Throwable cause) throws IOException {
      channel.close();
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, cause);
      targetFile.delete();
    }
  }
}
