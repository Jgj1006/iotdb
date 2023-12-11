/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class ChunkReader implements IChunkReader {

  private final ChunkHeader chunkHeader;
  private final ByteBuffer chunkDataBuffer;
  private final IUnCompressor unCompressor;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  // any filter, no matter value filter or time filter
  private final Filter queryFilter;
  private final long currentTimestamp;

  private final List<IPageReader> pageReaderList = new LinkedList<>();

  /** A list of deleted intervals. */
  private final List<TimeRange> deleteIntervalList;

  /**
   * constructor of ChunkReader.
   *
   * @param chunk input Chunk object
   * @param queryFilter filter
   * @throws IOException exception when initAllPageReaders
   */
  public ChunkReader(Chunk chunk, Filter queryFilter) throws IOException {
    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.unCompressor = IUnCompressor.getUnCompressor(this.chunkHeader.getCompressionType());
    this.queryFilter = queryFilter;
    this.currentTimestamp = Long.MIN_VALUE;
    this.deleteIntervalList = chunk.getDeleteIntervalList();

    initAllPageReaders(chunk.getChunkStatistic());
  }

  /**
   * Constructor of ChunkReader by timestamp. This constructor is used to accelerate queries by
   * filtering out pages whose endTime is less than current timestamp.
   *
   * @throws IOException exception when initAllPageReaders
   */
  public ChunkReader(Chunk chunk, long currentTimestamp) throws IOException {
    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.unCompressor = IUnCompressor.getUnCompressor(this.chunkHeader.getCompressionType());
    this.queryFilter = null;
    this.currentTimestamp = currentTimestamp;
    this.deleteIntervalList = chunk.getDeleteIntervalList();

    initAllPageReaders(chunk.getChunkStatistic());
  }

  /**
   * Constructor of ChunkReader without deserializing chunk into page. This is used for fast
   * compaction.
   */
  public ChunkReader(Chunk chunk) {
    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    this.queryFilter = null;
    this.currentTimestamp = Long.MIN_VALUE;
    this.deleteIntervalList = chunk.getDeleteIntervalList();
  }

  private void initAllPageReaders(Statistics chunkStatistic) throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkStatistic);
        // when there is only one page in the chunk, the page statistic is the same as the chunk, so
        // we needn't filter the page again
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        // if the current page satisfies
        if (pageCanSkip(pageHeader)) {
          skipBytesInStreamByLength(pageHeader.getCompressedSize());
          continue;
        }
      }
      pageReaderList.add(constructPageReaderForNextPage(pageHeader));
    }
  }

  /** judge if has next page whose page header satisfies the filter. */
  @Override
  public boolean hasNextSatisfiedPage() {
    return !pageReaderList.isEmpty();
  }

  /**
   * get next data batch.
   *
   * @return next data batch
   * @throws IOException IOException
   */
  @Override
  public BatchData nextPageData() throws IOException {
    if (pageReaderList.isEmpty()) {
      throw new IOException("No more page");
    }
    return pageReaderList.remove(0).getAllSatisfiedPageData();
  }

  private void skipBytesInStreamByLength(int length) {
    chunkDataBuffer.position(chunkDataBuffer.position() + length);
  }

  protected boolean pageCanSkip(PageHeader pageHeader) {
    if (currentTimestamp > pageHeader.getEndTime()) {
      // used for chunk reader by timestamp
      return true;
    }

    long startTime = pageHeader.getStartTime();
    long endTime = pageHeader.getEndTime();
    if (deleteIntervalList != null) {
      for (TimeRange range : deleteIntervalList) {
        if (range.contains(startTime, endTime)) {
          return true;
        }
        if (range.overlaps(new TimeRange(startTime, endTime))) {
          pageHeader.setModified(true);
        }
      }
    }
    return queryFilter != null && !queryFilter.satisfyStartEndTime(startTime, endTime);
  }

  private PageReader constructPageReaderForNextPage(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      unCompressor.uncompress(
          compressedPageBody, 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage());
    }

    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);
    PageReader reader =
        new PageReader(
            pageHeader,
            pageData,
            chunkHeader.getDataType(),
            valueDecoder,
            timeDecoder,
            queryFilter);
    reader.setDeleteIntervalList(deleteIntervalList);
    return reader;
  }

  /**
   * Read page data without uncompressing it.
   *
   * @return compressed page data
   */
  public ByteBuffer readPageDataWithoutUncompressing(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    return ByteBuffer.wrap(compressedPageBody);
  }

  /**
   * Read data from compressed page data. Uncompress the page and decode it to batch data.
   *
   * @param compressedPageData Compressed page data
   */
  public TsBlock readPageData(PageHeader pageHeader, ByteBuffer compressedPageData)
      throws IOException {
    // uncompress page data
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      unCompressor.uncompress(
          compressedPageData.array(), 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage());
    }

    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);

    // decode page data
    TSDataType dataType = chunkHeader.getDataType();
    Decoder valueDecoder = Decoder.getDecoderByType(chunkHeader.getEncodingType(), dataType);
    PageReader pageReader =
        new PageReader(pageHeader, pageData, dataType, valueDecoder, timeDecoder, queryFilter);
    pageReader.setDeleteIntervalList(deleteIntervalList);
    return pageReader.getAllSatisfiedData();
  }

  @Override
  public void close() {
    // do nothing
  }

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return pageReaderList;
  }
}
