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

package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Series reader is used to query one series of one TsFile, and this reader has a filter operating
 * on the same series.
 */
public class FileSeriesReader extends AbstractFileSeriesReader {

  public FileSeriesReader(
      IChunkLoader chunkLoader, List<IChunkMetadata> chunkMetadataList, Filter filter) {
    super(chunkLoader, chunkMetadataList, filter);
  }

  @Override
  protected void initChunkReader(IChunkMetadata chunkMetaData) throws IOException {
    if (chunkMetaData instanceof ChunkMetadata) {
      Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
      this.chunkReader = new ChunkReader(chunk, filter);
    } else {
      AlignedChunkMetadata alignedChunkMetadata = (AlignedChunkMetadata) chunkMetaData;
      Chunk timeChunk =
          chunkLoader.loadChunk((ChunkMetadata) (alignedChunkMetadata.getTimeChunkMetadata()));
      List<Chunk> valueChunkList = new ArrayList<>();
      for (IChunkMetadata metadata : alignedChunkMetadata.getValueChunkMetadataList()) {
        valueChunkList.add(chunkLoader.loadChunk((ChunkMetadata) metadata));
      }
      this.chunkReader = new AlignedChunkReader(timeChunk, valueChunkList, filter, false);
    }
  }

  @Override
  protected boolean chunkCanSkip(IChunkMetadata chunkMetaData) {
    return filter != null && filter.canSkip(chunkMetaData);
  }
}
