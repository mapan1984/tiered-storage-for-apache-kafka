/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import io.aiven.kafka.tieredstorage.config.ChunkCacheConfig;
import io.aiven.kafka.tieredstorage.fetch.ChunkKey;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectMemoryChunkCache extends ChunkCache<ByteBuffer> {
    private static final Logger log = LoggerFactory.getLogger(DirectMemoryChunkCache.class);

    public DirectMemoryChunkCache(final ChunkManager chunkManager) {
        super(chunkManager);
    }

    @Override
    public InputStream cachedChunkToInputStream(final ByteBuffer cachedChunk) {
        ByteBuffer duplicate = cachedChunk.duplicate();
        return new InputStream() {
            @Override
            public int read() {
                if (!duplicate.hasRemaining()) {
                    return -1;
                }
                return duplicate.get() & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (!duplicate.hasRemaining()) {
                    return -1;
                }
                int bytesToRead = Math.min(len, duplicate.remaining());
                duplicate.get(b, off, bytesToRead);
                return bytesToRead;
            }
        };
    }

    @Override
    public ByteBuffer cacheChunk(final ChunkKey chunkKey, final InputStream chunk) throws IOException {
        try (chunk) {
            byte[] bytes = chunk.readAllBytes();
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(bytes.length);
            directBuffer.put(bytes);
            directBuffer.flip();
            return directBuffer;
        }
    }

    @Override
    public RemovalListener<ChunkKey, ByteBuffer> removalListener() {
        return (key, content, cause) -> {
            if (content != null) {
                if (content.isDirect()) {
                    try {
                        content.clear();
                    } catch (Exception e) {
                        log.warn("Failed to clean direct buffer", e);
                    }
                }
            }
            log.debug("Deleted cached value for key {} from cache. The reason of the deletion is {}", key, cause);
        };
    }

    @Override
    public Weigher<ChunkKey, ByteBuffer> weigher() {
        return (key, value) -> value.capacity();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final ChunkCacheConfig config = new ChunkCacheConfig(configs);
        this.cache = buildCache(config);
    }
}
