/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import io.airlift.testing.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static com.google.common.base.Preconditions.checkState;

public final class EmbeddedElasticsearch
        implements Closeable
{
    private static Node elasticServer;

    public static final String HOST = "localhost";
    public static final Integer PORT = 19300;
    public static final String DATA_PATH = "target/elasticsearch-data";
    private static boolean initialized;

    private EmbeddedElasticsearch()
    {
    }

    public static EmbeddedElasticsearch createEmbeddedElasticsearch()
            throws IOException, URISyntaxException
    {
        return new EmbeddedElasticsearch();
    }

    public static synchronized void start()
            throws Exception
    {
        if (initialized) {
            return;
        }
        FileUtils.deleteDirectoryContents(new File(DATA_PATH));

        // http://stackoverflow.com/questions/41298467/how-to-start-elasticsearch-5-1-embedded-in-my-java-application
        Settings settings = Settings.builder()
                .put("transport.type", "local")
                .put("http.enabled", "false")
                .put("path.home", DATA_PATH)
                .put("transport.bind_host", HOST)
                .put("transport.tcp.port", PORT)
                .build();
        elasticServer = new Node(settings);

        elasticServer.start();

        initialized = true;
    }

    @Override
    public void close()
    {
        try {
            elasticServer.close();
        }
        catch (IOException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static synchronized String getHost()
    {
        checkIsInitialized();
        return HOST;
    }

    public static synchronized int getPort()
    {
        checkIsInitialized();
        return PORT;
    }

    public static synchronized Client getClient()
    {
        checkIsInitialized();
        return elasticServer.client();
    }

    private static void checkIsInitialized()
    {
        checkState(initialized, "EmbeddedElasticsearch must be started with #start() method before retrieving the cluster retrieval");
    }

    public static void flush(String keyspace, String table)
            throws Exception
    {
    }
}
