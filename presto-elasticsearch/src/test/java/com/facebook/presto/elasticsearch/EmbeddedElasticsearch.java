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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;

import static com.google.common.base.Preconditions.checkState;

public final class EmbeddedElasticsearch
        implements Closeable
{
    private static Node elasticServer;

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9300;
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

        Settings settings = Settings.builder().put("cluster.name", "presto-test-cluster").build();
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
        checkState(initialized, "EmbeddedCassandra must be started with #start() method before retrieving the cluster retrieval");
    }

    public static void flush(String keyspace, String table)
            throws Exception
    {
    }
}
