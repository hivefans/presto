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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class ElasticsearchConfig
{
    private String hostname;
    private Integer port;

    @Config("connection-hostname")
    public ElasticsearchConfig setHostname(String hostname)
    {
        this.hostname = hostname;
        return this;
    }

    @Config("connection-port")
    public ElasticsearchConfig setPort(Integer port)
    {
        this.port = port;
        return this;
    }

    @NotNull
    public String getHostname()
    {
        return hostname;
    }

    @NotNull
    public Integer getPort()
    {
        return port;
    }
}
