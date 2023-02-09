/*
 *  Copyright Nomura Research Institute, Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package jp.openstandia.connector.kintone;

import jp.openstandia.connector.util.ObjectHandler;
import jp.openstandia.connector.util.SchemaDefinition;
import jp.openstandia.connector.util.Utils;
import okhttp3.*;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.*;
import org.identityconnectors.framework.spi.operations.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ConnectorClass(configurationClass = KintoneConfiguration.class, displayNameKey = "Kintone Connector")
public class KintoneConnector implements PoolableConnector, CreateOp, UpdateDeltaOp, DeleteOp, SchemaOp, TestOp, SearchOp<KintoneFilter>, InstanceNameAware {

    private static final Log LOG = Log.getLog(KintoneConnector.class);

    protected KintoneConfiguration configuration;
    protected KintoneRESTClient client;

    private KintoneSchema cachedSchema;
    private String instanceName;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (KintoneConfiguration) configuration;

        try {
            authenticateResource();
        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }

        LOG.ok("Connector {0} successfully initialized", getClass().getName());
    }

    protected void authenticateResource() {
        OkHttpClient.Builder okHttpBuilder = new OkHttpClient.Builder();
        okHttpBuilder.connectTimeout(configuration.getConnectionTimeoutInMilliseconds(), TimeUnit.MILLISECONDS);
        okHttpBuilder.readTimeout(configuration.getReadTimeoutInMilliseconds(), TimeUnit.MILLISECONDS);
        okHttpBuilder.writeTimeout(configuration.getWriteTimeoutInMilliseconds(), TimeUnit.MILLISECONDS);
        okHttpBuilder.addInterceptor(getInterceptor(configuration.getLoginName(), configuration.getPassword()));

        // Setup http proxy aware httpClient
        if (StringUtil.isNotEmpty(configuration.getHttpProxyHost())) {
            okHttpBuilder.proxy(new Proxy(Proxy.Type.HTTP,
                    new InetSocketAddress(configuration.getHttpProxyHost(), configuration.getHttpProxyPort())));

            if (StringUtil.isNotEmpty(configuration.getHttpProxyUser()) && configuration.getHttpProxyPassword() != null) {
                configuration.getHttpProxyPassword().access(c -> {
                    okHttpBuilder.proxyAuthenticator((Route route, Response response) -> {
                        String credential = Credentials.basic(configuration.getHttpProxyUser(), String.valueOf(c));
                        return response.request().newBuilder()
                                .header("Proxy-Authorization", credential)
                                .build();
                    });
                });
            }
        }

        OkHttpClient httpClient = okHttpBuilder.build();

        client = new KintoneRESTClient();
        client.init(instanceName, configuration, httpClient);

        // Verify we can access the kintone API
        client.test();
    }

    private Interceptor getInterceptor(String loginName, GuardedString accessToken) {
        return new Interceptor() {
            @Override
            public Response intercept(Chain chain) throws IOException {
                Request.Builder builder = chain.request().newBuilder()
                        .addHeader("Accept", "application/json");
                accessToken.access(c -> {
                    builder.addHeader("X-Cybozu-Authorization",
                            Base64.getEncoder().encodeToString((loginName + ":" + String.valueOf(c)).getBytes(StandardCharsets.UTF_8)));
                });
                return chain.proceed(builder.build());
            }
        };
    }

    @Override
    public Schema schema() {
        try {
            cachedSchema = new KintoneSchema(configuration, client);
            return cachedSchema.schema;

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    private ObjectHandler getSchemaHandler(ObjectClass objectClass) {
        if (objectClass == null) {
            throw new InvalidAttributeValueException("ObjectClass value not provided");
        }

        // Load schema map if it's not loaded yet
        if (cachedSchema == null) {
            schema();
        }

        ObjectHandler handler = cachedSchema.getSchemaHandler(objectClass);

        if (handler == null) {
            throw new InvalidAttributeValueException("Unsupported object class " + objectClass);
        }

        return handler;
    }

    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> createAttributes, OperationOptions options) {
        if (createAttributes == null || createAttributes.isEmpty()) {
            throw new InvalidAttributeValueException("Attributes not provided or empty");
        }

        try {
            return getSchemaHandler(objectClass).create(createAttributes);

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public Set<AttributeDelta> updateDelta(ObjectClass objectClass, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        try {
            return getSchemaHandler(objectClass).updateDelta(uid, modifications, options);

        } catch (UnknownUidException e) {
            LOG.warn("Not found object when updating. objectClass: {0}, uid: {1}", objectClass, uid);
            throw processRuntimeException(e);

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        try {
            getSchemaHandler(objectClass).delete(uid, options);

        } catch (UnknownUidException e) {
            LOG.warn("Not found object when deleting. objectClass: {0}, uid: {1}", objectClass, uid);
            throw processRuntimeException(e);

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public FilterTranslator<KintoneFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions options) {
        return new KintoneFilterTranslator(objectClass, options);
    }

    @Override
    public void executeQuery(ObjectClass objectClass, KintoneFilter filter, ResultsHandler resultsHandler, OperationOptions options) {
        ObjectHandler schemaHandler = getSchemaHandler(objectClass);
        SchemaDefinition schema = schemaHandler.getSchema();

        int pageSize = Utils.resolvePageSize(options, configuration.getDefaultQueryPageSize());
        int pageOffset = Utils.resolvePageOffset(options);

        // Create full attributesToGet by RETURN_DEFAULT_ATTRIBUTES + ATTRIBUTES_TO_GET
        Map<String, String> attributesToGet = Utils.createFullAttributesToGet(schema, options);
        Set<String> returnAttributesSet = attributesToGet.keySet();
        // Collect actual resource fields for fetching (We can them for filtering attributes if the resource supports it)
        Set<String> fetchFieldSet = attributesToGet.values().stream().collect(Collectors.toSet());

        boolean allowPartialAttributeValues = Utils.shouldAllowPartialAttributeValues(options);

        int total = 0;

        if (filter != null) {
            if (filter.isByUid()) {
                total = schemaHandler.getByUid((Uid) filter.attributeValue, resultsHandler, options,
                        returnAttributesSet, fetchFieldSet,
                        allowPartialAttributeValues, pageSize, pageOffset);
            } else if (filter.isByName()) {
                total = schemaHandler.getByName((Name) filter.attributeValue, resultsHandler, options,
                        returnAttributesSet, fetchFieldSet,
                        allowPartialAttributeValues, pageSize, pageOffset);
            }
            // No result
        } else {
            total = schemaHandler.getAll(resultsHandler, options,
                    returnAttributesSet, fetchFieldSet,
                    allowPartialAttributeValues, pageSize, pageOffset);
        }

        if (resultsHandler instanceof SearchResultsHandler &&
                pageOffset > 0) {

            int remaining = total - (pageSize * pageOffset);

            SearchResultsHandler searchResultsHandler = (SearchResultsHandler) resultsHandler;
            SearchResult searchResult = new SearchResult(null, remaining);
            searchResultsHandler.handleResult(searchResult);
        }
    }

    @Override
    public void test() {
        try {
            dispose();
            authenticateResource();
        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public void dispose() {
        client.close();
        this.client = null;
        this.cachedSchema = null;
    }

    @Override
    public void checkAlive() {
        // Do nothing
    }

    @Override
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    protected ConnectorException processRuntimeException(RuntimeException e) {
        if (e instanceof ConnectorException) {
            // Write error log because IDM might not write full stack trace
            // It's hard to debug the error
            if (e instanceof AlreadyExistsException) {
                LOG.warn(e, "Detected the object already exists");
            } else {
                LOG.error(e, "Detected Kintone connector error");
            }
            return (ConnectorException) e;
        }

        LOG.error(e, "Detected Kintone connector unexpected error");

        return new ConnectorIOException(e);
    }
}
