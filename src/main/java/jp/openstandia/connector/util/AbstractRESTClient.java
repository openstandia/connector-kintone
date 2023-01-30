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
package jp.openstandia.connector.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public abstract class AbstractRESTClient<C extends Configuration> {

    private static final Log LOG = Log.getLog(AbstractRESTClient.class);

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected String instanceName;
    protected C configuration;
    protected OkHttpClient httpClient;
    protected ErrorHandler errorHandler;
    protected int startOffset;
    protected int retryCount = 2;


    public interface ErrorHandler {
        boolean inNotAuthenticated(Response response);

        boolean isInvalidRequest(Response response);

        boolean isAlreadyExists(Response response);

        boolean isNotFound(Response response);

        boolean isOk(Response response);

        boolean isServerError(Response response);
    }

    public void init(String instanceName, C configuration, OkHttpClient httpClient, ErrorHandler errorHandler, int startOffset) {
        this.instanceName = instanceName;
        this.configuration = configuration;
        this.httpClient = httpClient;
        this.errorHandler = errorHandler;
        this.startOffset = startOffset;
    }

    public abstract void test();

    public void close() {
    }

    // Utilities

    /**
     * Generic create method.
     *
     * @param objectClass
     * @param url
     * @param target
     * @param name
     */
    protected void callCreate(ObjectClass objectClass, String url, Object target, String name) {
        try (Response response = post(url, target)) {
            if (errorHandler.isAlreadyExists(response)) {
                throw new AlreadyExistsException(String.format("%s %s '%s' already exists.", instanceName, objectClass.getObjectClassValue(), name));
            }
            if (errorHandler.isInvalidRequest(response)) {
                throw new InvalidAttributeValueException(String.format("Bad request when creating %s %s '%s'", instanceName, objectClass.getObjectClassValue(), name));
            }

            if (!this.errorHandler.isOk(response)) {
                throw new ConnectorIOException(String.format("Failed to create %s %s '%s', statusCode: %d, response: %s",
                        instanceName, objectClass.getObjectClassValue(), name, response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to create %s %s '%s'",
                    instanceName, objectClass.getObjectClassValue(), name), e);
        }
    }

    protected void callPatch(ObjectClass objectClass, String url, Uid uid, Object target) {
        try (Response response = patch(url, target)) {
            if (this.errorHandler.isInvalidRequest(response)) {
                throw new InvalidAttributeValueException(String.format("Bad request when updating %s %s: %s, response: %s",
                        this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue(), toBody(response)));
            }

            if (this.errorHandler.isNotFound(response)) {
                throw new UnknownUidException(uid, objectClass);
            }

            if (!this.errorHandler.isOk(response)) {
                throw new ConnectorIOException(String.format("Failed to patch %s %s: %s, statusCode: %d, response: %s",
                        this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue(), response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to patch %s %s: %s",
                    this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue()), e);
        }
    }

    protected void callUpdate(ObjectClass objectClass, String url, Uid uid, Object target) {
        try (Response response = put(url, target)) {
            if (this.errorHandler.isInvalidRequest(response)) {
                throw new InvalidAttributeValueException(String.format("Bad request when updating %s %s: %s, response: %s",
                        this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue(), toBody(response)));
            }

            if (this.errorHandler.isNotFound(response)) {
                throw new UnknownUidException(uid, objectClass);
            }

            if (!this.errorHandler.isOk(response)) {
                throw new ConnectorIOException(String.format("Failed to update %s %s: %s, statusCode: %d, response: %s",
                        this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue(), response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to update %s %s: %s",
                    this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue()), e);
        }
    }

    private String toBody(Response response) {
        ResponseBody resBody = response.body();
        if (resBody == null) {
            return null;
        }
        try {
            return resBody.string();
        } catch (IOException e) {
            LOG.error(e, "Unexpected {} API response", this.instanceName);
            return "<failed_to_parse_response>";
        }
    }

    /**
     * Generic delete method.
     *
     * @param objectClass
     * @param url
     * @param uid
     * @param body
     */
    protected void callDelete(ObjectClass objectClass, String url, Uid uid, Object body) {
        try (Response response = delete(url, body)) {
            if (this.errorHandler.isNotFound(response)) {
                throw new UnknownUidException(uid, objectClass);
            }

            if (!this.errorHandler.isOk(response)) {
                throw new ConnectorIOException(String.format("Failed to delete %s %s: %s, statusCode: %d, response: %s",
                        this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue(), response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to delete %s %s: %s",
                    this.instanceName, objectClass.getObjectClassValue(), uid.getUidValue()), e);
        }
    }

    private RequestBody createJsonRequestBody(Object body) {
        String bodyString;
        try {
            bodyString = MAPPER.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new ConnectorIOException("Failed to write request json body", e);
        }

        return RequestBody.create(bodyString, MediaType.parse("application/json; charset=UTF-8"));
    }

    private void throwExceptionIfUnauthorized(Response response) throws ConnectorIOException {
        if (!this.errorHandler.inNotAuthenticated(response)) {
            throw new ConnectionFailedException(String.format("Cannot authenticate to the %s REST API: %s",
                    this.instanceName, response.message()));
        }
    }

    private void throwExceptionIfServerError(Response response) throws ConnectorIOException {
        if (this.errorHandler.isServerError(response)) {
            try {
                String body = response.body().string();
                throw new ConnectorIOException(this.instanceName + " server error: " + body);
            } catch (IOException e) {
                throw new ConnectorIOException(this.instanceName + " server error", e);
            }
        }
    }

    protected Response get(String url) throws ConnectorIOException {
        return get(url, null);
    }

    protected Response get(String url, Map<String, String> params) throws ConnectorIOException {
        HttpUrl.Builder httpBuilder = HttpUrl.parse(url).newBuilder();

        if (params != null) {
            params.entrySet().stream().forEach(entry -> httpBuilder.addQueryParameter(entry.getKey(), entry.getValue()));
        }

        final Request request = new Request.Builder()
                .url(httpBuilder.build())
                .get()
                .build();

        final Response response;
        try {
            response = httpClient.newCall(request).execute();
        } catch (IOException e) {
            throw new ConnectorIOException(this.instanceName + " server error", e);
        }

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }

    protected <T> int getAll(QueryHandler<T> handler, int pageSize, BiFunction<Integer, Integer, List<T>> apiCall) {
        // Start offset (0 or 1) depends on the resource
        int start = startOffset;
        int count = 0;
        try {
            while (true) {
                List<T> results = apiCall.apply(start, pageSize);

                if (results.size() == 0) {
                    // End of the page
                    return count;
                }

                for (T result : results) {
                    count++;
                    if (!handler.handle(result)) {
                        return count;
                    }
                }

                // search next page
                start += pageSize;
            }
        } catch (RuntimeException e) {
            if (!(e instanceof ConnectorException)) {
                throw new ConnectorException(e);
            }
            throw e;
        }
    }

    private Response post(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        final Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();

        final Response response = httpClient.newCall(request).execute();

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }

    private Response put(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        final Request request = new Request.Builder()
                .url(url)
                .put(requestBody)
                .build();

        final Response response = httpClient.newCall(request).execute();

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }

    private Response patch(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        final Request request = new Request.Builder()
                .url(url)
                .patch(requestBody)
                .build();

        final Response response = httpClient.newCall(request).execute();

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }

    private Response delete(String url, Object body) throws IOException {
        final Request.Builder builder = new Request.Builder()
                .url(url);

        if (body != null) {
            RequestBody requestBody = createJsonRequestBody(body);
            builder.delete(requestBody);
        } else {
            builder.delete();
        }

        final Request request = builder.build();

        final Response response = httpClient.newCall(request).execute();

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }
}