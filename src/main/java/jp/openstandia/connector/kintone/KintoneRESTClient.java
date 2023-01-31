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

import jp.openstandia.connector.util.AbstractRESTClient;
import jp.openstandia.connector.util.QueryHandler;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static jp.openstandia.connector.kintone.KintoneGroupHandler.GROUP_OBJECT_CLASS;
import static jp.openstandia.connector.kintone.KintoneOrganizationHandler.ORGANIZATION_OBJECT_CLASS;
import static jp.openstandia.connector.kintone.KintoneUserHandler.USER_OBJECT_CLASS;

public class KintoneRESTClient extends AbstractRESTClient<KintoneConfiguration> {
    private static final Log LOG = Log.getLog(KintoneRESTClient.class);
    private ErrorHandler ERROR_HANDLER = new KintoneErrorHandler();

    private String testEndpoint;
    private String userEndpoint;
    private String userRenameEndpoint;
    private String userServicesEndpoint;
    private String userOrganizationsEndpoint;
    private String userOrganizationsUpdateEndpoint;
    private String userGroupsEndpoint;
    private String organizationEndpoint;
    private String organizationRenameEndpoint;
    private String groupEndpoint;
    private String groupRenameEndpoint;

    static class ErrorResponse {
        public String id;
        public String code;
        public String message;
        public Map<String, Map<String, List<String>>> errors;
    }

    static class ListBody {
        public List<KintoneUserModel> users;
        public List<KintoneOrganizationModel> organizations;
        public List<KintoneGroupModel> groups;
    }

    static class UserServicesBody {
        public List<UserServiceBody> users;
    }

    static class UserServiceBody {
        public String code;
        public List<String> services;
    }

    static class UserOrganizationsBody {
        public List<UserOrganizationBody> organizationTitles;
    }

    static class UserOrganizationBody {
        public KintoneOrganizationModel organization;
        public Title title;
    }

    static class Title {
        public String id;
        public String code;
        public String name;
        public String description;
    }

    static class UserOrganizationsUpdateBody {
        public List<UserOrganizationUpdateBody> userOrganizations;
    }

    static class UserOrganizationUpdateBody {
        public String code;
        public List<OrganizationUpdateBody> organizations;
    }

    static class OrganizationUpdateBody {
        public String orgCode;
        public String titleCode;
    }

    static class UserGroupsBody {
        public List<KintoneGroupModel> groups;
    }

    static class UserGroupsUpdateBody {
        public String code;
        public List<String> groups;
    }

    static class CodesBody<T> {
        public List<T> codes;
    }

    static class RenameCode {
        public String currentCode;
        public String newCode;
    }

    class KintoneErrorHandler implements ErrorHandler {
        @Override
        public boolean inNotAuthenticated(Response response) {
            if (response.code() != 520) {
                return false;
            }
            try {
                String bodyText = snapshotResponse(response);

                ErrorResponse res = MAPPER.readValue(bodyText, ErrorResponse.class);

                return res.code.equals("CB_WA01") // Invalid credential
                        || res.code.equals("CB_AU01"); // No auth header
            } catch (IOException e) {
                throw new ConnectorIOException(e);
            }
        }

        @Override
        public boolean isAlreadyExists(Response response) {
            if (response.code() != 400) {
                return false;
            }

            String bodyText = snapshotResponse(response);

            try {
                ErrorResponse res = MAPPER.readValue(bodyText, ErrorResponse.class);

                if (!res.code.equals("CB_VA01")) {
                    return false;
                }

                String[] keywords = new String[]{
                        "すでに登録されています",
                        "already exists",
                        "已存在"
                };

                if (res.errors.containsKey("users.code")) {
                    return hasMessage(res.errors, "users.code", keywords);
                }
                if (res.errors.containsKey("groups.code")) {
                    return hasMessage(res.errors, "groups.code", keywords);
                }
                if (res.errors.containsKey("organizations.code")) {
                    return hasMessage(res.errors, "organizations.code", keywords);
                }
            } catch (IOException e) {
                throw new ConnectorIOException(e);
            }

            return false;
        }

        private boolean hasMessage(Map<String, Map<String, List<String>>> error, String key, String... keywords) {
            Map<String, List<String>> message = error.get(key);
            if (message != null) {
                List<String> messages = message.get("messages");
                if (messages == null) {
                    return false;
                }
                Optional<String> found = messages.stream().filter(m -> {
                            // The error message depends on the API user's locale
                            for (String k : keywords) {
                                if (m.contains(k)) {
                                    return true;
                                }
                            }
                            return false;
                        })
                        .findFirst();
                return found.isPresent();
            }
            return false;
        }

        @Override
        public boolean isInvalidRequest(Response response) {
            return response.code() == 400;
        }

        @Override
        public boolean isNotFound(Response response) {
            if (response.code() != 400) {
                return false;
            }

            String bodyText = snapshotResponse(response);

            try {
                ErrorResponse res = MAPPER.readValue(bodyText, ErrorResponse.class);

                if (!res.code.equals("CB_VA01")) {
                    return false;
                }

                String[] keywords = new String[]{
                        "見つかりません",
                        "not found",
                        "未找到指定的",
                        "未找到相應的"
                };

                if (res.errors.containsKey("users.code")) {
                    return hasMessage(res.errors, "users.code", keywords);
                }
                if (res.errors.containsKey("groups.code")) {
                    return hasMessage(res.errors, "groups.code", keywords);
                }
                if (res.errors.containsKey("organizations.code")) {
                    return hasMessage(res.errors, "organizations.code", keywords);
                }
            } catch (IOException e) {
                throw new ConnectorIOException(e);
            }

            return false;
        }

        @Override
        public boolean isOk(Response response) {
            return response.code() == 200 || response.code() == 204;
        }

        @Override
        public boolean isServerError(Response response) {
            return response.code() >= 500 && response.code() <= 599;
        }
    }

    public void init(String instanceName, KintoneConfiguration configuration, OkHttpClient httpClient) {
        super.init(instanceName, configuration, httpClient, ERROR_HANDLER, 0);
        this.testEndpoint = configuration.getBaseURL() + "/v1/users.json?size=1";
        this.userEndpoint = configuration.getBaseURL() + "/v1/users.json";
        this.userRenameEndpoint = configuration.getBaseURL() + "/v1/users/codes.json";
        this.userServicesEndpoint = configuration.getBaseURL() + "/v1/users/services.json";
        this.userOrganizationsEndpoint = configuration.getBaseURL() + "/v1/user/organizations.json";
        this.userOrganizationsUpdateEndpoint = configuration.getBaseURL() + "/v1/userOrganizations.json";
        this.userGroupsEndpoint = configuration.getBaseURL() + "/v1/user/groups.json";
        this.organizationEndpoint = configuration.getBaseURL() + "/v1/organizations.json";
        this.organizationRenameEndpoint = configuration.getBaseURL() + "/v1/organizations/codes.json";
        this.groupEndpoint = configuration.getBaseURL() + "/v1/groups.json";
        this.groupRenameEndpoint = configuration.getBaseURL() + "/v1/groups/codes.json";
    }

    public void test() {
        try (Response response = get(testEndpoint)) {
            if (response.code() != 200) {
                // Something wrong..
                String body = response.body().string();
                throw new ConnectionFailedException(String.format("Failed %s test response. statusCode: %s, body: %s",
                        instanceName,
                        response.code(),
                        body));
            }

            LOG.info("{0} connector's connection test is OK", instanceName);

        } catch (IOException e) {
            throw new ConnectionFailedException(String.format("Cannot connect to %s REST API", instanceName), e);
        }
    }

    // User

    public Uid createUser(KintoneUserModel newUser) throws AlreadyExistsException {
        ListBody body = new ListBody();
        body.users = new ArrayList<>(1);
        body.users.add(newUser);

        callCreate(USER_OBJECT_CLASS, userEndpoint, body, newUser.code);

        // We need to fetch the created object for getting the generated id
        KintoneUserModel created = getUser(new Name(newUser.code), null, null);

        return new Uid(created.id, newUser.code);
    }

    public KintoneUserModel getUser(Uid uid, OperationOptions options, Set<String> fetchFieldsSet) throws UnknownUidException {
        Map<String, String> params = new HashMap<>();
        params.put("ids", uid.getUidValue());

        try (Response response = get(userEndpoint, params)) {
            try {
                ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                if (list.users == null || list.users.size() != 1) {
                    // Something wrong..
                    throw new ConnectorIOException(String.format("Cannot find %s user %s", instanceName, uid.getUidValue()));
                }
                return list.users.get(0);

            } catch (IOException e) {
                throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
            }
        }
    }

    public KintoneUserModel getUser(Name name, OperationOptions options, Set<String> fetchFieldsSet) throws UnknownUidException {
        Map<String, String> params = new HashMap<>();
        params.put("codes", name.getNameValue());

        try (Response response = get(userEndpoint, params)) {
            try {
                ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                if (list.users == null || list.users.size() != 1) {
                    // Something wrong..
                    throw new ConnectorIOException(String.format("Cannot find %s user %s", instanceName, name.getNameValue()));
                }
                return list.users.get(0);

            } catch (IOException e) {
                throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
            }
        }
    }

    public void updateUser(Uid uid, KintoneUserModel update) {
        ListBody body = new ListBody();
        body.users = new ArrayList<>(1);
        body.users.add(update);

        callUpdate(USER_OBJECT_CLASS, userEndpoint, uid, body);
    }

    public void renameUser(Uid uid, String newCode) {
        CodesBody<RenameCode> codes = new CodesBody<>();

        RenameCode renameCode = new RenameCode();
        renameCode.currentCode = uid.getNameHintValue();
        renameCode.newCode = newCode;

        codes.codes = new ArrayList<>(1);
        codes.codes.add(renameCode);

        callUpdate(USER_OBJECT_CLASS, userRenameEndpoint, uid, codes);
    }

    public void deleteUser(Uid uid) {
        Uid resolvedUid = resolveUserCode(uid);

        CodesBody<String> codes = new CodesBody<>();
        codes.codes = new ArrayList<>(1);
        codes.codes.add(resolvedUid.getNameHintValue());

        callDelete(USER_OBJECT_CLASS, userEndpoint, uid, codes);
    }

    protected Uid resolveUserCode(Uid uid) {
        if (uid.getNameHint() != null) {
            return uid;
        } else {
            KintoneUserModel user = getUser(uid, null, null);
            return new Uid(uid.getUidValue(), user.code);
        }
    }

    public int getUsers(QueryHandler<KintoneUserModel> handler, OperationOptions options, Set<String> fetchFieldsSet, int pageSize, int pageOffset) {
        // ConnId starts from 1, 0 means no offset (requested all data)
        if (pageOffset < 1) {
            return getAll(handler, pageSize, (start, size) -> {
                Map<String, String> params = new HashMap<>();
                params.put("offset", String.valueOf(start));
                params.put("size", String.valueOf(size));

                try (Response response = get(userEndpoint, params)) {
                    ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                    return list.users;

                } catch (IOException e) {
                    throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
                }
            });
        }

        // Pagination
        // Kintone starts from 0
        int start = pageOffset - 1;

        Map<String, String> params = new HashMap<>();
        params.put("offset", String.valueOf(start));
        params.put("size", String.valueOf(pageSize));

        try (Response response = get(userEndpoint, params)) {
            ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
            for (KintoneUserModel user : list.users) {
                if (!handler.handle(user)) {
                    break;
                }
            }
            return list.users.size();

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    // User-Service

    public Stream<String> getServicesForUser(Uid uid, int pageSize) {
        return getServicesForUser(uid.getNameHintValue(), pageSize);
    }

    public Stream<String> getServicesForUser(String code, int pageSize) {
        Map<String, String> params = new HashMap<>();
        params.put("codes", code);

        try (Response response = get(userServicesEndpoint, params)) {
            UserServicesBody body = MAPPER.readValue(response.body().byteStream(), UserServicesBody.class);
            Optional<UserServiceBody> services = body.users.stream()
                    .filter(u -> u.code.equals(code))
                    .findFirst();
            if (services.isPresent()) {
                return services.get().services.stream();
            }
            return Stream.empty();

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    public void updateServicesForUser(Uid uid, List<String> services) {
        UserServiceBody userService = new UserServiceBody();
        userService.code = uid.getNameHintValue();
        userService.services = services;

        UserServicesBody body = new UserServicesBody();
        body.users = new ArrayList<>(1);
        body.users.add(userService);

        callUpdate(USER_OBJECT_CLASS, userServicesEndpoint, uid, body);
    }

    // User-Organization

    public Stream<String> getOrganizationsForUser(Uid uid, int pageSize) {
        return getOrganizationsForUser(uid.getNameHintValue(), pageSize);
    }

    public Stream<String> getOrganizationsForUser(String code, int pageSize) {
        Map<String, String> params = new HashMap<>();
        params.put("code", code);

        try (Response response = get(userOrganizationsEndpoint, params)) {
            UserOrganizationsBody body = MAPPER.readValue(response.body().byteStream(), UserOrganizationsBody.class);
            return body.organizationTitles.stream()
                    .map(o -> {
                        if (o.title == null) {
                            return o.organization.code;
                        }
                        return o.organization.code + configuration.getOrganizationTitleDelimiter() + o.title.code;
                    });

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    public void updateOrganizationsForUser(Uid uid, List<String> organizations) {
        UserOrganizationUpdateBody userOrg = new UserOrganizationUpdateBody();
        userOrg.code = uid.getNameHintValue();
        userOrg.organizations = organizations.stream()
                .map(o -> {
                    OrganizationUpdateBody org = new OrganizationUpdateBody();

                    if (o.contains(configuration.getOrganizationTitleDelimiter())) {
                        String[] split = o.split(configuration.getOrganizationTitleDelimiter());
                        org.orgCode = split[0];
                        org.titleCode = split[1];
                    } else {
                        org.orgCode = o;
                    }

                    return org;
                })
                .collect(Collectors.toList());

        UserOrganizationsUpdateBody body = new UserOrganizationsUpdateBody();
        body.userOrganizations = new ArrayList<>(1);
        body.userOrganizations.add(userOrg);

        callUpdate(USER_OBJECT_CLASS, userOrganizationsUpdateEndpoint, uid, body);
    }

    // User-Group

    public void updateGroupsForUser(Uid uid, List<String> groups) {
        UserGroupsUpdateBody userGroups = new UserGroupsUpdateBody();
        userGroups.code = uid.getNameHintValue();
        userGroups.groups = groups;

        callUpdate(USER_OBJECT_CLASS, userGroupsEndpoint, uid, userGroups);
    }

    public Stream<String> getGroupsForUser(Uid uid, int pageSize) {
        return getGroupsForUser(uid.getNameHintValue(), pageSize);
    }

    public Stream<String> getGroupsForUser(String code, int pageSize) {
        Map<String, String> params = new HashMap<>();
        params.put("code", code);

        try (Response response = get(userGroupsEndpoint, params)) {
            UserGroupsBody body = MAPPER.readValue(response.body().byteStream(), UserGroupsBody.class);
            return body.groups.stream()
                    .map(o -> o.code);

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    // Organization

    public Uid createOrganization(KintoneOrganizationModel newOrganization) throws AlreadyExistsException {
        ListBody body = new ListBody();
        body.organizations = new ArrayList<>(1);
        body.organizations.add(newOrganization);

        callCreate(ORGANIZATION_OBJECT_CLASS, organizationEndpoint, body, newOrganization.code);

        // We need to fetch the created object for getting the generated id
        KintoneOrganizationModel created = getOrganization(new Name(newOrganization.code), null, null);

        return new Uid(created.id, newOrganization.code);
    }

    public void updateOrganization(Uid uid, KintoneOrganizationModel update) {
        ListBody body = new ListBody();
        body.organizations = new ArrayList<>(1);
        body.organizations.add(update);

        callUpdate(ORGANIZATION_OBJECT_CLASS, organizationEndpoint, uid, body);
    }

    public void renameOrganization(Uid uid, String newCode) {
        CodesBody<RenameCode> codes = new CodesBody<>();

        RenameCode renameCode = new RenameCode();
        renameCode.currentCode = uid.getNameHintValue();
        renameCode.newCode = newCode;

        codes.codes = new ArrayList<>(1);
        codes.codes.add(renameCode);

        callUpdate(ORGANIZATION_OBJECT_CLASS, organizationRenameEndpoint, uid, codes);
    }

    public Uid resolveOrganizationCode(Uid uid) {
        if (uid.getNameHint() != null) {
            return uid;
        } else {
            KintoneOrganizationModel organization = getOrganization(uid, null, null);
            return new Uid(uid.getUidValue(), organization.code);
        }
    }

    public KintoneOrganizationModel getOrganization(Uid uid, OperationOptions options, Set<String> fetchFieldsSet) throws UnknownUidException {
        Map<String, String> params = new HashMap<>();
        params.put("ids", uid.getUidValue());

        try (Response response = get(organizationEndpoint, params)) {
            try {
                ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                if (list.organizations == null || list.organizations.size() != 1) {
                    // Something wrong..
                    throw new ConnectorIOException(String.format("Cannot find %s organization %s", instanceName, uid.getUidValue()));
                }
                return list.organizations.get(0);

            } catch (IOException e) {
                throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
            }
        }
    }

    public KintoneOrganizationModel getOrganization(Name name, OperationOptions options, Set<String> fetchFieldsSet) {
        Map<String, String> params = new HashMap<>();
        params.put("codes", name.getNameValue());

        try (Response response = get(organizationEndpoint, params)) {
            try {
                ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                if (list.organizations == null || list.organizations.size() != 1) {
                    // Something wrong..
                    throw new ConnectorIOException(String.format("Cannot find %s organization %s", instanceName, name.getNameValue()));
                }
                return list.organizations.get(0);

            } catch (IOException e) {
                throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
            }
        }
    }

    public int getOrganizations(QueryHandler<KintoneOrganizationModel> handler, OperationOptions options, Set<String> fetchFieldsSet, int pageSize, int pageOffset) {
        // ConnId starts from 1, 0 means no offset (requested all data)
        if (pageOffset < 1) {
            return getAll(handler, pageSize, (start, size) -> {
                Map<String, String> params = new HashMap<>();
                params.put("offset", String.valueOf(start));
                params.put("size", String.valueOf(size));

                try (Response response = get(organizationEndpoint, params)) {
                    ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                    return list.organizations;

                } catch (IOException e) {
                    throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
                }
            });
        }

        // Pagination
        // Kintone starts from 0
        int start = pageOffset - 1;

        Map<String, String> params = new HashMap<>();
        params.put("offset", String.valueOf(start));
        params.put("size", String.valueOf(pageSize));

        try (Response response = get(organizationEndpoint, params)) {
            ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
            for (KintoneOrganizationModel org : list.organizations) {
                if (!handler.handle(org)) {
                    break;
                }
            }
            return list.organizations.size();

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    public void deleteOrganization(Uid uid) {
        Uid resolvedUid = resolveOrganizationCode(uid);

        CodesBody<String> codes = new CodesBody<>();
        codes.codes = new ArrayList<>(1);
        codes.codes.add(resolvedUid.getNameHintValue());

        callDelete(ORGANIZATION_OBJECT_CLASS, organizationEndpoint, uid, codes);
    }

    // Group

    public Uid createGroup(KintoneGroupModel newGroup) throws AlreadyExistsException {
        ListBody body = new ListBody();
        body.groups = new ArrayList<>(1);
        body.groups.add(newGroup);

        callCreate(GROUP_OBJECT_CLASS, groupEndpoint, body, newGroup.code);

        // We need to fetch the created object for getting the generated id
        KintoneGroupModel created = getGroup(new Name(newGroup.code), null, null);

        return new Uid(created.id, newGroup.code);
    }

    public void updateGroup(Uid uid, KintoneGroupModel update) {
        ListBody body = new ListBody();
        body.groups = new ArrayList<>(1);
        body.groups.add(update);

        callUpdate(GROUP_OBJECT_CLASS, groupEndpoint, uid, body);
    }

    public void renameGroup(Uid uid, String newCode) {
        CodesBody<RenameCode> codes = new CodesBody<>();

        RenameCode renameCode = new RenameCode();
        renameCode.currentCode = uid.getNameHintValue();
        renameCode.newCode = newCode;

        codes.codes = new ArrayList<>(1);
        codes.codes.add(renameCode);

        callUpdate(GROUP_OBJECT_CLASS, groupRenameEndpoint, uid, codes);
    }

    protected Uid resolveGroupCode(Uid uid) {
        if (uid.getNameHint() != null) {
            return uid;
        } else {
            KintoneGroupModel group = getGroup(uid, null, null);
            return new Uid(uid.getUidValue(), group.code);
        }
    }

    public KintoneGroupModel getGroup(Uid uid, OperationOptions options, Set<String> fetchFieldsSet) throws UnknownUidException {
        Map<String, String> params = new HashMap<>();
        params.put("ids", uid.getUidValue());

        try (Response response = get(groupEndpoint, params)) {
            ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
            if (list.groups == null || list.groups.size() != 1) {
                // Something wrong..
                throw new ConnectorIOException(String.format("Cannot find %s group %s", instanceName, uid.getUidValue()));
            }
            return list.groups.get(0);

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    public KintoneGroupModel getGroup(Name name, OperationOptions options, Set<String> fetchFieldsSet) {
        Map<String, String> params = new HashMap<>();
        params.put("codes", name.getNameValue());

        try (Response response = get(groupEndpoint, params)) {
            ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
            if (list.groups == null || list.groups.size() != 1) {
                // Something wrong..
                throw new ConnectorIOException(String.format("Cannot find %s group %s", instanceName, name.getNameValue()));
            }
            return list.groups.get(0);

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    public int getGroups(QueryHandler<KintoneGroupModel> handler, OperationOptions options, Set<String> fetchFieldsSet, int pageSize, int pageOffset) {
        // ConnId starts from 1, 0 means no offset (requested all data)
        if (pageOffset < 1) {
            return getAll(handler, pageSize, (start, size) -> {
                Map<String, String> params = new HashMap<>();
                params.put("offset", String.valueOf(start));
                params.put("size", String.valueOf(size));

                try (Response response = get(groupEndpoint, params)) {
                    ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                    return list.groups;

                } catch (IOException e) {
                    throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
                }
            });
        }

        // Pagination
        // Kintone starts from 0
        int start = pageOffset - 1;

        Map<String, String> params = new HashMap<>();
        params.put("offset", String.valueOf(start));
        params.put("size", String.valueOf(pageSize));

        try (Response response = get(groupEndpoint, params)) {
            ListBody list = MAPPER.readValue(response.body().byteStream(), ListBody.class);
            for (KintoneGroupModel group : list.groups) {
                if (!handler.handle(group)) {
                    break;
                }
            }
            return list.groups.size();

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
    }

    public void deleteGroup(Uid uid) {
        Uid resolvedUid = resolveGroupCode(uid);

        CodesBody<String> codes = new CodesBody<>();
        codes.codes = new ArrayList<>(1);
        codes.codes.add(resolvedUid.getNameHintValue());

        callDelete(GROUP_OBJECT_CLASS, groupEndpoint, uid, codes);
    }
}
