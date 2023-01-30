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
    private static ErrorHandler ERROR_HANDLER = new KintoneErrorHandler();

    private String testEndpoint;
    private String userEndpoint;
    private String userRenameEndpoint;
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

    static class KintoneErrorHandler implements ErrorHandler {
        @Override
        public boolean inNotAuthenticated(Response response) {
            if (response.code() != 520) {
                return false;
            }
            try {
                ErrorResponse res = MAPPER.readValue(response.body().byteStream(), ErrorResponse.class);
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

            try {
                ErrorResponse res = MAPPER.readValue(response.body().byteStream(), ErrorResponse.class);
                if (!res.code.equals("CB_VA01")) {
                    return false;
                }

                if (res.errors.containsKey("users.code")) {
                    return hasExistsMessage(res.errors, "users.code");
                }
                if (res.errors.containsKey("groups.code")) {
                    return hasExistsMessage(res.errors, "groups.code");
                }
                if (res.errors.containsKey("organizations.code")) {
                    return hasExistsMessage(res.errors, "organizations.code");
                }
            } catch (IOException e) {
                throw new ConnectorIOException(e);
            }

            return false;
        }

        private boolean hasExistsMessage(Map<String, Map<String, List<String>>> error, String key) {
            Map<String, List<String>> messages = error.get(key);
            if (messages != null) {
                List<String> message = messages.get("message");
                Optional<String> found = message.stream().filter(m -> {
                            // The error message depends on the API user's locale
                            return m.endsWith("すでに登録されています") ||
                                    m.endsWith("already exists") ||
                                    m.endsWith("已存在");
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
            try {
                ListBody res = MAPPER.readValue(response.body().byteStream(), ListBody.class);
                if (res.users != null && res.users.isEmpty() &&
                        res.organizations != null && res.organizations.isEmpty() &&
                        res.groups != null && res.groups.isEmpty()) {
                    return true;
                }
                return false;

            } catch (IOException e) {
                throw new ConnectorIOException(e);
            }
        }

        @Override
        public boolean isOk(Response response) {
            return response.code() == 200 || response.code() == 204;
        }

        @Override
        public boolean isServerError(Response response) {
            return response.code() >= 500 || response.code() <= 599;
        }

    }

    public void init(String instanceName, KintoneConfiguration configuration, OkHttpClient httpClient) {
        super.init(instanceName, configuration, httpClient, ERROR_HANDLER, 0);
        this.testEndpoint = configuration.getBaseURL() + "/v1/users.json?size=1";
        this.userEndpoint = configuration.getBaseURL() + "/v1/users.json";
        this.userRenameEndpoint = configuration.getBaseURL() + "/v1/users/codes.json";
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
        callCreate(USER_OBJECT_CLASS, userEndpoint, newUser, newUser.code);

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
        // Need to specify the code for update
        update.code = uid.getNameHintValue();

        ListBody list = new ListBody();
        list.users = new ArrayList<>(1);
        list.users.add(update);

        callUpdate(USER_OBJECT_CLASS, userEndpoint, uid, list);
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
            return list.users.size();

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Cannot parse %s REST API Response", instanceName), e);
        }
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
                    .map(o -> o.organization + configuration.getOrganizationTitleDelimiter() + o.title.code);

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
        callCreate(ORGANIZATION_OBJECT_CLASS, organizationEndpoint, newOrganization, newOrganization.code);

        // We need to fetch the created object for getting the generated id
        KintoneGroupModel created = getGroup(new Name(newOrganization.code), null, null);

        return new Uid(created.id, newOrganization.code);
    }

    public void updateOrganization(Uid uid, KintoneOrganizationModel update) {
        ListBody list = new ListBody();
        list.organizations = new ArrayList<>(1);
        list.organizations.add(update);

        callUpdate(ORGANIZATION_OBJECT_CLASS, organizationEndpoint, uid, list);
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
        callCreate(GROUP_OBJECT_CLASS, groupEndpoint, newGroup, newGroup.code);

        // We need to fetch the created object for getting the generated id
        KintoneGroupModel created = getGroup(new Name(newGroup.code), null, null);

        return new Uid(created.id, newGroup.code);
    }

    public void updateGroup(Uid uid, KintoneGroupModel update) {
        ListBody list = new ListBody();
        list.groups = new ArrayList<>(1);
        list.groups.add(update);

        callUpdate(GROUP_OBJECT_CLASS, groupEndpoint, uid, list);
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
            try {
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
    }

    public KintoneGroupModel getGroup(Name name, OperationOptions options, Set<String> fetchFieldsSet) {
        Map<String, String> params = new HashMap<>();
        params.put("codes", name.getNameValue());

        try (Response response = get(groupEndpoint, params)) {
            try {
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
