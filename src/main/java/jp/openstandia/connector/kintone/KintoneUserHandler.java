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
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static jp.openstandia.connector.util.Utils.*;
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.*;

public class KintoneUserHandler implements ObjectHandler {

    public static final ObjectClass USER_OBJECT_CLASS = new ObjectClass("user");

    private static final Log LOGGER = Log.getLog(KintoneUserHandler.class);
    private static final Set<String> SUPPORTED_TYPES = Arrays.asList("string").stream().collect(Collectors.toSet());

    private final KintoneConfiguration configuration;
    private final KintoneRESTClient client;
    private final SchemaDefinition schema;

    public KintoneUserHandler(KintoneConfiguration configuration, KintoneRESTClient client,
                              SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema(KintoneConfiguration configuration, KintoneRESTClient client) {
        SchemaDefinition.Builder<KintoneUserModel, KintoneUserModel, KintoneUserModel> sb
                = SchemaDefinition.newBuilder(USER_OBJECT_CLASS, KintoneUserModel.class, KintoneUserModel.class);

        // https://kintone.dev/en/docs/common/user-api/users/add-users/

        // __UID__
        // The id for the user. Must be unique and unchangeable.
        sb.addUid("userId",
                SchemaDefinition.Types.STRING_CASE_IGNORE,
                null,
                (source) -> source.id,
                "id",
                NOT_CREATABLE, NOT_UPDATEABLE
        );

        // code (__NAME__)
        // The name for the user. Must be unique and changeable.
        // Also, it's case-sensitive.
        sb.addName("code",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.code = source,
                (source, dest) -> dest.newCode = source,
                (source) -> source.code,
                null,
                REQUIRED
        );

        // __ENABLE__
        sb.add(OperationalAttributes.ENABLE_NAME,
                SchemaDefinition.Types.BOOLEAN,
                (source, dest) -> dest.valid = source,
                (source) -> source.valid,
                "active"
        );

        // __PASSWORD__
        sb.add(OperationalAttributes.PASSWORD_NAME,
                SchemaDefinition.Types.GUARDED_STRING,
                (source, dest) -> source.access(c -> dest.password = String.valueOf(c)),
                null,
                null,
                REQUIRED, NOT_READABLE, NOT_RETURNED_BY_DEFAULT
        );

        // Attributes
        // The name (displayName) is required.
        sb.add("name",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.name = handleNullAsEmpty(source),
                (source) -> source.name,
                null,
                REQUIRED
        );
        sb.add("surName",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.surName = handleNullAsEmpty(source),
                (source) -> source.surName,
                null
        );
        sb.add("givenName",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.givenName = handleNullAsEmpty(source),
                (source) -> source.givenName,
                null
        );
        sb.add("surNameReading",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.surNameReading = handleNullAsEmpty(source),
                (source) -> source.surNameReading,
                null
        );
        sb.add("givenNameReading",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.givenNameReading = handleNullAsEmpty(source),
                (source) -> source.givenNameReading,
                null
        );
        sb.add("localName",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.localName = handleNullAsEmpty(source),
                (source) -> source.localName,
                null
        );
        sb.add("localNameLocale",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.localNameLocale = handleNullAsEmpty(source),
                (source) -> source.localNameLocale,
                null
        );
        sb.add("timezone",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.timezone = handleNullAsEmpty(source),
                (source) -> source.timezone,
                null
        );
        sb.add("locale",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.locale = handleNullAsEmpty(source),
                (source) -> source.locale,
                null
        );
        sb.add("description",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.description = handleNullAsEmpty(source),
                (source) -> source.description,
                null
        );
        sb.add("phone",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.phone = handleNullAsEmpty(source),
                (source) -> source.phone,
                null
        );
        sb.add("mobilePhone",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.mobilePhone = handleNullAsEmpty(source),
                (source) -> source.mobilePhone,
                null
        );
        sb.add("extensionNumber",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.extensionNumber = handleNullAsEmpty(source),
                (source) -> source.extensionNumber,
                null
        );
        sb.add("email",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.email = handleNullAsEmpty(source),
                (source) -> source.email,
                null
        );
        sb.add("callto",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.callto = handleNullAsEmpty(source),
                (source) -> source.callto,
                null
        );
        sb.add("url",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.url = handleNullAsEmpty(source),
                (source) -> source.url,
                null
        );
        sb.add("employeeNumber",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.employeeNumber = handleNullAsEmpty(source),
                (source) -> source.employeeNumber,
                null
        );
        sb.add("birthDate",
                SchemaDefinition.Types.DATE_STRING,
                (source, dest) -> dest.birthDate = handleNullAsEmpty(source),
                (source) -> source.birthDate,
                null
        );
        sb.add("joinDate",
                SchemaDefinition.Types.DATE_STRING,
                (source, dest) -> dest.joinDate = handleNullAsEmpty(source),
                (source) -> source.joinDate,
                null
        );
        sb.add("sortOrder",
                SchemaDefinition.Types.INTEGER,
                (source, dest) -> {
                    if (source == null) {
                        // To handle remove sortOrder, use empty string
                        dest.sortOrder = "";
                    } else {
                        dest.sortOrder = source;
                    }
                },
                (source) -> (Integer) source.sortOrder,
                null
        );

        // Custom Attributes
        Arrays.stream(configuration.getUserCustomItemSchema())
                .forEach(x -> {
                    String attrName = x;
                    sb.add("customItem." + attrName,
                            SchemaDefinition.Types.STRING,
                            (source, dest) -> dest.setCustomItem(attrName, handleNullAsEmpty(source)),
                            (source, dest) -> dest.setCustomItem(attrName, handleNullAsEmpty(source)),
                            (source) -> source.getCustomItem(attrName),
                            null
                    );
                });

        // Association
        sb.addAsMultiple("services",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.addServices(source),
                (add, dest) -> dest.addServices(add),
                (remove, dest) -> dest.removeServices(remove),
                (source) -> filterService(configuration, client.getServicesForUser(source.code, configuration.getDefaultQueryPageSize())),
                null,
                NOT_RETURNED_BY_DEFAULT
        );
        sb.addAsMultiple("organizations",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.addOrganizations(source),
                (add, dest) -> dest.addOrganizations(add),
                (remove, dest) -> dest.removeOrganizations(remove),
                (source) -> filterOrganization(configuration, client.getOrganizationsForUser(source.code, configuration.getDefaultQueryPageSize())),
                null,
                NOT_RETURNED_BY_DEFAULT
        );
        sb.addAsMultiple("groups",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.addGroups(source),
                (add, dest) -> dest.addGroups(add),
                (remove, dest) -> dest.removeGroups(remove),
                (source) -> filterGroups(configuration, client.getGroupsForUser(source.code, configuration.getDefaultQueryPageSize())),
                null,
                NOT_RETURNED_BY_DEFAULT
        );

        // Metadata (readonly)
        sb.add("ctime",
                SchemaDefinition.Types.DATETIME,
                null,
                (source) -> toZoneDateTime(DateTimeFormatter.ISO_INSTANT, source.ctime),
                null,
                NOT_CREATABLE, NOT_UPDATEABLE
        );
        sb.add("mtime",
                SchemaDefinition.Types.DATETIME,
                null,
                (source) -> toZoneDateTime(DateTimeFormatter.ISO_INSTANT, source.mtime),
                null,
                NOT_CREATABLE, NOT_UPDATEABLE
        );

        LOGGER.ok("The constructed user schema");

        return sb;
    }

    private static Stream<String> filterService(KintoneConfiguration configuration, Stream<String> services) {
        Set<String> ignoreServices = configuration.getIgnoreServiceSet();
        return services.filter(g -> !ignoreServices.contains(g));
    }

    private static Stream<String> filterOrganization(KintoneConfiguration configuration, Stream<String> organizations) {
        Set<String> ignoreOrganizations = configuration.getIgnoreOrganizationSet();
        return organizations.filter(g -> !ignoreOrganizations.contains(g));
    }

    private static Stream<String> filterGroups(KintoneConfiguration configuration, Stream<String> groups) {
        Set<String> ignoreGroup = configuration.getIgnoreGroupSet();
        return groups.filter(g -> !ignoreGroup.contains(g))
                // Ignore Everyone group
                .filter(g -> !g.equals("everyone"));
    }

    @Override
    public SchemaDefinition getSchema() {
        return schema;
    }

    @Override
    public Uid create(Set<Attribute> attributes) {
        KintoneUserModel user = new KintoneUserModel();
        KintoneUserModel mapped = schema.apply(attributes, user);

        Uid newUid = client.createUser(mapped);

        if (mapped.addServices != null) {
            client.updateServicesForUser(newUid, mapped.addServices);
        }

        if (mapped.addOrganizations != null) {
            client.updateOrganizationsForUser(newUid, mapped.addOrganizations);
        }

        if (mapped.addGroups != null) {
            client.updateGroupsForUser(newUid, mapped.addGroups);
        }

        return newUid;
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        KintoneUserModel dest = new KintoneUserModel();

        schema.applyDelta(modifications, dest);

        Uid resolvedUid = client.resolveUserCode(uid);

        if (dest.hasAttributesChange()) {
            // Need to specify the current code for update
            dest.code = uid.getNameHintValue();
            client.updateUser(resolvedUid, dest);
        }

        // We need to fetch the current organizations
        if (dest.hasServiceChange()) {
            List<String> current = client.getServicesForUser(resolvedUid, resolvePageSize(options, configuration.getDefaultQueryPageSize()))
                    .collect(Collectors.toList());

            if (dest.addServices != null) {
                current.addAll(dest.addServices);
            }
            if (dest.removeServices != null) {
                current.removeAll(dest.removeServices);
            }

            client.updateServicesForUser(uid, current);
        }

        // We need to fetch the current organizations
        if (dest.hasOrganizationChange()) {
            List<String> current = client.getOrganizationsForUser(resolvedUid, resolvePageSize(options, configuration.getDefaultQueryPageSize()))
                    .collect(Collectors.toList());

            if (dest.addOrganizations != null) {
                current.addAll(dest.addOrganizations);
            }
            if (dest.removeOrganizations != null) {
                current.removeAll(dest.removeOrganizations);
            }

            client.updateOrganizationsForUser(uid, current);
        }

        // We need to fetch the current groups for update
        if (dest.hasGroupChange()) {
            List<String> current = client.getGroupsForUser(resolvedUid, resolvePageSize(options, configuration.getDefaultQueryPageSize()))
                    .collect(Collectors.toList());

            if (dest.addGroups != null) {
                current.addAll(dest.addGroups);
            }
            if (dest.removeGroups != null) {
                current.removeAll(dest.removeGroups);
            }

            client.updateGroupsForUser(uid, current);
        }

        if (dest.hasCodeChange()) {
            client.renameUser(resolvedUid, dest.newCode);
        }

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteUser(uid);
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                        Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        KintoneUserModel user = client.getUser(uid, options, fetchFieldsSet);

        if (user != null) {
            resultsHandler.handle(toConnectorObject(schema, user, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getByName(Name name, ResultsHandler resultsHandler, OperationOptions options,
                         Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                         boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        KintoneUserModel user = client.getUser(name, options, fetchFieldsSet);

        if (user != null) {
            resultsHandler.handle(toConnectorObject(schema, user, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getAll(ResultsHandler resultsHandler, OperationOptions options,
                      Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                      boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        return client.getUsers((u) -> resultsHandler.handle(toConnectorObject(schema, u, returnAttributesSet, allowPartialAttributeValues)),
                options, fetchFieldsSet, pageSize, pageOffset);
    }
}
