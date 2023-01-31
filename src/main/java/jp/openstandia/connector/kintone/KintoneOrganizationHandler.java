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

import java.util.Set;

import static jp.openstandia.connector.util.Utils.handleNullAsEmpty;
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.*;

public class KintoneOrganizationHandler implements ObjectHandler {

    public static final ObjectClass ORGANIZATION_OBJECT_CLASS = new ObjectClass("organization");

    private static final Log LOGGER = Log.getLog(KintoneOrganizationHandler.class);

    private final KintoneConfiguration configuration;
    private final KintoneRESTClient client;
    private final SchemaDefinition schema;

    public KintoneOrganizationHandler(KintoneConfiguration configuration, KintoneRESTClient client,
                                      SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema(KintoneConfiguration configuration, KintoneRESTClient client) {
        SchemaDefinition.Builder<KintoneOrganizationModel, KintoneOrganizationModel, KintoneOrganizationModel> sb
                = SchemaDefinition.newBuilder(ORGANIZATION_OBJECT_CLASS, KintoneOrganizationModel.class, KintoneOrganizationModel.class);

        // __UID__
        // The id for the organization. Must be unique and unchangeable.
        sb.addUid("organizationId",
                SchemaDefinition.Types.STRING_CASE_IGNORE,
                null,
                (source) -> source.id,
                "id",
                NOT_CREATABLE, NOT_UPDATEABLE
        );

        // code (__NAME__)
        // The name for the organization. Must be unique and changeable.
        // Also, it's case-sensitive.
        sb.addName("code",
                SchemaDefinition.Types.STRING_CASE_IGNORE,
                (source, dest) -> dest.code = source,
                (source, dest) -> dest.newCode = source,
                (source) -> source.code,
                null,
                REQUIRED, NOT_UPDATEABLE
        );

        // Attributes
        // The name (display name) is required.
        sb.add("name",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.name = source,
                (source) -> source.name,
                null,
                REQUIRED
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
        sb.add("parentCode",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.parentCode = handleNullAsEmpty(source),
                (source) -> source.parentCode,
                null
        );
        sb.add("description",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.description = handleNullAsEmpty(source),
                (source) -> source.description,
                null
        );

        LOGGER.ok("The constructed organization schema");

        return sb;
    }

    @Override
    public SchemaDefinition getSchema() {
        return schema;
    }

    @Override
    public Uid create(Set<Attribute> attributes) {
        KintoneOrganizationModel user = new KintoneOrganizationModel();
        KintoneOrganizationModel mapped = schema.apply(attributes, user);

        Uid newUid = client.createOrganization(mapped);

        return newUid;
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        KintoneOrganizationModel dest = new KintoneOrganizationModel();

        schema.applyDelta(modifications, dest);

        Uid resolvedUid = client.resolveOrganizationCode(uid);

        if (dest.hasAttributesChange()) {
            // Need to specify the current code for update
            dest.code = uid.getNameHintValue();
            client.updateOrganization(resolvedUid, dest);
        }

        if (dest.hasCodeChange()) {
            client.renameOrganization(resolvedUid, dest.newCode);
        }

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteOrganization(uid);
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                        Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        KintoneOrganizationModel group = client.getOrganization(uid, options, fetchFieldsSet);

        if (group != null) {
            resultsHandler.handle(toConnectorObject(schema, group, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getByName(Name name, ResultsHandler resultsHandler, OperationOptions options,
                         Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                         boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        KintoneOrganizationModel group = client.getOrganization(name, options, fetchFieldsSet);

        if (group != null) {
            resultsHandler.handle(toConnectorObject(schema, group, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getAll(ResultsHandler resultsHandler, OperationOptions options,
                      Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                      boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        return client.getOrganizations((g) -> resultsHandler.handle(toConnectorObject(schema, g, returnAttributesSet, allowPartialAttributeValues)),
                options, fetchFieldsSet, pageSize, pageOffset);
    }
}
