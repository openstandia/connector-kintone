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

public class KintoneGroupHandler implements ObjectHandler {

    public static final ObjectClass GROUP_OBJECT_CLASS = new ObjectClass("group");

    private static final Log LOGGER = Log.getLog(KintoneGroupHandler.class);

    private final KintoneConfiguration configuration;
    private final KintoneRESTClient client;
    private final SchemaDefinition schema;

    public KintoneGroupHandler(KintoneConfiguration configuration, KintoneRESTClient client,
                               SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema(KintoneConfiguration configuration, KintoneRESTClient client) {
        SchemaDefinition.Builder<KintoneGroupModel, KintoneGroupModel, KintoneGroupModel> sb
                = SchemaDefinition.newBuilder(GROUP_OBJECT_CLASS, KintoneGroupModel.class, KintoneGroupModel.class);

        // __UID__
        // The id for the group. Must be unique and unchangeable.
        sb.addUid("groupId",
                SchemaDefinition.Types.STRING_CASE_IGNORE,
                null,
                (source) -> source.id,
                "id",
                NOT_CREATABLE, NOT_UPDATEABLE
        );

        // code (__NAME__)
        // The name for the group. Must be unique and changeable.
        // Also, it's case-sensitive.
        sb.addName("code",
                SchemaDefinition.Types.STRING_CASE_IGNORE,
                (source, dest) -> dest.code = source,
                (source) -> source.code,
                null,
                REQUIRED, NOT_UPDATEABLE
        );

        // Attributes
        // The type is required, unchangeable and not readable.
        sb.add("type",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.type = source,
                null,
                null,
                REQUIRED, NOT_UPDATEABLE, NOT_READABLE, NOT_RETURNED_BY_DEFAULT
        );

        // The name (displayName) is required.
        sb.add("name",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.name = source,
                (source) -> source.name,
                null,
                REQUIRED
        );
        sb.add("description",
                SchemaDefinition.Types.STRING,
                (source, dest) -> dest.description = handleNullAsEmpty(source),
                (source) -> source.description,
                null
        );

        LOGGER.ok("The constructed group schema");

        return sb;
    }

    @Override
    public SchemaDefinition getSchema() {
        return schema;
    }

    @Override
    public Uid create(Set<Attribute> attributes) {
        KintoneGroupModel user = new KintoneGroupModel();
        KintoneGroupModel mapped = schema.apply(attributes, user);

        Uid newUid = client.createGroup(mapped);

        return newUid;
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        KintoneGroupModel dest = new KintoneGroupModel();

        schema.applyDelta(modifications, dest);

        Uid resolvedUid = client.resolveGroupCode(uid);

        if (dest.hasAttributesChange()) {
            client.updateGroup(resolvedUid, dest);
        }

        if (dest.hasCodeChange()) {
            client.renameGroup(resolvedUid, dest.code);
        }

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteGroup(uid);
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                        Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        KintoneGroupModel group = client.getGroup(uid, options, fetchFieldsSet);

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
        KintoneGroupModel group = client.getGroup(name, options, fetchFieldsSet);

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
        return client.getGroups((g) -> resultsHandler.handle(toConnectorObject(schema, g, returnAttributesSet, allowPartialAttributeValues)),
                options, fetchFieldsSet, pageSize, pageOffset);
    }
}
