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

import jp.openstandia.connector.kintone.testutil.AbstractTest;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static jp.openstandia.connector.kintone.KintoneOrganizationHandler.ORGANIZATION_OBJECT_CLASS;
import static org.junit.jupiter.api.Assertions.*;

class OrganizationTest extends AbstractTest {

    @Test
    void addOrganization() {
        // Given
        String organizationId = "1";
        String code = "foo";
        String name = "FOO";
        String localName = "FOO FOO";
        String localNameLocale = "en";
        String parentCode = "bar";
        String desc = "This is foo organization.";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.build("name", name));
        attrs.add(AttributeBuilder.build("localName", localName));
        attrs.add(AttributeBuilder.build("localNameLocale", localNameLocale));
        attrs.add(AttributeBuilder.build("parentCode", parentCode));
        attrs.add(AttributeBuilder.build("description", desc));

        AtomicReference<KintoneOrganizationModel> created = new AtomicReference<>();
        mockClient.createOrganization = ((g) -> {
            created.set(g);

            return new Uid(organizationId, new Name(code));
        });

        // When
        Uid uid = connector.create(ORGANIZATION_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals(organizationId, uid.getUidValue());
        assertEquals(code, uid.getNameHintValue());

        KintoneOrganizationModel newOrganization = created.get();
        assertEquals(name, newOrganization.name);
        assertEquals(localName, newOrganization.localName);
        assertEquals(localNameLocale, newOrganization.localNameLocale);
        assertEquals(parentCode, newOrganization.parentCode);
        assertEquals(desc, newOrganization.description);
    }

    @Test
    void addOrganizationButAlreadyExists() {
        // Given
        String organizationId = "1";
        String code = "foo";
        String name = "FOO";
        String desc = "This is foo organization.";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.build("name", name));
        attrs.add(AttributeBuilder.build("description", desc));

        mockClient.createOrganization = ((g) -> {
            throw new AlreadyExistsException();
        });

        // When
        Throwable expect = null;
        try {
            Uid uid = connector.create(ORGANIZATION_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof AlreadyExistsException);
    }

    @Test
    void updateOrganization() {
        // Given
        String currentId = "1";
        String currentCode = "foo";

        String code = "bar";
        String name = "BAR";
        String localName = "FOO FOO";
        String localNameLocale = "en";
        String parentCode = "bar";
        String desc = "This is bar organization.";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build(Name.NAME, code));
        modifications.add(AttributeDeltaBuilder.build("name", name));
        modifications.add(AttributeDeltaBuilder.build("localName", localName));
        modifications.add(AttributeDeltaBuilder.build("localNameLocale", localNameLocale));
        modifications.add(AttributeDeltaBuilder.build("parentCode", parentCode));
        modifications.add(AttributeDeltaBuilder.build("description", desc));

        AtomicReference<Uid> targetUid1 = new AtomicReference<>();
        AtomicReference<KintoneOrganizationModel> updated = new AtomicReference<>();
        mockClient.updateOrganization = ((u, g) -> {
            targetUid1.set(u);
            updated.set(g);
        });

        AtomicReference<Uid> targetUid2 = new AtomicReference<>();
        AtomicReference<String> targetNewCode = new AtomicReference<>();
        mockClient.renameOrganization = ((u, c) -> {
            targetUid2.set(u);
            targetNewCode.set(c);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentId, targetUid1.get().getUidValue());
        assertEquals(currentCode, targetUid1.get().getNameHintValue());

        KintoneOrganizationModel updatedOrganization = updated.get();
        assertNull(updatedOrganization.id);
        assertEquals(currentCode, updatedOrganization.code, "The code must be the current code for update");
        assertEquals(name, updatedOrganization.name);
        assertEquals(localName, updatedOrganization.localName);
        assertEquals(localNameLocale, updatedOrganization.localNameLocale);
        assertEquals(parentCode, updatedOrganization.parentCode);
        assertEquals(desc, updatedOrganization.description);

        assertEquals(currentId, targetUid2.get().getUidValue());
        assertEquals(currentCode, targetUid2.get().getNameHintValue());
        assertEquals(code, targetNewCode.get());
    }

    @Test
    void updateOrganizationWithNoValues() {
        // Given
        String currentId = "1";
        String currentCode = "foo";

        Set<AttributeDelta> modifications = new HashSet<>();
        // IDM sets empty list to remove the single value
        modifications.add(AttributeDeltaBuilder.build("localName", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("localNameLocale", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("parentCode", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("description", Collections.emptyList()));

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<KintoneOrganizationModel> updated = new AtomicReference<>();
        mockClient.updateOrganization = ((u, g) -> {
            targetUid.set(u);
            updated.set(g);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentId, targetUid.get().getUidValue());
        assertEquals(currentCode, targetUid.get().getNameHintValue());

        KintoneOrganizationModel updatedOrganization = updated.get();
        assertNull(updatedOrganization.id);
        assertEquals(currentCode, updatedOrganization.code, "The code must be the current code for update");
        assertNull(updatedOrganization.name);
        assertEquals("", updatedOrganization.localName);
        assertEquals("", updatedOrganization.localNameLocale);
        assertEquals("", updatedOrganization.parentCode);
        assertEquals("", updatedOrganization.description);
    }

    @Test
    void renameOrganization() {
        // Given
        String currentId = "1";
        String currentCode = "foo";

        String code = "bar";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build(Name.NAME, code));

        AtomicReference<Uid> targetUid1 = new AtomicReference<>();
        AtomicReference<String> targetNewCode = new AtomicReference<>();
        mockClient.renameOrganization = ((u, c) -> {
            targetUid1.set(u);
            targetNewCode.set(c);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentId, targetUid1.get().getUidValue());
        assertEquals(currentCode, targetUid1.get().getNameHintValue());
        assertEquals(code, targetNewCode.get());
    }

    @Test
    void updateOrganizationButNotFound() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String desc = "This is foo organization.";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("description", desc));

        mockClient.updateOrganization = ((u, organization) -> {
            throw new UnknownUidException();
        });

        // When
        Throwable expect = null;
        try {
            connector.updateDelta(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof UnknownUidException);
    }

    @Test
    void getOrganizationByUid() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String currentLocalName = "";
        String currentLocalNameLocale = "";
        String currentParentCode = "";
        String currentDesc = "";

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getOrganizationByUid = ((u) -> {
            targetUid.set(u);

            KintoneOrganizationModel result = new KintoneOrganizationModel();
            result.id = currentId;
            result.code = currentCode;
            result.name = currentName;
            result.localName = currentLocalName;
            result.localNameLocale = currentLocalNameLocale;
            result.parentCode = currentParentCode;
            result.description = currentDesc;
            return result;
        });

        // When
        ConnectorObject result = connector.getObject(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), defaultGetOperation());

        // Then
        assertEquals(ORGANIZATION_OBJECT_CLASS, result.getObjectClass());
        assertEquals(currentId, result.getUid().getUidValue());
        assertEquals(currentCode, result.getName().getNameValue());
        assertEquals(currentName, singleAttr(result, "name"));
        assertEquals(currentLocalName, singleAttr(result, "localName"));
        assertEquals(currentLocalNameLocale, singleAttr(result, "localNameLocale"));
        assertEquals(currentParentCode, singleAttr(result, "parentCode"));
        assertEquals(currentDesc, singleAttr(result, "description"));
    }

    @Test
    void getOrganizationByUidWithAttributes() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String currentLocalName = "FOO FOO";
        String currentLocalNameLocale = "en";
        String currentParentCode = "bar";
        String currentDesc = "This is foo organization.";

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getOrganizationByUid = ((u) -> {
            targetUid.set(u);

            KintoneOrganizationModel result = new KintoneOrganizationModel();
            result.id = currentId;
            result.code = currentCode;
            result.name = currentName;
            result.localName = currentLocalName;
            result.localNameLocale = currentLocalNameLocale;
            result.parentCode = currentParentCode;
            result.description = currentDesc;
            return result;
        });

        // When
        ConnectorObject result = connector.getObject(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), defaultGetOperation());

        // Then
        assertEquals(ORGANIZATION_OBJECT_CLASS, result.getObjectClass());
        assertEquals(currentId, result.getUid().getUidValue());
        assertEquals(currentCode, result.getName().getNameValue());
        assertEquals(currentName, singleAttr(result, "name"));
        assertEquals(currentLocalName, singleAttr(result, "localName"));
        assertEquals(currentLocalNameLocale, singleAttr(result, "localNameLocale"));
        assertEquals(currentParentCode, singleAttr(result, "parentCode"));
        assertEquals(currentDesc, singleAttr(result, "description"));
    }

    @Test
    void getOrganizations() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String currentDesc = "This is foo organization.";

        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getOrganizations = ((h, size, offset) -> {
            targetPageSize.set(size);
            targetOffset.set(offset);

            KintoneOrganizationModel result = new KintoneOrganizationModel();
            result.id = currentId;
            result.code = currentCode;
            result.name = currentName;
            result.description = currentDesc;
            h.handle(result);

            return 1;
        });

        // When
        List<ConnectorObject> results = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            results.add(connectorObject);
            return true;
        };
        connector.search(ORGANIZATION_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(1, results.size());
        ConnectorObject result = results.get(0);
        assertEquals(ORGANIZATION_OBJECT_CLASS, result.getObjectClass());
        assertEquals(currentId, result.getUid().getUidValue());
        assertEquals(currentCode, result.getName().getNameValue());
        assertEquals(currentDesc, singleAttr(result, "description"));

        assertEquals(20, targetPageSize.get(), "Not page size in the operation option");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void getOrganizationsZero() {
        // Given
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getOrganizations = ((h, size, offset) -> {
            targetPageSize.set(size);
            targetOffset.set(offset);

            return 0;
        });

        // When
        List<ConnectorObject> results = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            results.add(connectorObject);
            return true;
        };
        connector.search(ORGANIZATION_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(0, results.size());
        assertEquals(20, targetPageSize.get(), "Not default page size in the configuration");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void getOrganizationsTwo() {
        // Given
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getOrganizations = ((h, size, offset) -> {
            targetPageSize.set(size);
            targetOffset.set(offset);

            KintoneOrganizationModel result = new KintoneOrganizationModel();
            result.id = "1";
            result.code = "a";
            result.name = "A";
            h.handle(result);

            result = new KintoneOrganizationModel();
            result.id = "2";
            result.code = "b";
            result.name = "B";
            h.handle(result);

            return 2;
        });

        // When
        List<ConnectorObject> results = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            results.add(connectorObject);
            return true;
        };
        connector.search(ORGANIZATION_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(2, results.size());

        ConnectorObject result = results.get(0);
        assertEquals(ORGANIZATION_OBJECT_CLASS, result.getObjectClass());
        assertEquals("1", result.getUid().getUidValue());
        assertEquals("a", result.getName().getNameValue());

        result = results.get(1);
        assertEquals(ORGANIZATION_OBJECT_CLASS, result.getObjectClass());
        assertEquals("2", result.getUid().getUidValue());
        assertEquals("b", result.getName().getNameValue());

        assertEquals(20, targetPageSize.get(), "Not default page size in the configuration");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void deleteOrganization() {
        // Given
        String currentId = "foo";
        String currentCode = "foo";

        AtomicReference<Uid> deleted = new AtomicReference<>();
        mockClient.deleteOrganization = ((u) -> {
            deleted.set(u);
        });

        // When
        connector.delete(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(currentId, deleted.get().getUidValue());
        assertEquals(currentCode, deleted.get().getNameHintValue());
    }

    @Test
    void deleteOrganizationButNotFound() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String desc = "This is foo organization.";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("description", desc));

        mockClient.deleteOrganization = ((u) -> {
            throw new UnknownUidException();
        });

        // When
        Throwable expect = null;
        try {
            connector.delete(ORGANIZATION_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof UnknownUidException);
    }
}
