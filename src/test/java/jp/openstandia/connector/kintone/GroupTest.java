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

import static jp.openstandia.connector.kintone.KintoneGroupHandler.GROUP_OBJECT_CLASS;
import static org.junit.jupiter.api.Assertions.*;

class GroupTest extends AbstractTest {

    @Test
    void addGroup() {
        // Given
        String groupId = "1";
        String code = "foo";
        String type = "static";
        String name = "FOO";
        String desc = "This is foo group.";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.build("type", type));
        attrs.add(AttributeBuilder.build("name", name));
        attrs.add(AttributeBuilder.build("description", desc));

        AtomicReference<KintoneGroupModel> created = new AtomicReference<>();
        mockClient.createGroup = ((g) -> {
            created.set(g);

            return new Uid(groupId, new Name(code));
        });

        // When
        Uid uid = connector.create(GROUP_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals(groupId, uid.getUidValue());
        assertEquals(code, uid.getNameHintValue());

        KintoneGroupModel newGroup = created.get();
        assertEquals(name, newGroup.name);
        assertEquals(desc, newGroup.description);
    }

    @Test
    void addGroupButAlreadyExists() {
        // Given
        String groupId = "1";
        String code = "foo";
        String type = "static";
        String name = "FOO";
        String desc = "This is foo group.";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.build("type", type));
        attrs.add(AttributeBuilder.build("name", name));
        attrs.add(AttributeBuilder.build("description", desc));

        mockClient.createGroup = ((g) -> {
            throw new AlreadyExistsException();
        });

        // When
        Throwable expect = null;
        try {
            Uid uid = connector.create(GROUP_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof AlreadyExistsException);
    }

    @Test
    void updateGroup() {
        // Given
        String currentId = "1";
        String currentCode = "foo";

        String code = "bar";
        String name = "BAR";
        String desc = "This is bar group.";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build(Name.NAME, code));
        modifications.add(AttributeDeltaBuilder.build("name", name));
        modifications.add(AttributeDeltaBuilder.build("description", desc));

        AtomicReference<Uid> targetUid1 = new AtomicReference<>();
        AtomicReference<KintoneGroupModel> updated = new AtomicReference<>();
        mockClient.updateGroup = ((u, g) -> {
            targetUid1.set(u);
            updated.set(g);
        });

        AtomicReference<Uid> targetUid2 = new AtomicReference<>();
        AtomicReference<String> targetNewCode = new AtomicReference<>();
        mockClient.renameGroup = ((u, c) -> {
            targetUid2.set(u);
            targetNewCode.set(c);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentId, targetUid1.get().getUidValue());
        assertEquals(currentCode, targetUid1.get().getNameHintValue());

        KintoneGroupModel updatedGroup = updated.get();
        assertNull(updatedGroup.id);
        assertEquals(currentCode, updatedGroup.code, "The code must be the current code for update");
        assertEquals(name, updatedGroup.name);
        assertEquals(desc, updatedGroup.description);

        assertEquals(currentId, targetUid2.get().getUidValue());
        assertEquals(currentCode, targetUid2.get().getNameHintValue());
        assertEquals(code, targetNewCode.get());
    }

    @Test
    void updateGroupWithNoValues() {
        // Given
        String currentId = "1";
        String currentCode = "foo";

        Set<AttributeDelta> modifications = new HashSet<>();
        // IDM sets empty list to remove the single value
        modifications.add(AttributeDeltaBuilder.build("description", Collections.emptyList()));

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<KintoneGroupModel> updated = new AtomicReference<>();
        mockClient.updateGroup = ((u, g) -> {
            targetUid.set(u);
            updated.set(g);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentId, targetUid.get().getUidValue());
        assertEquals(currentCode, targetUid.get().getNameHintValue());

        KintoneGroupModel updatedGroup = updated.get();
        assertNull(updatedGroup.id);
        assertEquals(currentCode, updatedGroup.code, "The code must be the current code for update");
        assertNull(updatedGroup.name);
        assertEquals("", updatedGroup.description);
    }

    @Test
    void renameGroup() {
        // Given
        String currentId = "1";
        String currentCode = "foo";

        String code = "bar";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build(Name.NAME, code));

        AtomicReference<Uid> targetUid1 = new AtomicReference<>();
        AtomicReference<String> targetNewCode = new AtomicReference<>();
        mockClient.renameGroup = ((u, c) -> {
            targetUid1.set(u);
            targetNewCode.set(c);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentId, targetUid1.get().getUidValue());
        assertEquals(currentCode, targetUid1.get().getNameHintValue());
        assertEquals(code, targetNewCode.get());
    }

    @Test
    void updateGroupButNotFound() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String desc = "This is foo group.";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("description", desc));

        mockClient.updateGroup = ((u, group) -> {
            throw new UnknownUidException();
        });

        // When
        Throwable expect = null;
        try {
            connector.updateDelta(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof UnknownUidException);
    }

    @Test
    void getGroupByUid() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String currentDesc = "";

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getGroupByUid = ((u) -> {
            targetUid.set(u);

            KintoneGroupModel result = new KintoneGroupModel();
            result.id = currentId;
            result.code = currentCode;
            result.name = currentName;
            result.description = currentDesc;
            return result;
        });

        // When
        ConnectorObject result = connector.getObject(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), defaultGetOperation());

        // Then
        assertEquals(GROUP_OBJECT_CLASS, result.getObjectClass());
        assertEquals(currentId, result.getUid().getUidValue());
        assertEquals(currentCode, result.getName().getNameValue());
        assertEquals(currentName, singleAttr(result, "name"));
        assertEquals(currentDesc, singleAttr(result, "description"));
    }

    @Test
    void getGroupByUidWithAttributes() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String currentDesc = "This is foo group.";

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getGroupByUid = ((u) -> {
            targetUid.set(u);

            KintoneGroupModel result = new KintoneGroupModel();
            result.id = currentId;
            result.code = currentCode;
            result.name = currentName;
            result.description = currentDesc;
            return result;
        });

        // When
        ConnectorObject result = connector.getObject(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), defaultGetOperation());

        // Then
        assertEquals(GROUP_OBJECT_CLASS, result.getObjectClass());
        assertEquals(currentId, result.getUid().getUidValue());
        assertEquals(currentCode, result.getName().getNameValue());
        assertEquals(currentName, singleAttr(result, "name"));
        assertEquals(currentDesc, singleAttr(result, "description"));
    }

    @Test
    void getGroups() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String currentDesc = "This is foo group.";

        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getGroups = ((h, size, offset) -> {
            targetPageSize.set(size);
            targetOffset.set(offset);

            KintoneGroupModel result = new KintoneGroupModel();
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
        connector.search(GROUP_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(1, results.size());
        ConnectorObject result = results.get(0);
        assertEquals(GROUP_OBJECT_CLASS, result.getObjectClass());
        assertEquals(currentId, result.getUid().getUidValue());
        assertEquals(currentCode, result.getName().getNameValue());
        assertEquals(currentDesc, singleAttr(result, "description"));

        assertEquals(20, targetPageSize.get(), "Not page size in the operation option");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void getGroupsZero() {
        // Given
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getGroups = ((h, size, offset) -> {
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
        connector.search(GROUP_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(0, results.size());
        assertEquals(20, targetPageSize.get(), "Not default page size in the configuration");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void getGroupsTwo() {
        // Given
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getGroups = ((h, size, offset) -> {
            targetPageSize.set(size);
            targetOffset.set(offset);

            KintoneGroupModel result = new KintoneGroupModel();
            result.id = "1";
            result.code = "a";
            result.name = "A";
            h.handle(result);

            result = new KintoneGroupModel();
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
        connector.search(GROUP_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(2, results.size());

        ConnectorObject result = results.get(0);
        assertEquals(GROUP_OBJECT_CLASS, result.getObjectClass());
        assertEquals("1", result.getUid().getUidValue());
        assertEquals("a", result.getName().getNameValue());

        result = results.get(1);
        assertEquals(GROUP_OBJECT_CLASS, result.getObjectClass());
        assertEquals("2", result.getUid().getUidValue());
        assertEquals("b", result.getName().getNameValue());

        assertEquals(20, targetPageSize.get(), "Not default page size in the configuration");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void deleteGroup() {
        // Given
        String currentId = "foo";
        String currentCode = "foo";

        AtomicReference<Uid> deleted = new AtomicReference<>();
        mockClient.deleteGroup = ((u) -> {
            deleted.set(u);
        });

        // When
        connector.delete(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(currentId, deleted.get().getUidValue());
        assertEquals(currentCode, deleted.get().getNameHintValue());
    }

    @Test
    void deleteGroupButNotFound() {
        // Given
        String currentId = "1";
        String currentCode = "foo";
        String currentName = "FOO";
        String desc = "This is foo group.";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("description", desc));

        mockClient.deleteGroup = ((u) -> {
            throw new UnknownUidException();
        });

        // When
        Throwable expect = null;
        try {
            connector.delete(GROUP_OBJECT_CLASS, new Uid(currentId, new Name(currentCode)), new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof UnknownUidException);
    }
}
