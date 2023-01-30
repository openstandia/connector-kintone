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
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static jp.openstandia.connector.kintone.KintoneUserHandler.USER_OBJECT_CLASS;
import static jp.openstandia.connector.util.Utils.toZoneDateTime;
import static org.junit.jupiter.api.Assertions.*;

class UserTest extends AbstractTest {

    @Test
    void addUser() {
        // Given
        String userId = "12345";
        String code = "foo";
        String email = "foo@example.com";
        String name = "Foo Bar";
        String givenName = "Foo";
        String surName = "Bar";
        String password = "secret";
        List<String> organizations = list("org1", "org2");
        List<String> groups = list("group1", "group2");

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.buildEnabled(true));
        attrs.add(AttributeBuilder.buildPassword(password.toCharArray()));
        attrs.add(AttributeBuilder.build("email", email));
        attrs.add(AttributeBuilder.build("name", name));
        attrs.add(AttributeBuilder.build("givenName", givenName));
        attrs.add(AttributeBuilder.build("surName", surName));
        attrs.add(AttributeBuilder.build("organizations", organizations));
        attrs.add(AttributeBuilder.build("groups", groups));

        AtomicReference<KintoneUserModel> created = new AtomicReference<>();
        mockClient.createUser = ((user) -> {
            created.set(user);

            return new Uid(userId, new Name(code));
        });
        AtomicReference<Uid> targetOrgName = new AtomicReference<>();
        AtomicReference<List<String>> targetOrganizations = new AtomicReference<>();
        mockClient.updateOrganizationsForUser = ((u, o) -> {
            targetOrgName.set(u);
            targetOrganizations.set(o);
        });
        AtomicReference<Uid> targetGroupName = new AtomicReference<>();
        AtomicReference<List<String>> targetGroups = new AtomicReference<>();
        mockClient.updateGroupsForUser = ((u, g) -> {
            targetGroupName.set(u);
            targetGroups.set(g);
        });

        // When
        Uid uid = connector.create(USER_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals(userId, uid.getUidValue());
        assertEquals(code, uid.getNameHintValue());

        KintoneUserModel newUser = created.get();
        assertEquals(code, newUser.code);
        assertEquals(email, newUser.email);
        assertEquals(name, newUser.name);
        assertEquals(givenName, newUser.givenName);
        assertEquals(surName, newUser.surName);
        assertTrue(newUser.valid);
        assertEquals(password, newUser.password);

        assertEquals(code, targetOrgName.get().getNameHintValue());
        assertEquals(organizations, targetOrganizations.get());
        assertEquals(code, targetGroupName.get().getNameHintValue());
        assertEquals(groups, targetGroups.get());
    }

    @Test
    void addUserWithInactive() {
        // Given
        String userId = "12345";
        String code = "foo";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.buildEnabled(false));

        AtomicReference<KintoneUserModel> created = new AtomicReference<>();
        mockClient.createUser = ((user) -> {
            created.set(user);

            return new Uid(userId, new Name(code));
        });

        // When
        Uid uid = connector.create(USER_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals(userId, uid.getUidValue());
        assertEquals(code, uid.getNameHintValue());
        assertFalse(created.get().valid);
    }

    @Test
    void addUserButAlreadyExists() {
        // Given
        String code = "foo";
        String password = "secret";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.buildEnabled(true));
        attrs.add(AttributeBuilder.buildPassword(password.toCharArray()));

        mockClient.createUser = ((user) -> {
            throw new AlreadyExistsException("");
        });

        // When
        Throwable expect = null;
        try {
            Uid uid = connector.create(USER_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof AlreadyExistsException);
    }

    @Test
    void updateUser() {
        // Given
        String currentCode = "hoge";

        String userId = "12345";
        String code = "foo";
        String email = "foo@example.com";
        String name = "Foo Bar";
        String givenName = "Foo";
        String surName = "Bar";
        String password = "secret";
        List<String> organizations = list("org1", "org2");
        List<String> groups = list("group1", "group2");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build(Name.NAME, code));
        modifications.add(AttributeDeltaBuilder.build("name", name));
        modifications.add(AttributeDeltaBuilder.build("givenName", givenName));
        modifications.add(AttributeDeltaBuilder.build("surName", surName));
        modifications.add(AttributeDeltaBuilder.buildPassword(password.toCharArray()));
        modifications.add(AttributeDeltaBuilder.buildEnabled(true));
        modifications.add(AttributeDeltaBuilder.build("organizations", organizations, null));
        modifications.add(AttributeDeltaBuilder.build("groups", groups, null));

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<KintoneUserModel> updated = new AtomicReference<>();
        mockClient.updateUser = ((u, user) -> {
            targetUid.set(u);
            updated.set(user);
        });
        mockClient.getOrganizationsForUser = ((c, pageSize) -> {
            return Stream.empty();
        });
        AtomicReference<Uid> targetName1 = new AtomicReference<>();
        AtomicReference<List<String>> targetAddOrgs = new AtomicReference<>();
        mockClient.updateOrganizationsForUser = ((u, o) -> {
            targetName1.set(u);
            targetAddOrgs.set(o);
        });
        mockClient.getGroupsForUser = ((c, pageSize) -> {
            return Stream.empty();
        });
        AtomicReference<Uid> targetName2 = new AtomicReference<>();
        AtomicReference<List<String>> targetAddGroups = new AtomicReference<>();
        mockClient.updateGroupsForUser = ((u, g) -> {
            targetName2.set(u);
            targetAddGroups.set(g);
        });
        AtomicReference<Uid> targetName3 = new AtomicReference<>();
        AtomicReference<String> targetNewCode = new AtomicReference<>();
        mockClient.renameUser = ((u, n) -> {
            targetName3.set(u);
            targetNewCode.set(n);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(userId, targetUid.get().getUidValue());
        assertEquals(currentCode, targetUid.get().getNameHintValue());

        KintoneUserModel updatedUser = updated.get();
        assertEquals(name, updatedUser.name);
        assertEquals(givenName, updatedUser.givenName);
        assertEquals(surName, updatedUser.surName);
        assertTrue(updatedUser.valid);
        assertEquals(password, updatedUser.password);

        assertEquals(currentCode, targetName1.get().getNameHintValue());
        assertEquals(organizations, targetAddOrgs.get());

        assertEquals(currentCode, targetName2.get().getNameHintValue());
        assertEquals(groups, targetAddGroups.get());

        assertEquals(currentCode, targetName3.get().getNameHintValue());
        assertEquals(code, targetNewCode.get());
    }

    @Test
    void updateUserWithInactive() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        boolean active = false;

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.buildEnabled(active));

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<KintoneUserModel> updated = new AtomicReference<>();
        mockClient.updateUser = ((u, user) -> {
            targetUid.set(u);
            updated.set(user);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(userId, targetUid.get().getUidValue());

        KintoneUserModel updatedUser = updated.get();
        assertEquals(active, updatedUser.valid);
    }

    @Test
    void updateUserWithCustomItemValues() {
        // Apply custom configuration for this test
        configuration.setUserAttributesSchema(new String[]{"custom1", "custom2"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String currentCode = "foo";

        String userId = "12345";
        String custom1 = "abc";
        String custom2 = "efg";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("customItemValues.custom1", custom1));
        modifications.add(AttributeDeltaBuilder.build("customItemValues.custom2", custom2));

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<KintoneUserModel> updated = new AtomicReference<>();
        mockClient.updateUser = ((u, user) -> {
            targetUid.set(u);
            updated.set(user);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(userId, targetUid.get().getUidValue());

        assertEquals(currentCode, targetUid.get().getNameHintValue());
        assertNotNull(updated.get());
        KintoneUserModel updatedAttrs = updated.get();
        assertEquals(custom1, updatedAttrs.customItemValues.stream().filter(c -> c.code.equals("custom1")).findFirst().get().value);
        assertEquals(custom2, updatedAttrs.customItemValues.stream().filter(c -> c.code.equals("custom2")).findFirst().get().value);
    }

    @Test
    void updateUserWithNoValues() {
        // Apply custom configuration for this test
        configuration.setUserAttributesSchema(new String[]{"custom1"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String currentCode = "foo";

        String userId = "12345";
        String code = "foo";
        String email = "foo@example.com";
        String name = "Foo Bar";
        String givenName = "Foo";
        String surName = "Bar";
        String givenNameReading = "FooFoo";
        String surNameReading = "BarBar";
        boolean active = true;
        Date createdDate = Date.from(Instant.now());
        Date updatedDate = Date.from(Instant.now());
        String custom1 = "abc";

        Set<AttributeDelta> modifications = new HashSet<>();
        // IDM sets empty list to remove the single value
        modifications.add(AttributeDeltaBuilder.build("email", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("givenName", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("surName", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("customItemValues.custom1", Collections.emptyList()));

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<KintoneUserModel> updated = new AtomicReference<>();
        mockClient.updateUser = ((u, user) -> {
            targetUid.set(u);
            updated.set(user);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(userId, targetUid.get().getUidValue());
        assertEquals(code, targetUid.get().getNameHintValue());

        KintoneUserModel updatedUser = updated.get();
        // Kintone API treats empty string as removing the value
        assertEquals("", updatedUser.email);
        assertEquals("", updatedUser.givenName);
        assertEquals("", updatedUser.surName);
        assertNotNull(updatedUser.customItemValues);
        assertEquals(1, updatedUser.customItemValues.size());
        assertEquals("custom1", updatedUser.customItemValues.get(0).code);
        assertEquals("", updatedUser.customItemValues.get(0).value);
    }

    @Test
    void updateUserGroups() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        List<String> addGroups = list("group1", "group2");
        List<String> delGroups = list("group3", "group4");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("groups", addGroups, delGroups));

        mockClient.getGroupsForUser = ((code, pageSize) -> {
            List<String> groups = new ArrayList<>();
            groups.add("group3");
            groups.add("group4");
            groups.add("group5");
            return groups.stream();
        });
        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<List<String>> targetGroups = new AtomicReference<>();
        mockClient.updateGroupsForUser = ((u, g) -> {
            targetUid.set(u);
            targetGroups.set(g);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentCode, targetUid.get().getNameHintValue());
        assertEquals(list("group5", "group1", "group2"), targetGroups.get());
    }

    @Test
    void updateUserButNotFound() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        String email = "foo@example.com";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("email", email));

        mockClient.updateUser = ((u, user) -> {
            throw new UnknownUidException();
        });

        // When
        Throwable expect = null;
        try {
            connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof UnknownUidException);
    }

    @Test
    void getUserByUid() {
        // Given
        String userId = "12345";
        String code = "foo";
        String email = "foo@example.com";
        String name = "Foo Bar";
        String givenName = "Foo";
        String surName = "Bar";
        String givenNameReading = "FooFoo";
        String surNameReading = "BarBar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        String custom1 = "abc";
        List<String> groups = list("group1", "group2");

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getUserByUid = ((u) -> {
            targetUid.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.email = email;
            result.givenName = givenName;
            result.surName = surName;
            result.givenNameReading = givenNameReading;
            result.surNameReading = surNameReading;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            result.customItemValues = new ArrayList<>();
            KintoneUserModel.CustomItemValue value1 = new KintoneUserModel.CustomItemValue();
            value1.code = "custom1";
            value1.value = custom1;
            result.customItemValues.add(value1);
            return result;
        });
        AtomicReference<String> targetName = new AtomicReference<>();
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        mockClient.getGroupsForUser = ((u, size) -> {
            targetName.set(u);
            targetPageSize.set(size);

            return groups.stream();
        });

        // When
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), defaultGetOperation());

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(email, singleAttr(result, "email"));
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(givenName, singleAttr(result, "givenName"));
        assertEquals(surName, singleAttr(result, "surName"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertNull(result.getAttributeByName("groups"), "Unexpected returned groups even if not requested");
        assertNull(targetName.get());
        assertNull(targetPageSize.get());
    }

    @Test
    void getUserByUidWithAttributes() {
        // Apply custom configuration for this test
        configuration.setUserAttributesSchema(new String[]{"custom1", "custom2"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        String custom1 = "abc";
        String custom2 = "efg";

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getUserByUid = ((u) -> {
            targetUid.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            result.customItemValues = new ArrayList<>();
            KintoneUserModel.CustomItemValue value1 = new KintoneUserModel.CustomItemValue();
            value1.code = "custom1";
            value1.value = custom1;
            result.customItemValues.add(value1);
            KintoneUserModel.CustomItemValue value2 = new KintoneUserModel.CustomItemValue();
            value2.code = "custom2";
            value2.value = custom2;
            result.customItemValues.add(value2);
            return result;
        });

        // When
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), defaultGetOperation());

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertEquals(custom1, singleAttr(result, "customItemValues.custom1"));
        assertEquals(custom2, singleAttr(result, "customItemValues.custom2"));
    }

    @Test
    void getUserByUidWithEmpty() {
        // Apply custom configuration for this test
        configuration.setUserAttributesSchema(new String[]{"custom1", "custom2"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getUserByUid = ((u) -> {
            targetUid.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            result.customItemValues = new ArrayList<>();
            KintoneUserModel.CustomItemValue value1 = new KintoneUserModel.CustomItemValue();
            value1.code = "custom1";
            value1.value = "";
            result.customItemValues.add(value1);
            KintoneUserModel.CustomItemValue value2 = new KintoneUserModel.CustomItemValue();
            value2.code = "custom2";
            value2.value = "";
            result.customItemValues.add(value2);
            return result;
        });

        // When
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), defaultGetOperation());

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertEquals("", singleAttr(result, "customItemValues.custom1"));
        assertEquals("", singleAttr(result, "customItemValues.custom2"));
    }

    @Test
    void getUserByUidWithGroupsButNoOperation() {
        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        List<String> groups = list("group1", "group2");

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getUserByUid = ((u) -> {
            targetUid.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            return result;
        });
        AtomicReference<String> targetName = new AtomicReference<>();
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        mockClient.getGroupsForUser = ((u, size) -> {
            targetName.set(u);
            targetPageSize.set(size);

            return groups.stream();
        });

        // When
        // No operation options
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertNull(result.getAttributeByName("groups"), "Unexpected returned groups even if not requested");
        assertNull(targetName.get());
        assertNull(targetPageSize.get());
    }

    @Test
    void getUserByUidWithGroups() {
        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        List<String> groups = list("group1", "group2");

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getUserByUid = ((u) -> {
            targetUid.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            return result;
        });
        AtomicReference<String> targetName = new AtomicReference<>();
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        mockClient.getGroupsForUser = ((u, size) -> {
            targetName.set(u);
            targetPageSize.set(size);

            return groups.stream();
        });

        // When
        // Request "groups"
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), defaultGetOperation("groups"));

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertEquals(groups, multiAttr(result, "groups"));
        assertEquals(code, targetName.get());
        assertEquals(50, targetPageSize.get(), "Not default page size in the configuration");
    }

    @Test
    void getUserByUidWithGroupsWithIgnoreGroup() {
        // Apply custom configuration for this test
        configuration.setIgnoreGroup(new String[]{"NotManagedGroup"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        List<String> groups = list("group1", "NotManagedGroup", "group2");

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        mockClient.getUserByUid = ((u) -> {
            targetUid.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            return result;
        });
        AtomicReference<String> targetName = new AtomicReference<>();
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        mockClient.getGroupsForUser = ((u, size) -> {
            targetName.set(u);
            targetPageSize.set(size);

            return groups.stream();
        });

        // When
        // Request "groups"
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), defaultGetOperation("groups"));

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertEquals(list("group1", "group2"), multiAttr(result, "groups"));
        assertEquals(code, targetName.get());
        assertEquals(50, targetPageSize.get(), "Not default page size in the configuration");
    }

    @Test
    void getUserByName() {
        // Given
        String userId = "12345";
        String code = "foo";
        String email = "foo@example.com";
        String name = "Foo Bar";
        String givenName = "Foo";
        String surName = "Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";

        AtomicReference<Name> targetName = new AtomicReference<>();
        mockClient.getUserByName = ((u) -> {
            targetName.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            return result;
        });

        // When
        List<ConnectorObject> results = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            results.add(connectorObject);
            return true;
        };
        connector.search(USER_OBJECT_CLASS, FilterBuilder.equalTo(new Name(code)), handler, defaultSearchOperation());

        // Then
        assertEquals(1, results.size());
        ConnectorObject result = results.get(0);
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertNull(result.getAttributeByName("groups"), "Unexpected returned groups even if not requested");
    }

    @Test
    void getUserByNameWithGroupsWithPartialAttributeValues() {
        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";

        AtomicReference<Name> targetName = new AtomicReference<>();
        mockClient.getUserByName = ((u) -> {
            targetName.set(u);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            return result;
        });

        // When
        List<ConnectorObject> results = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            results.add(connectorObject);
            return true;
        };
        connector.search(USER_OBJECT_CLASS, FilterBuilder.equalTo(new Name(code)), handler, defaultSearchOperation("groups"));

        // Then
        assertEquals(1, results.size());
        ConnectorObject result = results.get(0);
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertTrue(isIncompleteAttribute(result.getAttributeByName("groups")));
    }

    @Test
    void getUsers() {
        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";

        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getUsers = ((h, size, offset) -> {
            targetPageSize.set(size);
            targetOffset.set(offset);

            KintoneUserModel result = new KintoneUserModel();
            result.id = userId;
            result.code = code;
            result.name = name;
            result.valid = active;
            result.ctime = createdDate;
            result.mtime = updatedDate;
            h.handle(result);

            return 1;
        });

        // When
        List<ConnectorObject> results = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            results.add(connectorObject);
            return true;
        };
        connector.search(USER_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(1, results.size());
        ConnectorObject result = results.get(0);
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertNull(result.getAttributeByName("groups"), "Unexpected returned groups even if not requested");

        assertEquals(20, targetPageSize.get(), "Not page size in the operation option");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void getUsersZero() {
        // Given
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getUsers = ((h, size, offset) -> {
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
        connector.search(USER_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(0, results.size());
        assertEquals(20, targetPageSize.get(), "Not default page size in the configuration");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void getUsersTwo() {
        // Given
        AtomicReference<Integer> targetPageSize = new AtomicReference<>();
        AtomicReference<Integer> targetOffset = new AtomicReference<>();
        mockClient.getUsers = ((h, size, offset) -> {
            targetPageSize.set(size);
            targetOffset.set(offset);

            KintoneUserModel result = new KintoneUserModel();
            result.id = "1";
            result.code = "a";
            result.name = "A";
            result.valid = true;
            result.ctime = "2023-01-30T08:29:29Z";
            result.mtime = "2023-01-30T10:15:10Z";
            h.handle(result);

            result = new KintoneUserModel();
            result.id = "2";
            result.code = "b";
            result.name = "B";
            result.valid = true;
            result.ctime = "2023-02-01T10:29:29Z";
            result.mtime = "2023-02-01T11:29:29Z";
            h.handle(result);

            return 2;
        });

        // When
        List<ConnectorObject> results = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            results.add(connectorObject);
            return true;
        };
        connector.search(USER_OBJECT_CLASS, null, handler, defaultSearchOperation());

        // Then
        assertEquals(2, results.size());

        ConnectorObject result = results.get(0);
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals("1", result.getUid().getUidValue());
        assertEquals("a", result.getName().getNameValue());

        result = results.get(1);
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals("2", result.getUid().getUidValue());
        assertEquals("b", result.getName().getNameValue());

        assertEquals(20, targetPageSize.get(), "Not default page size in the configuration");
        assertEquals(1, targetOffset.get());
    }

    @Test
    void deleteUser() {
        // Given
        String userId = "12345";
        String code = "foo";

        AtomicReference<Uid> deleted = new AtomicReference<>();
        mockClient.deleteUser = ((uid) -> {
            deleted.set(uid);
        });

        // When
        connector.delete(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(userId, deleted.get().getUidValue());
        assertEquals(code, deleted.get().getNameHintValue());
    }
}
