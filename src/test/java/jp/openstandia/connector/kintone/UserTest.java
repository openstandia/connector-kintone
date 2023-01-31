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

import java.time.ZonedDateTime;
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
        String password = "secret";
        String name = "Foo Bar";
        String surName = "Bar";
        String givenName = "Foo";
        String surNameReading = "BarBar";
        String givenNameReading = "FooFoo";
        String localName = "FOO BAR";
        String localNameLocale = "ja";
        String timezone = "Asia/Tokyo";
        String locale = "en";
        String description = "This is test user.";
        String phone = "0123";
        String mobilePhone = "4567";
        String extensionNumber = "890";
        String email = "foo@example.com";
        String callto = "foo-bar";
        String url = "https://example.com/foo";
        String employeeNumber = "emp001";
        ZonedDateTime birthDate = toZoneDateTime("1990-01-01");
        ZonedDateTime joinDate = toZoneDateTime("2014-04-01");
        Integer sortOrder = 10;
        List<String> services = list("kintone", "garoon");
        List<String> organizations = list("org1", "org2");
        List<String> groups = list("group1", "group2");

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(code));
        attrs.add(AttributeBuilder.buildEnabled(true));
        attrs.add(AttributeBuilder.buildPassword(password.toCharArray()));
        attrs.add(AttributeBuilder.build("name", name));
        attrs.add(AttributeBuilder.build("surName", surName));
        attrs.add(AttributeBuilder.build("givenName", givenName));
        attrs.add(AttributeBuilder.build("surNameReading", surNameReading));
        attrs.add(AttributeBuilder.build("givenNameReading", givenNameReading));
        attrs.add(AttributeBuilder.build("localName", localName));
        attrs.add(AttributeBuilder.build("localNameLocale", localNameLocale));
        attrs.add(AttributeBuilder.build("timezone", timezone));
        attrs.add(AttributeBuilder.build("locale", locale));
        attrs.add(AttributeBuilder.build("description", description));
        attrs.add(AttributeBuilder.build("phone", phone));
        attrs.add(AttributeBuilder.build("mobilePhone", mobilePhone));
        attrs.add(AttributeBuilder.build("extensionNumber", extensionNumber));
        attrs.add(AttributeBuilder.build("email", email));
        attrs.add(AttributeBuilder.build("callto", callto));
        attrs.add(AttributeBuilder.build("url", url));
        attrs.add(AttributeBuilder.build("employeeNumber", employeeNumber));
        attrs.add(AttributeBuilder.build("birthDate", birthDate));
        attrs.add(AttributeBuilder.build("joinDate", joinDate));
        attrs.add(AttributeBuilder.build("sortOrder", sortOrder));
        attrs.add(AttributeBuilder.build("services", services));
        attrs.add(AttributeBuilder.build("organizations", organizations));
        attrs.add(AttributeBuilder.build("groups", groups));

        AtomicReference<KintoneUserModel> created = new AtomicReference<>();
        mockClient.createUser = ((user) -> {
            created.set(user);

            return new Uid(userId, new Name(code));
        });
        AtomicReference<Uid> targetServiceName = new AtomicReference<>();
        AtomicReference<List<String>> targetServices = new AtomicReference<>();
        mockClient.updateServicesForUser = ((u, s) -> {
            targetServiceName.set(u);
            targetServices.set(s);
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
        assertTrue(newUser.valid);
        assertEquals(password, newUser.password);
        assertEquals(name, newUser.name);
        assertEquals(surName, newUser.surName);
        assertEquals(givenName, newUser.givenName);
        assertEquals(surNameReading, newUser.surNameReading);
        assertEquals(givenNameReading, newUser.givenNameReading);
        assertEquals(localName, newUser.localName);
        assertEquals(localNameLocale, newUser.localNameLocale);
        assertEquals(timezone, newUser.timezone);
        assertEquals(locale, newUser.locale);
        assertEquals(description, newUser.description);
        assertEquals(phone, newUser.phone);
        assertEquals(mobilePhone, newUser.mobilePhone);
        assertEquals(extensionNumber, newUser.extensionNumber);
        assertEquals(email, newUser.email);
        assertEquals(callto, newUser.callto);
        assertEquals(url, newUser.url);
        assertEquals(employeeNumber, newUser.employeeNumber);
        assertEquals(birthDate.format(DateTimeFormatter.ISO_LOCAL_DATE), newUser.birthDate);
        assertEquals(joinDate.format(DateTimeFormatter.ISO_LOCAL_DATE), newUser.joinDate);
        assertEquals(sortOrder, newUser.sortOrder);

        assertEquals(code, targetServiceName.get().getNameHintValue());
        assertEquals(services, targetServices.get());

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
        boolean active = true;
        String password = "secret";
        String name = "Foo Bar";
        String surName = "Bar";
        String givenName = "Foo";
        String surNameReading = "BarBar";
        String givenNameReading = "FooFoo";
        String localName = "FOO BAR";
        String localNameLocale = "ja";
        String timezone = "Asia/Tokyo";
        String locale = "en";
        String description = "This is test user.";
        String phone = "0123";
        String mobilePhone = "4567";
        String extensionNumber = "890";
        String email = "foo@example.com";
        String callto = "foo-bar";
        String url = "https://example.com/foo";
        String employeeNumber = "emp001";
        ZonedDateTime birthDate = toZoneDateTime("1990-01-01");
        ZonedDateTime joinDate = toZoneDateTime("2014-04-01");
        Integer sortOrder = 10;
        List<String> services = list("kintone", "garoon");
        List<String> organizations = list("org1", "org2");
        List<String> groups = list("group1", "group2");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build(Name.NAME, code));
        modifications.add(AttributeDeltaBuilder.buildEnabled(active));
        modifications.add(AttributeDeltaBuilder.buildPassword(password.toCharArray()));
        modifications.add(AttributeDeltaBuilder.build("name", name));
        modifications.add(AttributeDeltaBuilder.build("surName", surName));
        modifications.add(AttributeDeltaBuilder.build("givenName", givenName));
        modifications.add(AttributeDeltaBuilder.build("surNameReading", surNameReading));
        modifications.add(AttributeDeltaBuilder.build("givenNameReading", givenNameReading));
        modifications.add(AttributeDeltaBuilder.build("localName", localName));
        modifications.add(AttributeDeltaBuilder.build("localNameLocale", localNameLocale));
        modifications.add(AttributeDeltaBuilder.build("timezone", timezone));
        modifications.add(AttributeDeltaBuilder.build("locale", locale));
        modifications.add(AttributeDeltaBuilder.build("description", description));
        modifications.add(AttributeDeltaBuilder.build("phone", phone));
        modifications.add(AttributeDeltaBuilder.build("mobilePhone", mobilePhone));
        modifications.add(AttributeDeltaBuilder.build("extensionNumber", extensionNumber));
        modifications.add(AttributeDeltaBuilder.build("email", email));
        modifications.add(AttributeDeltaBuilder.build("callto", callto));
        modifications.add(AttributeDeltaBuilder.build("url", url));
        modifications.add(AttributeDeltaBuilder.build("employeeNumber", employeeNumber));
        modifications.add(AttributeDeltaBuilder.build("birthDate", birthDate));
        modifications.add(AttributeDeltaBuilder.build("joinDate", joinDate));
        modifications.add(AttributeDeltaBuilder.build("sortOrder", sortOrder));
        modifications.add(AttributeDeltaBuilder.build("services", services, null));
        modifications.add(AttributeDeltaBuilder.build("organizations", organizations, null));
        modifications.add(AttributeDeltaBuilder.build("groups", groups, null));

        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<KintoneUserModel> updated = new AtomicReference<>();
        mockClient.updateUser = ((u, user) -> {
            targetUid.set(u);
            updated.set(user);
        });

        mockClient.getServicesForUser = ((c, pageSize) -> {
            return Stream.empty();
        });
        AtomicReference<Uid> targetName1 = new AtomicReference<>();
        AtomicReference<List<String>> targetAddServices = new AtomicReference<>();
        mockClient.updateServicesForUser = ((u, s) -> {
            targetName1.set(u);
            targetAddServices.set(s);
        });

        mockClient.getOrganizationsForUser = ((c, pageSize) -> {
            return Stream.empty();
        });
        AtomicReference<Uid> targetName2 = new AtomicReference<>();
        AtomicReference<List<String>> targetAddOrgs = new AtomicReference<>();
        mockClient.updateOrganizationsForUser = ((u, o) -> {
            targetName2.set(u);
            targetAddOrgs.set(o);
        });

        mockClient.getGroupsForUser = ((c, pageSize) -> {
            return Stream.empty();
        });
        AtomicReference<Uid> targetName3 = new AtomicReference<>();
        AtomicReference<List<String>> targetAddGroups = new AtomicReference<>();
        mockClient.updateGroupsForUser = ((u, g) -> {
            targetName3.set(u);
            targetAddGroups.set(g);
        });

        AtomicReference<Uid> targetName4 = new AtomicReference<>();
        AtomicReference<String> targetNewCode = new AtomicReference<>();
        mockClient.renameUser = ((u, n) -> {
            targetName4.set(u);
            targetNewCode.set(n);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(userId, targetUid.get().getUidValue());
        assertEquals(currentCode, targetUid.get().getNameHintValue());

        KintoneUserModel updatedUser = updated.get();
        assertNull(updatedUser.id);
        assertEquals(currentCode, updatedUser.code, "The code must be the current code for update");
        assertTrue(updatedUser.valid);
        assertEquals(password, updatedUser.password);
        assertEquals(name, updatedUser.name);
        assertEquals(surName, updatedUser.surName);
        assertEquals(givenName, updatedUser.givenName);
        assertEquals(surNameReading, updatedUser.surNameReading);
        assertEquals(givenNameReading, updatedUser.givenNameReading);
        assertEquals(localName, updatedUser.localName);
        assertEquals(localNameLocale, updatedUser.localNameLocale);
        assertEquals(timezone, updatedUser.timezone);
        assertEquals(locale, updatedUser.locale);
        assertEquals(description, updatedUser.description);
        assertEquals(phone, updatedUser.phone);
        assertEquals(mobilePhone, updatedUser.mobilePhone);
        assertEquals(extensionNumber, updatedUser.extensionNumber);
        assertEquals(email, updatedUser.email);
        assertEquals(callto, updatedUser.callto);
        assertEquals(url, updatedUser.url);
        assertEquals(employeeNumber, updatedUser.employeeNumber);
        assertEquals(birthDate.format(DateTimeFormatter.ISO_LOCAL_DATE), updatedUser.birthDate);
        assertEquals(joinDate.format(DateTimeFormatter.ISO_LOCAL_DATE), updatedUser.joinDate);
        assertEquals(sortOrder, updatedUser.sortOrder);

        assertEquals(currentCode, targetName1.get().getNameHintValue());
        assertEquals(services, targetAddServices.get());

        assertEquals(currentCode, targetName2.get().getNameHintValue());
        assertEquals(organizations, targetAddOrgs.get());

        assertEquals(currentCode, targetName3.get().getNameHintValue());
        assertEquals(groups, targetAddGroups.get());

        assertEquals(currentCode, targetName4.get().getNameHintValue());
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
        assertNull(updatedUser.id);
        assertEquals(currentCode, updatedUser.code, "The code must be the current code for update");
        assertEquals(active, updatedUser.valid);
    }

    @Test
    void updateUserWithCustomItem() {
        // Apply custom configuration for this test
        configuration.setUserCustomItemSchema(new String[]{"custom1", "custom2"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String currentCode = "foo";

        String userId = "12345";
        String custom1 = "abc";
        String custom2 = "efg";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("customItem.custom1", custom1));
        modifications.add(AttributeDeltaBuilder.build("customItem.custom2", custom2));

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

        KintoneUserModel updatedUser = updated.get();

        assertNull(updatedUser.id);
        assertEquals(currentCode, updatedUser.code, "The code must be the current code for update");
        assertEquals(custom1, updatedUser.customItemValues.stream().filter(c -> c.code.equals("custom1")).findFirst().get().value);
        assertEquals(custom2, updatedUser.customItemValues.stream().filter(c -> c.code.equals("custom2")).findFirst().get().value);
    }

    @Test
    void updateUserWithNoValues() {
        // Apply custom configuration for this test
        configuration.setUserCustomItemSchema(new String[]{"custom1"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String currentCode = "foo";

        String userId = "12345";
        String code = "foo";

        Set<AttributeDelta> modifications = new HashSet<>();
        // IDM sets empty list to remove the single value
        modifications.add(AttributeDeltaBuilder.build("surName", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("givenName", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("surNameReading", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("givenNameReading", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("localName", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("localNameLocale", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("timezone", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("locale", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("description", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("phone", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("mobilePhone", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("extensionNumber", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("email", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("callto", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("url", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("employeeNumber", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("birthDate", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("joinDate", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("sortOrder", Collections.emptyList()));
        modifications.add(AttributeDeltaBuilder.build("customItem.custom1", Collections.emptyList()));

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
        assertNull(updatedUser.id);
        assertEquals(currentCode, updatedUser.code, "The code must be the current code for update");
        // Kintone API treats empty string as removing the value
        assertEquals("", updatedUser.givenName);
        assertEquals("", updatedUser.surName);
        assertEquals("", updatedUser.surNameReading);
        assertEquals("", updatedUser.givenNameReading);
        assertEquals("", updatedUser.localName);
        assertEquals("", updatedUser.localNameLocale);
        assertEquals("", updatedUser.timezone);
        assertEquals("", updatedUser.locale);
        assertEquals("", updatedUser.description);
        assertEquals("", updatedUser.phone);
        assertEquals("", updatedUser.mobilePhone);
        assertEquals("", updatedUser.extensionNumber);
        assertEquals("", updatedUser.email);
        assertEquals("", updatedUser.callto);
        assertEquals("", updatedUser.url);
        assertEquals("", updatedUser.employeeNumber);
        assertEquals("", updatedUser.birthDate);
        assertEquals("", updatedUser.joinDate);
        assertEquals("", updatedUser.sortOrder);
        assertNotNull(updatedUser.customItemValues);
        assertEquals(1, updatedUser.customItemValues.size());
        assertEquals("custom1", updatedUser.customItemValues.get(0).code);
        assertEquals("", updatedUser.customItemValues.get(0).value);
    }


    @Test
    void renameUser() {
        // Given
        String currentCode = "hoge";

        String userId = "12345";
        String code = "foo";

        List<String> groups = list("group1", "group2");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build(Name.NAME, code));

        AtomicReference<Uid> targetName1 = new AtomicReference<>();
        AtomicReference<String> targetNewCode = new AtomicReference<>();
        mockClient.renameUser = ((u, n) -> {
            targetName1.set(u);
            targetNewCode.set(n);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(userId, targetName1.get().getUidValue());
        assertEquals(currentCode, targetName1.get().getNameHintValue());
        assertEquals(code, targetNewCode.get());
    }

    @Test
    void updateUserServices() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        List<String> add = list("office");
        List<String> del = list("mailwise");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("services", add, del));

        mockClient.getServicesForUser = ((code, pageSize) -> {
            return Stream.of("kintone", "mailwise");
        });
        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<List<String>> targetServices = new AtomicReference<>();
        mockClient.updateServicesForUser = ((u, s) -> {
            targetUid.set(u);
            targetServices.set(s);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentCode, targetUid.get().getNameHintValue());
        assertEquals(list("kintone", "office"), targetServices.get());
    }

    @Test
    void clearUserServices() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        List<String> del = list("kintone");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("services", null, del));

        mockClient.getServicesForUser = ((code, pageSize) -> {
            return Stream.of("kintone");
        });
        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<List<String>> targetServices = new AtomicReference<>();
        mockClient.updateServicesForUser = ((u, s) -> {
            targetUid.set(u);
            targetServices.set(s);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentCode, targetUid.get().getNameHintValue());
        assertNotNull(targetServices.get());
        assertTrue(targetServices.get().isEmpty());
    }

    @Test
    void updateUserOrganizations() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        List<String> add = list("org1", "org2");
        List<String> del = list("org3", "org4");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("organizations", add, del));

        mockClient.getOrganizationsForUser = ((code, pageSize) -> {
            return Stream.of("org3", "org4", "org5");
        });
        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<List<String>> targetOrganizations = new AtomicReference<>();
        mockClient.updateOrganizationsForUser = ((u, o) -> {
            targetUid.set(u);
            targetOrganizations.set(o);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentCode, targetUid.get().getNameHintValue());
        assertEquals(list("org5", "org1", "org2"), targetOrganizations.get());
    }

    @Test
    void clearUserOrganizations() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        List<String> del = list("org1");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("organizations", null, del));

        mockClient.getOrganizationsForUser = ((code, pageSize) -> {
            return Stream.of("org1");
        });
        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<List<String>> targetOrganizations = new AtomicReference<>();
        mockClient.updateOrganizationsForUser = ((u, s) -> {
            targetUid.set(u);
            targetOrganizations.set(s);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentCode, targetUid.get().getNameHintValue());
        assertNotNull(targetOrganizations.get());
        assertTrue(targetOrganizations.get().isEmpty());
    }

    @Test
    void updateUserGroups() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        List<String> add = list("group1", "group2");
        List<String> del = list("group3", "group4");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("groups", add, del));

        mockClient.getGroupsForUser = ((code, pageSize) -> {
            return Stream.of("group3", "group4", "group5");
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
    void clearUserGroups() {
        // Given
        String currentCode = "foo";

        String userId = "12345";
        String name = "Foo Bar";
        List<String> del = list("group1");

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("groups", null, del));

        mockClient.getGroupsForUser = ((code, pageSize) -> {
            return Stream.of("group1");
        });
        AtomicReference<Uid> targetUid = new AtomicReference<>();
        AtomicReference<List<String>> targetGroups = new AtomicReference<>();
        mockClient.updateGroupsForUser = ((u, s) -> {
            targetUid.set(u);
            targetGroups.set(s);
        });

        // When
        Set<AttributeDelta> affected = connector.updateDelta(USER_OBJECT_CLASS, new Uid(userId, new Name(currentCode)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNull(affected);

        assertEquals(currentCode, targetUid.get().getNameHintValue());
        assertNotNull(targetGroups.get());
        assertTrue(targetGroups.get().isEmpty());
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
        boolean active = true;
        String name = "Foo Bar";
        String surName = "Bar";
        String givenName = "Foo";
        String surNameReading = "BarBar";
        String givenNameReading = "FooFoo";
        String localName = "FOO BAR";
        String localNameLocale = "ja";
        String timezone = "Asia/Tokyo";
        String locale = "en";
        String description = "This is test user.";
        String phone = "0123";
        String mobilePhone = "4567";
        String extensionNumber = "890";
        String email = "foo@example.com";
        String callto = "foo-bar";
        String url = "https://example.com/foo";
        String employeeNumber = "emp001";
        String birthDate = "1990-01-01";
        String joinDate = "2014-04-01";
        Integer sortOrder = 10;

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
            result.valid = active;
            result.name = name;
            result.surName = surName;
            result.givenName = givenName;
            result.surNameReading = surNameReading;
            result.givenNameReading = givenNameReading;
            result.localName = localName;
            result.localNameLocale = localNameLocale;
            result.timezone = timezone;
            result.locale = locale;
            result.description = description;
            result.phone = phone;
            result.mobilePhone = mobilePhone;
            result.extensionNumber = extensionNumber;
            result.email = email;
            result.callto = callto;
            result.url = url;
            result.employeeNumber = employeeNumber;
            result.birthDate = birthDate;
            result.joinDate = joinDate;
            result.sortOrder = sortOrder;

            result.ctime = createdDate;
            result.mtime = updatedDate;

            result.customItemValues = new ArrayList<>();
            KintoneUserModel.CustomItem value1 = new KintoneUserModel.CustomItem();
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
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(surName, singleAttr(result, "surName"));
        assertEquals(givenName, singleAttr(result, "givenName"));
        assertEquals(surNameReading, singleAttr(result, "surNameReading"));
        assertEquals(givenNameReading, singleAttr(result, "givenNameReading"));
        assertEquals(localName, singleAttr(result, "localName"));
        assertEquals(localNameLocale, singleAttr(result, "localNameLocale"));
        assertEquals(timezone, singleAttr(result, "timezone"));
        assertEquals(locale, singleAttr(result, "locale"));
        assertEquals(description, singleAttr(result, "description"));
        assertEquals(phone, singleAttr(result, "phone"));
        assertEquals(mobilePhone, singleAttr(result, "mobilePhone"));
        assertEquals(extensionNumber, singleAttr(result, "extensionNumber"));
        assertEquals(email, singleAttr(result, "email"));
        assertEquals(callto, singleAttr(result, "callto"));
        assertEquals(url, singleAttr(result, "url"));
        assertEquals(employeeNumber, singleAttr(result, "employeeNumber"));
        assertEquals(toZoneDateTime(birthDate), singleAttr(result, "birthDate"));
        assertEquals(toZoneDateTime(joinDate), singleAttr(result, "joinDate"));
        assertEquals(sortOrder, singleAttr(result, "sortOrder"));

        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));

        assertNull(result.getAttributeByName("customItem.custom1"), "Unexpected returned customItem.custom1 even if not configured");

        assertNull(result.getAttributeByName("groups"), "Unexpected returned groups even if not requested");
        assertNull(targetName.get());
        assertNull(targetPageSize.get());
    }

    @Test
    void getUserByUidWithCustomItem() {
        // Apply custom configuration for this test
        configuration.setUserCustomItemSchema(new String[]{"custom1", "custom2"});
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
            KintoneUserModel.CustomItem value1 = new KintoneUserModel.CustomItem();
            value1.code = "custom1";
            value1.value = custom1;
            result.customItemValues.add(value1);
            KintoneUserModel.CustomItem value2 = new KintoneUserModel.CustomItem();
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
        assertEquals(custom1, singleAttr(result, "customItem.custom1"));
        assertEquals(custom2, singleAttr(result, "customItem.custom2"));
    }

    @Test
    void getUserByUidWithEmpty() {
        // Apply custom configuration for this test
        configuration.setUserCustomItemSchema(new String[]{"custom1", "custom2"});
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
            result.email = "";
            result.ctime = createdDate;
            result.mtime = updatedDate;
            result.customItemValues = new ArrayList<>();
            KintoneUserModel.CustomItem value1 = new KintoneUserModel.CustomItem();
            value1.code = "custom1";
            value1.value = "";
            result.customItemValues.add(value1);
            KintoneUserModel.CustomItem value2 = new KintoneUserModel.CustomItem();
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
        assertEquals("", singleAttr(result, "email"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertEquals("", singleAttr(result, "customItem.custom1"));
        assertEquals("", singleAttr(result, "customItem.custom2"));
    }

    @Test
    void getUserByUidWithAssociationButNoOperation() {
        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        List<String> services = list("kintone", "garoon");
        List<String> organizations = list("org1", "org2");
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

        AtomicReference<String> targetName1 = new AtomicReference<>();
        mockClient.getServicesForUser = ((u, size) -> {
            targetName1.set(u);

            return services.stream();
        });

        AtomicReference<String> targetName2 = new AtomicReference<>();
        mockClient.getOrganizationsForUser = ((u, size) -> {
            targetName2.set(u);

            return organizations.stream();
        });

        AtomicReference<String> targetName3 = new AtomicReference<>();
        mockClient.getGroupsForUser = ((u, size) -> {
            targetName3.set(u);

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
        assertNull(result.getAttributeByName("services"), "Unexpected returned services even if not requested");
        assertNull(result.getAttributeByName("organizations"), "Unexpected returned organizations even if not requested");
        assertNull(result.getAttributeByName("groups"), "Unexpected returned groups even if not requested");
        assertNull(targetName1.get());
        assertNull(targetName2.get());
        assertNull(targetName3.get());
    }

    @Test
    void getUserByUidWithAssociation() {
        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        List<String> services = list("kintone", "garoon");
        List<String> organizations = list("org1", "org2");
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

        AtomicReference<String> targetName1 = new AtomicReference<>();
        AtomicReference<Integer> targetPageSize1 = new AtomicReference<>();
        mockClient.getServicesForUser = ((u, size) -> {
            targetName1.set(u);
            targetPageSize1.set(size);

            return services.stream();
        });

        AtomicReference<String> targetName2 = new AtomicReference<>();
        AtomicReference<Integer> targetPageSize2 = new AtomicReference<>();
        mockClient.getOrganizationsForUser = ((u, size) -> {
            targetName2.set(u);
            targetPageSize2.set(size);

            return organizations.stream();
        });

        AtomicReference<String> targetName3 = new AtomicReference<>();
        AtomicReference<Integer> targetPageSize3 = new AtomicReference<>();
        mockClient.getGroupsForUser = ((u, size) -> {
            targetName3.set(u);
            targetPageSize3.set(size);

            return groups.stream();
        });

        // When
        // Request association
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)),
                defaultGetOperation("services", "organizations", "groups"));

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertEquals(services, multiAttr(result, "services"));
        assertEquals(organizations, multiAttr(result, "organizations"));
        assertEquals(groups, multiAttr(result, "groups"));
        assertEquals(code, targetName1.get());
        assertEquals(code, targetName2.get());
        assertEquals(code, targetName3.get());
        assertEquals(50, targetPageSize1.get(), "Not default page size in the configuration");
        assertEquals(50, targetPageSize2.get(), "Not default page size in the configuration");
        assertEquals(50, targetPageSize2.get(), "Not default page size in the configuration");
    }

    @Test
    void getUserByUidWithAssociationWithIgnoreConfig() {
        // Apply configuration for this test
        configuration.setIgnoreService(new String[]{"kintone"});
        configuration.setIgnoreOrganization(new String[]{"NotManagedOrg"});
        configuration.setIgnoreGroup(new String[]{"NotManagedGroup"});
        ConnectorFacade connector = newFacade(configuration);

        // Given
        String userId = "12345";
        String code = "foo";
        String name = "Foo Bar";
        boolean active = true;
        String createdDate = "2023-01-30T08:29:29Z";
        String updatedDate = "2023-01-30T10:15:10Z";
        List<String> services = list("kintone", "garoon");
        List<String> organizations = list("org1", "NotManagedOrg", "org2");
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

        AtomicReference<String> targetName1 = new AtomicReference<>();
        mockClient.getServicesForUser = ((u, size) -> {
            targetName1.set(u);

            return services.stream();
        });

        AtomicReference<String> targetName2 = new AtomicReference<>();
        mockClient.getOrganizationsForUser = ((u, size) -> {
            targetName2.set(u);

            return organizations.stream();
        });

        AtomicReference<String> targetName3 = new AtomicReference<>();
        mockClient.getGroupsForUser = ((u, size) -> {
            targetName3.set(u);

            return groups.stream();
        });

        // When
        // Request association
        ConnectorObject result = connector.getObject(USER_OBJECT_CLASS, new Uid(userId, new Name(code)),
                defaultGetOperation("services", "organizations", "groups"));

        // Then
        assertEquals(USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(userId, result.getUid().getUidValue());
        assertEquals(code, result.getName().getNameValue());
        assertEquals(name, singleAttr(result, "name"));
        assertEquals(active, singleAttr(result, OperationalAttributes.ENABLE_NAME));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, createdDate), singleAttr(result, "ctime"));
        assertEquals(toZoneDateTime(DateTimeFormatter.ISO_INSTANT, updatedDate), singleAttr(result, "mtime"));
        assertEquals(list("garoon"), multiAttr(result, "services"));
        assertEquals(list("org1", "org2"), multiAttr(result, "organizations"));
        assertEquals(list("group1", "group2"), multiAttr(result, "groups"));
        assertEquals(code, targetName1.get());
        assertEquals(code, targetName2.get());
        assertEquals(code, targetName3.get());
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

    @Test
    void deleteUserButNotFound() {
        // Given
        String userId = "12345";
        String code = "foo";

        AtomicReference<Uid> deleted = new AtomicReference<>();
        mockClient.deleteUser = ((uid) -> {
            throw new UnknownUidException();
        });

        // When
        Throwable expect = null;
        try {
            connector.delete(USER_OBJECT_CLASS, new Uid(userId, new Name(code)), new OperationOptionsBuilder().build());
        } catch (Throwable t) {
            expect = t;
        }

        // Then
        assertNotNull(expect);
        assertTrue(expect instanceof UnknownUidException);
    }
}
