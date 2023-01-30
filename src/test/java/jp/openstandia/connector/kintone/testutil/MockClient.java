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
package jp.openstandia.connector.kintone.testutil;

import jp.openstandia.connector.kintone.KintoneGroupModel;
import jp.openstandia.connector.kintone.KintoneRESTClient;
import jp.openstandia.connector.kintone.KintoneUserModel;
import jp.openstandia.connector.util.QueryHandler;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class MockClient extends KintoneRESTClient {

    private static MockClient INSTANCE = new MockClient();

    public MockFunction<KintoneUserModel, Uid> createUser;
    public MockBiConsumer<Uid, KintoneUserModel> updateUser;
    public MockBiConsumer<Uid, String> renameUser;
    public MockFunction<Uid, KintoneUserModel> getUserByUid;
    public MockFunction<Name, KintoneUserModel> getUserByName;
    public MockTripleFunction<QueryHandler<KintoneUserModel>, Integer, Integer, Integer> getUsers;
    public MockConsumer<Uid> deleteUser;

    public MockBiFunction<String, Integer, Stream<String>> getOrganizationsForUser;
    public MockBiConsumer<Uid, List<String>> updateOrganizationsForUser;

    public MockBiFunction<String, Integer, Stream<String>> getGroupsForUser;
    public MockBiConsumer<Uid, List<String>> updateGroupsForUser;

    public MockFunction<KintoneGroupModel, Uid> createGroup;
    public MockBiConsumer<Uid, KintoneGroupModel> updateGroup;
    public MockFunction<Uid, KintoneGroupModel> getGroupByUid;
    public MockFunction<Name, KintoneGroupModel> getGroupByName;
    public MockTripleFunction<QueryHandler<KintoneGroupModel>, Integer, Integer, Integer> getGroups;
    public MockBiFunction<String, Integer, Stream<String>> getGroupsForGroup;
    public MockConsumer<Uid> deleteGroup;

    public boolean closed = false;

    public void init() {
        INSTANCE = new MockClient();
    }

    private MockClient() {
    }

    public static MockClient instance() {
        return INSTANCE;
    }

    @Override
    public void test() {
    }

    @Override
    public void close() {
        closed = true;
    }

    // User

    @Override
    public Uid createUser(KintoneUserModel newUser) throws AlreadyExistsException {
        return createUser.apply(newUser);
    }

    @Override
    public void updateUser(Uid uid, KintoneUserModel update) {
        updateUser.accept(uid, update);
    }

    @Override
    public void renameUser(Uid uid, String newCode) {
        renameUser.accept(uid, newCode);
    }

    @Override
    public KintoneUserModel getUser(Uid uid, OperationOptions options, Set<String> fetchFieldsSet) throws UnknownUidException {
        return getUserByUid.apply(uid);
    }

    @Override
    public KintoneUserModel getUser(Name name, OperationOptions options, Set<String> fetchFieldsSet) throws UnknownUidException {
        return getUserByName.apply(name);
    }

    @Override
    public int getUsers(QueryHandler<KintoneUserModel> handler, OperationOptions options, Set<String> fetchFieldsSet, int pageSize, int pageOffset) {
        return getUsers.apply(handler, pageSize, pageOffset);
    }

    @Override
    public void deleteUser(Uid uid) {
        deleteUser.accept(uid);
    }

    // User-Organization

    @Override
    public Stream<String> getOrganizationsForUser(String code, int pageSize) {
        return getOrganizationsForUser.apply(code, pageSize);
    }

    @Override
    public void updateOrganizationsForUser(Uid uid, List<String> organizations) {
        updateOrganizationsForUser.accept(uid, organizations);
    }

    // User-Group

    @Override
    public Stream<String> getGroupsForUser(String code, int pageSize) {
        return getGroupsForUser.apply(code, pageSize);
    }

    @Override
    public void updateGroupsForUser(Uid uid, List<String> groups) {
        updateGroupsForUser.accept(uid, groups);
    }

    // Group

    @Override
    public Uid createGroup(KintoneGroupModel group) throws AlreadyExistsException {
        return createGroup.apply(group);
    }

    @Override
    public void updateGroup(Uid uid, KintoneGroupModel update) {
        updateGroup.accept(uid, update);
    }

    @Override
    public KintoneGroupModel getGroup(Uid uid, OperationOptions options, Set<String> fetchFieldsSet) {
        return getGroupByUid.apply(uid);
    }

    @Override
    public KintoneGroupModel getGroup(Name name, OperationOptions options, Set<String> fetchFieldsSet) {
        return getGroupByName.apply(name);
    }

    @Override
    public int getGroups(QueryHandler<KintoneGroupModel> handler, OperationOptions options, Set<String> fetchFieldsSet, int pageSize, int pageOffset) {
        return getGroups.apply(handler, pageSize, pageOffset);
    }

    @Override
    public void deleteGroup(Uid uid) {
        deleteGroup.accept(uid);
    }

    @FunctionalInterface
    public interface MockFunction<T, R> {
        R apply(T t);
    }

    @FunctionalInterface
    public interface MockBiFunction<T, U, R> {
        R apply(T t, U u);
    }

    @FunctionalInterface
    public interface MockTripleFunction<T, U, V, R> {
        R apply(T t, U u, V v);
    }

    @FunctionalInterface
    public interface MockConsumer<T> {
        void accept(T t);
    }

    @FunctionalInterface
    public interface MockBiConsumer<T, U> {
        void accept(T t, U u);
    }

    @FunctionalInterface
    public interface MockTripleConsumer<T, U, V> {
        void accept(T t, U u, V v);
    }
}
