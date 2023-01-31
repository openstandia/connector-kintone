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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KintoneUserModel {
    public String id; // auto generated
    public String code;
    public String ctime;
    public String mtime;
    public Boolean valid;
    public String password;
    public String name;
    public String surName;
    public String givenName;
    public String surNameReading;
    public String givenNameReading;
    public String localName;
    public String localNameLocale;
    public String timezone;
    public String locale;
    public String description;
    public String phone;
    public String mobilePhone;
    public String extensionNumber;
    public String email;
    public String callto;
    public String url;
    public String employeeNumber;
    public String birthDate;
    public String joinDate;
    public String primaryOrganization; // The id of the organization
    public Object sortOrder; // The type is Number, but we need to set "" for remove the value
    public List<CustomItem> customItemValues;

    @JsonIgnore
    public String newCode;

    @JsonIgnore
    public List<String> addServices;
    @JsonIgnore
    public List<String> removeServices;

    @JsonIgnore
    public List<String> addOrganizations;
    @JsonIgnore
    public List<String> removeOrganizations;

    @JsonIgnore
    public List<String> addGroups;
    @JsonIgnore
    public List<String> removeGroups;

    public boolean hasCodeChange() {
        return newCode != null;
    }

    public boolean hasAttributesChange() {
        return valid != null ||
                password != null ||
                name != null ||
                surName != null ||
                givenName != null ||
                surNameReading != null ||
                givenNameReading != null ||
                localName != null ||
                localNameLocale != null ||
                timezone != null ||
                locale != null ||
                description != null ||
                phone != null ||
                mobilePhone != null ||
                extensionNumber != null ||
                email != null ||
                callto != null ||
                url != null ||
                employeeNumber != null ||
                birthDate != null ||
                joinDate != null ||
                primaryOrganization != null ||
                sortOrder != null ||
                customItemValues != null;
    }

    public boolean hasServiceChange() {
        return addServices != null || removeServices != null;
    }

    public boolean hasOrganizationChange() {
        return addOrganizations != null || removeOrganizations != null;
    }

    public boolean hasGroupChange() {
        return addGroups != null || removeGroups != null;
    }

    public static class CustomItem {
        public String code;
        public String value;
    }

    public void setCustomItem(String code, String value) {
        if (customItemValues == null) {
            customItemValues = new ArrayList<>();
        }
        CustomItem customValue = new CustomItem();
        customValue.code = code;
        customValue.value = value;

        customItemValues.add(customValue);
    }

    public String getCustomItem(String code) {
        if (customItemValues == null) {
            return null;
        }
        return customItemValues.stream().filter(c -> c.code.equals(code))
                .map(c -> c.value)
                .findFirst()
                .get();
    }

    public void addServices(List<String> services) {
        this.addServices = services;
    }

    public void removeServices(List<String> services) {
        this.removeServices = services;
    }

    public void addOrganizations(List<String> organizations) {
        this.addOrganizations = organizations;
    }

    public void removeOrganizations(List<String> organizations) {
        this.removeOrganizations = organizations;
    }

    public void addGroups(List<String> groups) {
        this.addGroups = groups;
    }

    public void removeGroups(List<String> groups) {
        this.removeGroups = groups;
    }
}
