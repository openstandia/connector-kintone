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
    public Object sortOrder;
    public List<CustomItemValue> customItemValues;

    @JsonIgnore
    public List<String> addOrganizations;
    @JsonIgnore
    public List<String> removeOrganizations;

    @JsonIgnore
    public List<String> addGroups;
    @JsonIgnore
    public List<String> removeGroups;

    public boolean hasCodeChange() {
        return this.code != null;
    }

    public boolean hasAttributesChange() {
        return this.valid != null ||
                this.password != null ||
                this.name != null ||
                this.surName != null ||
                this.givenName != null ||
                this.surNameReading != null ||
                this.givenNameReading != null ||
                this.localName != null ||
                this.localNameLocale != null ||
                this.timezone != null ||
                this.locale != null ||
                this.description != null ||
                this.phone != null ||
                this.mobilePhone != null ||
                this.extensionNumber != null ||
                this.email != null ||
                this.callto != null ||
                this.url != null ||
                this.employeeNumber != null ||
                this.birthDate != null ||
                this.joinDate != null ||
                this.primaryOrganization != null ||
                this.sortOrder != null ||
                this.customItemValues != null;
    }

    public boolean hasOrganizationChange() {
        return this.addOrganizations != null || this.removeOrganizations != null;
    }

    public boolean hasGroupChange() {
        return this.addGroups != null || this.removeGroups != null;
    }

    public static class CustomItemValue {
        public String code;
        public String value;
    }

    public void setCustomItemValue(String code, String value) {
        if (customItemValues == null) {
            customItemValues = new ArrayList<>();
        }
        CustomItemValue customValue = new CustomItemValue();
        customValue.code = code;
        customValue.value = value;

        customItemValues.add(customValue);
    }

    public String getCustomItemValue(String code) {
        if (customItemValues == null) {
            return null;
        }
        return customItemValues.stream().filter(c -> c.code.equals(code))
                .map(c -> c.value)
                .findFirst()
                .get();
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
