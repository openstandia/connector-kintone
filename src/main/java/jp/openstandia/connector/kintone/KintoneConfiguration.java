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

import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class KintoneConfiguration extends AbstractConfiguration {

    private String baseURL;
    private String loginName;
    private GuardedString password;
    private String httpProxyHost;
    private int httpProxyPort = 3128;
    private String httpProxyUser;
    private GuardedString httpProxyPassword;
    private int defaultQueryPageSize = 50;
    private int connectionTimeoutInMilliseconds = 10000;
    private int readTimeoutInMilliseconds = 10000;
    private int writeTimeoutInMilliseconds = 10000;
    private String[] userAttributesSchema = new String[]{};
    private String[] groupAttributesSchema = new String[]{};
    private Set<String> ignoreOrganization = new HashSet<>();
    private Set<String> ignoreGroup = new HashSet<>();
    private String organizationTitleDelimiter = "#";

    @ConfigurationProperty(
            order = 1,
            displayMessageKey = "Kintone Base URL",
            helpMessageKey = "Kintone Base URL.",
            required = true,
            confidential = false)
    public String getBaseURL() {
        return baseURL;
    }

    public void setBaseURL(String baseURL) {
        if (baseURL.endsWith("/")) {
            baseURL = baseURL.substring(0, baseURL.lastIndexOf("/") - 1);
        }
        this.baseURL = baseURL;
    }

    @ConfigurationProperty(
            order = 2,
            displayMessageKey = "Login Name",
            helpMessageKey = "Login Name of the kintone API.",
            required = false,
            confidential = false)
    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    @ConfigurationProperty(
            order = 3,
            displayMessageKey = "Password",
            helpMessageKey = "Password of the kintone API.",
            required = false,
            confidential = true)
    public GuardedString getPassword() {
        return password;
    }

    public void setPassword(GuardedString password) {
        this.password = password;
    }

    @ConfigurationProperty(
            order = 4,
            displayMessageKey = "HTTP Proxy Host",
            helpMessageKey = "Hostname for the HTTP Proxy.",
            required = false,
            confidential = false)
    public String getHttpProxyHost() {
        return httpProxyHost;
    }

    public void setHttpProxyHost(String httpProxyHost) {
        this.httpProxyHost = httpProxyHost;
    }

    @ConfigurationProperty(
            order = 5,
            displayMessageKey = "HTTP Proxy Port",
            helpMessageKey = "Port for the HTTP Proxy. (Default: 3128)",
            required = false,
            confidential = false)
    public int getHttpProxyPort() {
        return httpProxyPort;
    }

    public void setHttpProxyPort(int httpProxyPort) {
        this.httpProxyPort = httpProxyPort;
    }

    @ConfigurationProperty(
            order = 6,
            displayMessageKey = "HTTP Proxy User",
            helpMessageKey = "Username for the HTTP Proxy Authentication.",
            required = false,
            confidential = false)
    public String getHttpProxyUser() {
        return httpProxyUser;
    }

    public void setHttpProxyUser(String httpProxyUser) {
        this.httpProxyUser = httpProxyUser;
    }

    @ConfigurationProperty(
            order = 7,
            displayMessageKey = "HTTP Proxy Password",
            helpMessageKey = "Password for the HTTP Proxy Authentication.",
            required = false,
            confidential = true)
    public GuardedString getHttpProxyPassword() {
        return httpProxyPassword;
    }

    public void setHttpProxyPassword(GuardedString httpProxyPassword) {
        this.httpProxyPassword = httpProxyPassword;
    }

    @ConfigurationProperty(
            order = 8,
            displayMessageKey = "Default Query Page Size",
            helpMessageKey = "Number of results to return per page. (Default: 50)",
            required = false,
            confidential = false)
    public int getDefaultQueryPageSize() {
        return defaultQueryPageSize;
    }

    public void setDefaultQueryPageSize(int defaultQueryPageSize) {
        this.defaultQueryPageSize = defaultQueryPageSize;
    }

    @ConfigurationProperty(
            order = 9,
            displayMessageKey = "Connection Timeout (in milliseconds)",
            helpMessageKey = "Connection timeout when connecting to Kintone. (Default: 10000)",
            required = false,
            confidential = false)
    public int getConnectionTimeoutInMilliseconds() {
        return connectionTimeoutInMilliseconds;
    }

    public void setConnectionTimeoutInMilliseconds(int connectionTimeoutInMilliseconds) {
        this.connectionTimeoutInMilliseconds = connectionTimeoutInMilliseconds;
    }

    @ConfigurationProperty(
            order = 10,
            displayMessageKey = "Connection Read Timeout (in milliseconds)",
            helpMessageKey = "Connection read timeout when connecting to Kintone. (Default: 10000)",
            required = false,
            confidential = false)
    public int getReadTimeoutInMilliseconds() {
        return readTimeoutInMilliseconds;
    }

    public void setReadTimeoutInMilliseconds(int readTimeoutInMilliseconds) {
        this.readTimeoutInMilliseconds = readTimeoutInMilliseconds;
    }

    @ConfigurationProperty(
            order = 11,
            displayMessageKey = "Connection Write Timeout (in milliseconds)",
            helpMessageKey = "Connection write timeout when connecting to Kintone. (Default: 10000)",
            required = false,
            confidential = false)
    public int getWriteTimeoutInMilliseconds() {
        return writeTimeoutInMilliseconds;
    }

    public void setWriteTimeoutInMilliseconds(int writeTimeoutInMilliseconds) {
        this.writeTimeoutInMilliseconds = writeTimeoutInMilliseconds;
    }

    @ConfigurationProperty(
            order = 12,
            displayMessageKey = "User Attributes Schema",
            helpMessageKey = "Define schema for user attributes.",
            required = false,
            confidential = false)
    public String[] getUserAttributesSchema() {
        return userAttributesSchema;
    }

    public void setUserAttributesSchema(String[] userAttributesSchema) {
        this.userAttributesSchema = userAttributesSchema;
    }

    @ConfigurationProperty(
            order = 13,
            displayMessageKey = "Ignore Organization",
            helpMessageKey = "Define the organization code to be ignored when fetching organization membership. The code is case-sensitive.",
            required = false,
            confidential = false)
    public String[] getIgnoreOrganization() {
        return ignoreOrganization.toArray(new String[0]);
    }

    public void setIgnoreOrganization(String[] ignoreOrganization) {
        this.ignoreOrganization = Arrays.stream(ignoreOrganization).collect(Collectors.toSet());
    }

    public Set<String> getIgnoreOrganizationSet() {
        return ignoreOrganization;
    }

    @ConfigurationProperty(
            order = 14,
            displayMessageKey = "Ignore Group",
            helpMessageKey = "Define the group code to be ignored when fetching group membership. The code is case-sensitive.",
            required = false,
            confidential = false)
    public String[] getIgnoreGroup() {
        return ignoreGroup.toArray(new String[0]);
    }

    public void setIgnoreGroup(String[] ignoreGroup) {
        this.ignoreGroup = Arrays.stream(ignoreGroup).collect(Collectors.toSet());
    }

    public Set<String> getIgnoreGroupSet() {
        return ignoreGroup;
    }

    @ConfigurationProperty(
            order = 15,
            displayMessageKey = "Delimiter between organization and title",
            helpMessageKey = "Define the delimiter to separate the organisation code from the title code. (Default: #)",
            required = false,
            confidential = false)
    public String getOrganizationTitleDelimiter() {
        return organizationTitleDelimiter;
    }

    public void setOrganizationTitleDelimiter(String organizationTitleDelimiter) {
        this.organizationTitleDelimiter = organizationTitleDelimiter;
    }

    @Override
    public void validate() {
        if (baseURL == null) {
            throw new ConfigurationException("Kintone Base URL is required");
        }
        if (loginName == null) {
            throw new ConfigurationException("Kintone Application Name is required");
        }
        if (password == null) {
            throw new ConfigurationException("Kintone Application Password is required");
        }
    }
}
