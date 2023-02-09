package jp.openstandia.connector.kintone;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class KintoneOrganizationModel {
    public String id; // auto generated
    public String code;
    public String name;
    public String localName;
    public String localNameLocale;
    public String parentCode;
    public String description;

    @JsonIgnore
    public String newCode;

    public boolean hasAttributesChange() {
        return name != null ||
                localName != null ||
                localNameLocale != null ||
                parentCode != null ||
                description != null;
    }

    public boolean hasCodeChange() {
        return newCode != null;
    }
}