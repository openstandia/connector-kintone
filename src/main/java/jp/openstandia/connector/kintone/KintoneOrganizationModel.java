package jp.openstandia.connector.kintone;

public class KintoneOrganizationModel {
    public String id; // auto generated
    public String code;
    public String name;
    public String localName;
    public String localNameLocale;
    public String parentCode;
    public String description;

    public boolean hasAttributesChange() {
        return name != null ||
                localName != null ||
                localNameLocale != null ||
                parentCode != null ||
                description != null;
    }

    public boolean hasCodeChange() {
        return this.code != null;
    }
}