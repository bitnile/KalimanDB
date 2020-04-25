package org.bitnile.kalimandb.common.protocol;


public enum ServiceEnum {
    INSERT      ((byte)1, "insert"),
    UPDATE      ((byte)2, "update"),
    DELETE      ((byte)3, "delete"),
    FIND        ((byte)4, "find"),
    FIND_STW    ((byte)5, "findStartWith"),
    ERROR       ((byte)-1, "error");

    private byte code;
    private String name;

    ServiceEnum(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public static ServiceEnum valueOf(byte code) {
        for (ServiceEnum value : ServiceEnum.values()) {
            if (value.getCode() == code) {
                return value;
            }
        }
        return ERROR;
    }

    public static byte name2Code(String name) {
        switch (name) {
            case "insert":
                return INSERT.getCode();
            case "update":
                return UPDATE.getCode();
            case "delete":
                return DELETE.getCode();
            case "find":
                return FIND.getCode();
            case "findStartWith":
                return FIND_STW.getCode();
            default:
                return ERROR.getCode();
        }
    }

}
