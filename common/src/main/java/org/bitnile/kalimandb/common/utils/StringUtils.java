package org.bitnile.kalimandb.common.utils;


public class StringUtils {
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkConfStr(String confStr) {
        if (isBlank(confStr)) {
            return false;
        }

        String[] addresses = confStr.split(",");
        if (addresses.length == 0) {
            return false;
        }

        for (String address : addresses) {
            String[] temp = address.split(":");
            if (temp.length != 2) {
                return false;
            }

            // ipv4
            if (temp[0].split("\\.").length != 4) {
                return false;
            }
        }

        return true;
    }
}
