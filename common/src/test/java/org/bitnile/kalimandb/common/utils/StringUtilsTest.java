package org.bitnile.kalimandb.common.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class StringUtilsTest {

    @Test
    public void isBlank() {
        assertTrue(StringUtils.isBlank(""));
        assertTrue(StringUtils.isBlank(null));
        assertTrue(StringUtils.isBlank("  "));
        assertFalse(StringUtils.isBlank("123123"));
    }

    @Test
    public void checkConfStr() {
        assertFalse(StringUtils.checkConfStr(""));
        assertFalse(StringUtils.checkConfStr(null));
        assertFalse(StringUtils.checkConfStr("  "));
        assertFalse(StringUtils.checkConfStr("123123"));

        assertFalse(StringUtils.checkConfStr("127.0.1:8899"));
        assertTrue(StringUtils.checkConfStr("127.0.0.1:8899"));
        assertFalse(StringUtils.checkConfStr("127.0.0.1"));
        assertTrue(StringUtils.checkConfStr("127.0.0.1:8899,127.0.0.1:9999"));
        assertFalse(StringUtils.checkConfStr("127.0.0.1:8899:127.0.0.1.9999,192.168.0.1:9984"));
    }
}