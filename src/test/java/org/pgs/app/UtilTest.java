package org.pgs.app;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class UtilTest {

    @Test
    public void isBlank_null_returnsTrue() {
        assertTrue(Util.isBlank(null));
    }

    @Test
    public void isBlank_emptyString_returnsTrue() {
        assertTrue(Util.isBlank(""));
    }

    @Test
    public void isBlank_whitespace_returnsTrue() {
        assertTrue(Util.isBlank("   \t\n"));
    }

    @Test
    public void isBlank_nonEmpty_returnsFalse() {
        assertFalse(Util.isBlank("hello"));
    }
}
