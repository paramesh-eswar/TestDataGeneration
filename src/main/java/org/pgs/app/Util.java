package org.pgs.app;

public final class Util {
    private Util() {}

    // null-safe blank check that mirrors Apache StringUtils.isBlank behavior for simple use.
    public static boolean isBlank(Object o) {
        if (o == null) return true;
        String s = o.toString();
        return s.trim().isEmpty();
    }
}
