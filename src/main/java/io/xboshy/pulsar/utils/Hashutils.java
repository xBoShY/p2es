package io.xboshy.pulsar.utils;

import java.security.MessageDigest;
import java.util.Formatter;

public class Hashutils {
    public static MessageDigest getMessageDigestSHA1() throws Exception {
        return MessageDigest.getInstance("SHA-1");
    }

    public static String SHA1(final MessageDigest md, final byte[] arr) {
        md.reset();
        return byteArray2Hex(md.digest(arr));
    }

    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
