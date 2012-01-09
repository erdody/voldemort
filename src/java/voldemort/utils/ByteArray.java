package voldemort.utils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A byte array container that provides an equals and hashCode pair based on the
 * contents of the byte array. This is useful as a key for Maps.
 */
public final class ByteArray implements Serializable, Comparable<ByteArray> {

    private static final long serialVersionUID = 1L;

    public static final ByteArray EMPTY = new ByteArray();

    private final byte[] underlying;

    public ByteArray(byte... underlying) {
        this.underlying = Utils.notNull(underlying, "underlying");
    }

    public byte[] get() {
        return underlying;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(underlying);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof ByteArray))
            return false;
        ByteArray other = (ByteArray) obj;
        return Arrays.equals(underlying, other.underlying);
    }

    @Override
    public String toString() {
        return Arrays.toString(underlying);
    }

    public int length() {
        return underlying.length;
    }

    public int compareTo(ByteArray o) {
        return compare(this.get(), o.get());
    }

    public static int compare(byte[] key1, byte[] key2) {
        return compare(key1, 0, key1.length, key2, 0, key2.length);
    }

    public static int compare(byte[] key1,
                               int offset1,
                               int a1Len,
                               byte[] key2,
                               int offset2,
                               int a2Len) {
        int limit = Math.min(a1Len, a2Len);
        for(int i = 0; i < limit; i++) {
            byte b1 = key1[offset1 + i];
            byte b2 = key2[offset2 + i];
            if(b1 != b2) {
                // Remember, bytes are signed, so convert to shorts so that
                // we effectively do an unsigned byte comparison.
                return (b1 & 0xff) - (b2 & 0xff);
            }
        }
        return (a1Len - a2Len);
    }
}
