package com.github.ares.api.table.type;

public enum RowKind {
    // Note: Enums have no stable hash code across different JVMs, use toByteValue() for
    // this purpose.

    /** Insertion operation. */
    INSERT("+I", (byte) 0),

    /**
     * Update operation with the previous content of the updated row.
     *
     * <p>This kind SHOULD occur together with {@link #UPDATE_AFTER} for modelling an update that
     * needs to retract the previous row first. It is useful in cases of a non-idempotent update,
     * i.e., an update of a row that is not uniquely identifiable by a key.
     */
    UPDATE_BEFORE("-U", (byte) 1),

    /**
     * Update operation with new content of the updated row.
     *
     * <p>This kind CAN occur together with {@link #UPDATE_BEFORE} for modelling an update that
     * needs to retract the previous row first. OR it describes an idempotent update, i.e., an
     * update of a row that is uniquely identifiable by a key.
     */
    UPDATE_AFTER("+U", (byte) 2),

    /** Deletion operation. */
    DELETE("-D", (byte) 3);

    private final String shortString;

    private final byte value;

    /**
     * Creates a {@link RowKind} enum with the given short string and byte value representation of
     * the {@link RowKind}.
     */
    RowKind(String shortString, byte value) {
        this.shortString = shortString;
        this.value = value;
    }

    /**
     * Returns a short string representation of this {@link RowKind}.
     *
     * <p>
     *
     * <ul>
     *   <li>"+I" represents {@link #INSERT}.
     *   <li>"-U" represents {@link #UPDATE_BEFORE}.
     *   <li>"+U" represents {@link #UPDATE_AFTER}.
     *   <li>"-D" represents {@link #DELETE}.
     * </ul>
     */
    public String shortString() {
        return shortString;
    }

    /**
     * Returns the byte value representation of this {@link RowKind}. The byte value is used for
     * serialization and deserialization.
     *
     * <p>
     *
     * <ul>
     *   <li>"0" represents {@link #INSERT}.
     *   <li>"1" represents {@link #UPDATE_BEFORE}.
     *   <li>"2" represents {@link #UPDATE_AFTER}.
     *   <li>"3" represents {@link #DELETE}.
     * </ul>
     */
    public byte toByteValue() {
        return value;
    }

    /**
     * Creates a {@link RowKind} from the given byte value. Each {@link RowKind} has a byte value
     * representation.
     *
     * @see #toByteValue() for mapping of byte value and {@link RowKind}.
     */
    @SuppressWarnings("MagicNumber")
    public static RowKind fromByteValue(byte value) {
        switch (value) {
            case 0:
                return INSERT;
            case 1:
                return UPDATE_BEFORE;
            case 2:
                return UPDATE_AFTER;
            case 3:
                return DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for row kind.");
        }
    }
}