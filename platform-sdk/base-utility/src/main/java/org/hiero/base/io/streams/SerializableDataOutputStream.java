// SPDX-License-Identifier: Apache-2.0
package org.hiero.base.io.streams;

import static org.hiero.base.io.streams.SerializableStreamConstants.BOOLEAN_BYTES;
import static org.hiero.base.io.streams.SerializableStreamConstants.CLASS_ID_BYTES;
import static org.hiero.base.io.streams.SerializableStreamConstants.DEFAULT_CHECKSUM;
import static org.hiero.base.io.streams.SerializableStreamConstants.NULL_CLASS_ID;
import static org.hiero.base.io.streams.SerializableStreamConstants.NULL_INSTANT_EPOCH_SECOND;
import static org.hiero.base.io.streams.SerializableStreamConstants.NULL_LIST_ARRAY_LENGTH;
import static org.hiero.base.io.streams.SerializableStreamConstants.NULL_VERSION;
import static org.hiero.base.io.streams.SerializableStreamConstants.SERIALIZATION_PROTOCOL_VERSION;
import static org.hiero.base.io.streams.SerializableStreamConstants.VERSION_BYTES;

import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.io.SlimWriter;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.hiero.base.io.FunctionalSerialize;
import org.hiero.base.io.SelfSerializable;
import org.hiero.base.io.SerializableDet;
import org.hiero.base.io.SerializableWithKnownLength;
import org.hiero.base.utility.CommonUtils;

/**
 * A drop-in replacement for {@link java.io.DataOutputStream}, which handles SerializableDet classes specially.
 * It is designed for use with the SerializableDet interface, and its use is described there.
 * All writes are routed through a single {@link SlimWriter} buffer, eliminating any write-ordering hazard.
 */
public class SerializableDataOutputStream extends OutputStream {

    private final SlimWriter slimWriter;

    /**
     * Creates a new data output stream backed by the given {@link SlimWriter}.
     *
     * @param writer the underlying writer
     */
    public SerializableDataOutputStream(@NonNull final SlimWriter writer) {
        slimWriter = writer;
    }

    // -------------------------------------------------------------------------
    // Core write primitives (DataOutputStream-equivalent, all big-endian)
    // -------------------------------------------------------------------------

    @Override
    public void write(final int b) {
        slimWriter.writeByte((byte) b);
    }

    @Override
    public void write(@NonNull final byte[] b) {
        slimWriter.writeBytes(b);
    }

    @Override
    public void write(@NonNull final byte[] b, final int off, final int len) {
        slimWriter.writeBytes(b, off, len);
    }

    public void writeBoolean(final boolean v) throws IOException {
        slimWriter.writeByte((byte) (v ? 1 : 0));
    }

    public void writeByte(final int v) throws IOException {
        slimWriter.writeByte((byte) v);
    }

    public void writeShort(final int v) throws IOException {
        slimWriter.writeByte2((byte) (v >>> 8), (byte) v);
    }

    public void writeInt(final int v) throws IOException {
        slimWriter.writeInt(v); // delegates to writeIntBE
    }

    public void writeLong(final long v) throws IOException {
        slimWriter.writeLong(v); // delegates to writeLongBE
    }

    public void writeFloat(final float v) throws IOException {
        slimWriter.writeIntBE(Float.floatToRawIntBits(v));
    }

    public void writeDouble(final double v) throws IOException {
        slimWriter.writeLongBE(Double.doubleToRawLongBits(v));
    }

    /**
     * Returns the total number of bytes written to this stream so far.
     */
    public int size() {
        return slimWriter.position();
    }

    @Override
    public void flush() throws IOException {
        try {
            slimWriter.flush();
        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            slimWriter.close();
        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }

    // -------------------------------------------------------------------------
    // Augmented write helpers (formerly AugmentedDataOutputStream)
    // -------------------------------------------------------------------------

    /**
     * Writes a byte array to the stream. Can be null.
     *
     * @param data the array to write
     * @param writeChecksum whether to write the checksum or not
     * @throws IOException thrown if any IO problems occur
     */
    public void writeByteArray(@Nullable final byte[] data, final boolean writeChecksum) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
            return;
        }
        writeInt(data.length);
        if (writeChecksum) {
            writeInt(101 - data.length);
        }
        write(data);
    }

    /**
     * Writes a byte array to the stream. Can be null.
     *
     * @param data the array to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeByteArray(@Nullable final byte[] data) throws IOException {
        writeByteArray(data, DEFAULT_CHECKSUM);
    }

    /**
     * Writes an int array to the stream. Can be null.
     *
     * @param data the array to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeIntArray(@Nullable final int[] data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.length);
            for (final int datum : data) {
                writeInt(datum);
            }
        }
    }

    /**
     * Writes an int list to the stream. Can be null.
     *
     * @param data the list to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeIntList(@Nullable final List<Integer> data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.size());
            for (final int datum : data) {
                writeInt(datum);
            }
        }
    }

    /**
     * Writes a long array to the stream. Can be null.
     *
     * @param data the array to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeLongArray(@Nullable final long[] data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.length);
            for (final long datum : data) {
                writeLong(datum);
            }
        }
    }

    /**
     * Writes a long list to the stream. Can be null.
     *
     * @param data the list to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeLongList(@Nullable final List<Long> data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.size());
            for (final long datum : data) {
                writeLong(datum);
            }
        }
    }

    /**
     * Writes a boolean list to the stream. Can be null.
     *
     * @param data the list to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeBooleanList(@Nullable final List<Boolean> data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.size());
            for (final boolean datum : data) {
                writeBoolean(datum);
            }
        }
    }

    /**
     * Writes a float array to the stream. Can be null.
     *
     * @param data the array to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeFloatArray(@Nullable final float[] data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.length);
            for (final float datum : data) {
                writeFloat(datum);
            }
        }
    }

    /**
     * Writes a float list to the stream. Can be null.
     *
     * @param data the list to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeFloatList(@Nullable final List<Float> data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.size());
            for (final float datum : data) {
                writeFloat(datum);
            }
        }
    }

    /**
     * Writes a double array to the stream. Can be null.
     *
     * @param data the array to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeDoubleArray(@Nullable final double[] data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.length);
            for (final double datum : data) {
                writeDouble(datum);
            }
        }
    }

    /**
     * Writes a double list to the stream. Can be null.
     *
     * @param data the list to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeDoubleList(@Nullable final List<Double> data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.size());
            for (final double datum : data) {
                writeDouble(datum);
            }
        }
    }

    /**
     * Writes a String array to the stream. Can be null.
     *
     * @param data the array to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeStringArray(@Nullable final String[] data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.length);
            for (final String datum : data) {
                writeNormalisedString(datum);
            }
        }
    }

    /**
     * Writes a string list to the stream. Can be null.
     *
     * @param data the list to write
     * @throws IOException thrown if any IO problems occur
     */
    public void writeStringList(@Nullable final List<String> data) throws IOException {
        if (data == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
        } else {
            writeInt(data.size());
            for (final String datum : data) {
                writeNormalisedString(datum);
            }
        }
    }

    /**
     * Normalizes the string in accordance with the Swirlds default normalization method (NFD) and writes it
     * to the output stream encoded in the Swirlds default charset (UTF8).
     *
     * @param s the String to be converted and written
     * @throws IOException thrown if there are any problems during the operation
     */
    public void writeNormalisedString(@Nullable final String s) throws IOException {
        writeByteArray(CommonUtils.getNormalisedStringBytes(s));
    }

    /**
     * Write an Instant to the stream.
     *
     * @param instant the instant to write
     * @throws IOException thrown if there are any problems during the operation
     */
    public void writeInstant(@Nullable final Instant instant) throws IOException {
        if (instant == null) {
            writeLong(NULL_INSTANT_EPOCH_SECOND);
            return;
        }
        writeLong(instant.getEpochSecond());
        writeLong(instant.getNano());
    }

    // -------------------------------------------------------------------------
    // Static length helpers (formerly on AugmentedDataOutputStream)
    // -------------------------------------------------------------------------

    public static int getArraySerializedLength(@Nullable final long[] data) {
        int totalByteLength = Integer.BYTES;
        totalByteLength += (data == null) ? 0 : (data.length * Long.BYTES);
        return totalByteLength;
    }

    public static int getArraySerializedLength(@Nullable final int[] data) {
        int totalByteLength = Integer.BYTES;
        totalByteLength += (data == null) ? 0 : (data.length * Integer.BYTES);
        return totalByteLength;
    }

    public static int getArraySerializedLength(@Nullable final byte[] data) {
        return getArraySerializedLength(data, DEFAULT_CHECKSUM);
    }

    public static int getArraySerializedLength(@Nullable final byte[] data, final boolean writeChecksum) {
        int totalByteLength = Integer.BYTES;
        if (writeChecksum) {
            totalByteLength += Integer.BYTES;
        }
        totalByteLength += (data == null) ? 0 : data.length;
        return totalByteLength;
    }

    // -------------------------------------------------------------------------
    // SelfSerializable support
    // -------------------------------------------------------------------------

    /**
     * Write the serialization protocol version number to the stream.
     *
     * @throws IOException thrown if any IO problems occur
     */
    public void writeProtocolVersion() throws IOException {
        writeInt(SERIALIZATION_PROTOCOL_VERSION);
    }

    private void writeSerializable(
            @Nullable final SelfSerializable serializable,
            final boolean writeClassId,
            @NonNull final FunctionalSerialize serializeMethod)
            throws IOException {
        if (serializable == null) {
            if (writeClassId) {
                writeLong(NULL_CLASS_ID);
            } else {
                writeInt(NULL_VERSION);
            }
            return;
        }
        writeClassIdVersion(serializable, writeClassId);
        serializeMethod.serialize(this);
    }

    /**
     * Writes a {@link SelfSerializable} object to a stream.
     *
     * @param serializable the object to serialize
     * @param writeClassId whether to write the class ID or not
     * @throws IOException thrown if any IO problems occur
     */
    public void writeSerializable(@NonNull final SelfSerializable serializable, final boolean writeClassId)
            throws IOException {
        writeSerializable(serializable, writeClassId, serializable);
    }

    /**
     * Writes a list of objects returned by an {@link Iterator} when the size is known ahead of time.
     *
     * @param iterator the iterator that returns the data
     * @param size the size of the dataset
     * @param writeClassId whether to write the class ID or not
     * @param allSameClass should be set to true if all the objects in the list are the same class
     * @param <T> the type returned by the iterator
     * @throws IOException thrown if any IO problems occur
     */
    public <T extends SelfSerializable> void writeSerializableIterableWithSize(
            @NonNull final Iterator<T> iterator, final int size, final boolean writeClassId, final boolean allSameClass)
            throws IOException {
        writeInt(size);
        if (size == 0) {
            return;
        }
        writeBoolean(allSameClass);
        boolean classIdVersionWritten = false;
        while (iterator.hasNext()) {
            final SelfSerializable serializable = iterator.next();
            if (!allSameClass) {
                writeSerializable(serializable, writeClassId);
                continue;
            }
            if (serializable == null) {
                writeBoolean(true);
                continue;
            }
            writeBoolean(false);
            if (!classIdVersionWritten) {
                writeClassIdVersion(serializable, writeClassId);
                classIdVersionWritten = true;
            }
            serializable.serialize(this);
        }
    }

    /**
     * Writes a list of {@link SelfSerializable} objects to the stream.
     *
     * @param list the list to write, can be null
     * @param writeClassId set to true if the classID should be written
     * @param allSameClass should be set to true if all the objects in the list are the same class
     * @param <T> the class stored in the list
     * @throws IOException thrown if any IO problems occur
     */
    public <T extends SelfSerializable> void writeSerializableList(
            @Nullable final List<T> list, final boolean writeClassId, final boolean allSameClass) throws IOException {
        if (list == null) {
            writeInt(NULL_LIST_ARRAY_LENGTH);
            return;
        }
        writeSerializableIterableWithSize(list.iterator(), list.size(), writeClassId, allSameClass);
    }

    /**
     * Writes an array of {@link SelfSerializable} objects to the stream.
     *
     * @param array the array to write, can be null
     * @param writeClassId set to true if the classID should be written
     * @param allSameClass should be set to true if all the objects in the list are the same class
     * @param <T> the class stored in the list
     * @throws IOException thrown if any IO problems occur
     */
    public <T extends SelfSerializable> void writeSerializableArray(
            @Nullable final T[] array, final boolean writeClassId, final boolean allSameClass) throws IOException {
        if (array == null) {
            writeSerializableList(null, writeClassId, allSameClass);
        } else {
            writeSerializableList(Arrays.asList(array), writeClassId, allSameClass);
        }
    }

    public static <T extends SerializableWithKnownLength> int getSerializedLength(
            @Nullable final T[] array, final boolean writeClassId, final boolean allSameClass) {
        int totalByteLength = Integer.BYTES;
        if (array == null || array.length == 0) {
            return totalByteLength;
        }

        totalByteLength += BOOLEAN_BYTES;
        boolean classIdVersionWritten = false;
        for (final T t : array) {
            if (!allSameClass) {
                totalByteLength += getInstanceSerializedLength(t, true, writeClassId);
                continue;
            }
            if (t == null) {
                totalByteLength += BOOLEAN_BYTES;
                continue;
            }
            totalByteLength += BOOLEAN_BYTES;
            if (!classIdVersionWritten) {
                totalByteLength += VERSION_BYTES;
                if (writeClassId) {
                    totalByteLength += CLASS_ID_BYTES;
                }
                classIdVersionWritten = true;
            }
            totalByteLength += getInstanceSerializedLength(t, false, false);
        }

        return totalByteLength;
    }

    public static <T extends SerializableWithKnownLength> int getInstanceSerializedLength(
            @Nullable final T data, final boolean writeVersion, final boolean writeClassId) {
        if (data == null) {
            return writeClassId ? CLASS_ID_BYTES : writeVersion ? VERSION_BYTES : 0;
        }
        int totalByteLength = 0;
        if (writeClassId) {
            totalByteLength += CLASS_ID_BYTES;
        }
        if (writeVersion) {
            totalByteLength += VERSION_BYTES;
        }
        totalByteLength += data.getSerializedLength();
        return totalByteLength;
    }

    protected void writeClassIdVersion(@NonNull final SerializableDet serializable, final boolean writeClassId)
            throws IOException {
        if (writeClassId) {
            writeLong(serializable.getClassId());
        }
        writeInt(serializable.getVersion());
    }

    /**
     * Write a PBJ record to the stream.
     *
     * @param record the record to write
     * @param codec the codec to use to write the record
     * @param <T> the type of the record
     * @throws IOException thrown if any IO problems occur
     * @return the length in bytes that were written
     */
    public <T> long writePbjRecord(@NonNull final T record, @NonNull final Codec<T> codec) throws IOException {
        final int recordSize = codec.measureRecord(record);
        slimWriter.writeInt(recordSize);
        codec.write(record, slimWriter);
        slimWriter.flush();
        return recordSize + Integer.BYTES;
    }
}
