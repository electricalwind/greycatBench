package greycat.bench.graphbench;

import greycat.Constants;
import greycat.Type;
import greycat.internal.CoreConstants;
import greycat.struct.Buffer;
import greycat.utility.Base64;

public class StateChunckWrapper {
    private  long id;
    private  byte[] types;
    private  int[] keys;
    private  Object[] values;


    private static final byte LOAD_WAITING_ALLOC = 0;
    private static final byte LOAD_WAITING_TYPE = 1;
    private static final byte LOAD_WAITING_KEY = 2;
    private static final byte LOAD_WAITING_VALUE = 3;
    public StateChunckWrapper(long id, Buffer buffer) {
        this.id = id;
        loadFromBuffer(buffer);
    }

    public long getId() {
        return id;
    }

    public byte[] getTypes() {
        return types;
    }

    public int[] getKeys() {
        return keys;
    }

    public Object[] getValues() {
        return values;
    }

    private void loadFromBuffer(Buffer buffer) {
        if (buffer != null && buffer.length() > 0) {
            final long payloadSize = buffer.length();
            long previous = 0;
            long cursor = 0;
            byte state = LOAD_WAITING_ALLOC;
            byte read_type = -1;
            int read_key = -1;
            int position = 0;
            while (cursor < payloadSize) {
                byte current = buffer.read(cursor);
                if (current == Constants.CHUNK_SEP) {
                    switch (state) {
                        case LOAD_WAITING_ALLOC:
                            int size= Base64.decodeToIntWithBounds(buffer, previous, cursor);
                            types = new byte[size];
                            keys = new int[size];
                            values = new Object[size];
                            state = LOAD_WAITING_TYPE;
                            cursor++;
                            previous = cursor;
                            break;
                        case LOAD_WAITING_TYPE:
                            read_type = (byte) Base64.decodeToIntWithBounds(buffer, previous, cursor);
                            types[position] = read_type;
                            state = LOAD_WAITING_KEY;
                            cursor++;
                            previous = cursor;
                            break;case LOAD_WAITING_KEY:
                            read_key = Base64.decodeToIntWithBounds(buffer, previous, cursor);
                            keys[position]=read_key;
                            //primitive default loader
                            switch (read_type) {
                                //primitive types
                                case Type.BOOL:
                                case Type.INT:
                                case Type.DOUBLE:
                                case Type.LONG:
                                case Type.STRING:
                                    state = LOAD_WAITING_VALUE;
                                    cursor++;
                                    previous = cursor;
                                    break;
                                //arrays
                                case Type.DOUBLE_ARRAY:
                                    double[] doubleArrayLoaded = null;
                                    int doubleArrayIndex = 0;
                                    cursor++;
                                    previous = cursor;
                                    current = buffer.read(cursor);
                                    while (cursor < payloadSize && current != Constants.CHUNK_SEP) {
                                        if (current == Constants.CHUNK_VAL_SEP) {
                                            if (doubleArrayLoaded == null) {
                                                doubleArrayLoaded = new double[(int) Base64.decodeToLongWithBounds(buffer, previous, cursor)];
                                            } else {
                                                doubleArrayLoaded[doubleArrayIndex] = Base64.decodeToDoubleWithBounds(buffer, previous, cursor);
                                                doubleArrayIndex++;
                                            }
                                            previous = cursor + 1;
                                        }
                                        cursor++;
                                        if (cursor < payloadSize) {
                                            current = buffer.read(cursor);
                                        }
                                    }
                                    if (doubleArrayLoaded == null) {
                                        doubleArrayLoaded = new double[(int) Base64.decodeToLongWithBounds(buffer, previous, cursor)];
                                    } else {
                                        doubleArrayLoaded[doubleArrayIndex] = Base64.decodeToDoubleWithBounds(buffer, previous, cursor);
                                    }
                                    values[position]= doubleArrayLoaded;
                                    position++;
                                    state = LOAD_WAITING_TYPE;
                                    cursor++;
                                    previous = cursor;
                                    break;
                                case Type.LONG_ARRAY:
                                    long[] longArrayLoaded = null;
                                    int longArrayIndex = 0;
                                    cursor++;
                                    previous = cursor;
                                    current = buffer.read(cursor);
                                    while (cursor < payloadSize && current != Constants.CHUNK_SEP) {
                                        if (current == Constants.CHUNK_VAL_SEP) {
                                            if (longArrayLoaded == null) {
                                                longArrayLoaded = new long[(int) Base64.decodeToLongWithBounds(buffer, previous, cursor)];
                                            } else {
                                                longArrayLoaded[longArrayIndex] = Base64.decodeToLongWithBounds(buffer, previous, cursor);
                                                longArrayIndex++;
                                            }
                                            previous = cursor + 1;
                                        }
                                        cursor++;
                                        if (cursor < payloadSize) {
                                            current = buffer.read(cursor);
                                        }
                                    }
                                    if (longArrayLoaded == null) {
                                        longArrayLoaded = new long[(int) Base64.decodeToLongWithBounds(buffer, previous, cursor)];
                                    } else {
                                        longArrayLoaded[longArrayIndex] = Base64.decodeToLongWithBounds(buffer, previous, cursor);
                                    }
                                    values[position]= longArrayLoaded;
                                    position++;
                                    state = LOAD_WAITING_TYPE;
                                    cursor++;
                                    previous = cursor;
                                    break;
                                case Type.INT_ARRAY:
                                    int[] intArrayLoaded = null;
                                    int intArrayIndex = 0;
                                    cursor++;
                                    previous = cursor;
                                    current = buffer.read(cursor);
                                    while (cursor < payloadSize && current != Constants.CHUNK_SEP) {
                                        if (current == Constants.CHUNK_VAL_SEP) {
                                            if (intArrayLoaded == null) {
                                                intArrayLoaded = new int[(int) Base64.decodeToLongWithBounds(buffer, previous, cursor)];
                                            } else {
                                                intArrayLoaded[intArrayIndex] = Base64.decodeToIntWithBounds(buffer, previous, cursor);
                                                intArrayIndex++;
                                            }
                                            previous = cursor + 1;
                                        }
                                        cursor++;
                                        if (cursor < payloadSize) {
                                            current = buffer.read(cursor);
                                        }
                                    }
                                    if (intArrayLoaded == null) {
                                        intArrayLoaded = new int[(int) Base64.decodeToLongWithBounds(buffer, previous, cursor)];
                                    } else {
                                        intArrayLoaded[intArrayIndex] = Base64.decodeToIntWithBounds(buffer, previous, cursor);
                                    }
                                    values[position]= intArrayLoaded;
                                    position++;
                                    state = LOAD_WAITING_TYPE;
                                    cursor++;
                                    previous = cursor;
                                    break;
                                case Type.RELATION:
                                    cursor++;
                                    cursor = load_relation(buffer, cursor, payloadSize,position);
                                    position++;
                                    if (cursor < payloadSize) {
                                        current = buffer.read(cursor);
                                        if (current == Constants.CHUNK_SEP && cursor < payloadSize) {
                                            state = LOAD_WAITING_TYPE;
                                            cursor++;
                                            previous = cursor;
                                        }
                                    }
                                    break;
                                default:
                                    throw new RuntimeException("Not implemented yet!!!");
                            }
                            break;
                        case LOAD_WAITING_VALUE:
                            load_primitive(read_type, buffer, previous, cursor,position);
                            position ++;
                            state = LOAD_WAITING_TYPE;
                            cursor++;
                            previous = cursor;
                            break;
                    }
                } else {
                    cursor++;
                }
            }
            if (state == LOAD_WAITING_VALUE) {
                load_primitive(read_type, buffer, previous, cursor, position);
                position++;
            }
        }
    }

    private void load_primitive(final byte read_type, final Buffer buffer, final long previous, final long cursor, final int position) {
        switch (read_type) {
            case Type.BOOL:
                 values[position] =((byte) Base64.decodeToIntWithBounds(buffer, previous, cursor)) == CoreConstants.BOOL_TRUE;
                break;
            case Type.INT:
                values[position] = Base64.decodeToIntWithBounds(buffer, previous, cursor);
                break;
            case Type.DOUBLE:
                values[position] =Base64.decodeToDoubleWithBounds(buffer, previous, cursor);
                break;
            case Type.LONG:
                values[position] =Base64.decodeToLongWithBounds(buffer, previous, cursor);
                break;
            case Type.STRING:
                values[position] =Base64.decodeToStringWithBounds(buffer, previous, cursor);
                break;
        }
    }

    private long load_relation(final Buffer buffer, final long offset ,final long max, final int position){
        long cursor = offset;
        byte current = buffer.read(cursor);
        boolean isFirst = true;
        long previous = offset;
        long[] nodesId = null;
        int intheRelPos = 0;
        while (cursor < max && current != Constants.CHUNK_SEP && current != Constants.CHUNK_ENODE_SEP && current != Constants.CHUNK_ESEP) {
            if (current == Constants.CHUNK_VAL_SEP) {
                if (isFirst) {
                    nodesId = new long[Base64.decodeToIntWithBounds(buffer, previous, cursor)];
                    isFirst = false;
                } else {
                    nodesId[intheRelPos]= Base64.decodeToLongWithBounds(buffer, previous, cursor);
                    intheRelPos++;
                }
                previous = cursor + 1;
            }
            cursor++;
            if (cursor < max) {
                current = buffer.read(cursor);
            }
        }
        if (isFirst) {
            nodesId = new long[Base64.decodeToIntWithBounds(buffer, previous, cursor)];
        } else {
            nodesId[intheRelPos] = (Base64.decodeToLongWithBounds(buffer, previous, cursor));
        }
        values[position] = nodesId;
        return cursor;
    }

}
