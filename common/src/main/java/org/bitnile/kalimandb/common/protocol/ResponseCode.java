
package org.bitnile.kalimandb.common.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Same as rheakv.
 * <p>
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Note that client library will convert an unknown error code to the non-retriable UnknownServerException if the client library
 * version is old and does not recognize the newly-added error code. Therefore when a new server-side error is added,
 * we may need extra logic to convert the new error code to another existing error code before sending the response back to
 * the client if the request version suggests that the client may not recognize the new error code.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public class ResponseCode {
    public static final Logger logger = LoggerFactory.getLogger(ResponseCode.class);

    public static final int UNKNOWN_SERVER_ERROR = -1;

    public static final int NONE = 0;

    public static final int STORAGE_ERROR = 1;

    public static final int INVALID_REQUEST = 2;

    public static final int LEADER_NOT_AVAILABLE = 3;

    public static final int NOT_LEADER = 4;

    public static final int INVALID_PARAMETER = 5;

    public static final int NO_REGION_FOUND = 6;

    public static final int INVALID_REGION_MEMBERSHIP = 7;

    public static final int INVALID_REGION_VERSION = 8;

    public static final int INVALID_REGION_EPOCH = 9;

    public static final int INVALID_STORE_STATS = 10;

    public static final int INVALID_REGION_STATS = 11;

    public static final int  STORE_HEARTBEAT_OUT_OF_DATE = 12;

    public static final int  REGION_HEARTBEAT_OUT_OF_DATE = 13;

    public static final int  CALL_SELF_ENDPOINT_ERROR = 14;

    public static final int  SERVER_BUSY = 15;

    public static final int  REGION_ENGINE_FAIL = 16;

    public static final int  CONFLICT_REGION_ID = 17;

    public static final int  TOO_SMALL_TO_SPLIT = 18;

    public static final int  RAFT_TIMEOUT = 19;

    public static final int  NO_METHOD_SUPPORT = 20;

    public static final int  ARGS_NULL = 21;

    public static final int  SUCCESS = 200;

    public static final int  SYSTEM_ERROR = 500;

    public static final int  REQUEST_CODE_NOT_SUPPORTED = 501;


}
