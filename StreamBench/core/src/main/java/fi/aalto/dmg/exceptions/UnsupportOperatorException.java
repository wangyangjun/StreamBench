package fi.aalto.dmg.exceptions;

import java.io.Serializable;

/**
 * Created by jun on 27/02/16.
 */
public class UnsupportOperatorException extends Exception implements Serializable {

    private static final long serialVersionUID = 8844396756042772132L;

    public UnsupportOperatorException(String message) {
        super(message);
    }

    public UnsupportOperatorException() {
        super();
    }

    public UnsupportOperatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportOperatorException(Throwable cause) {
        super(cause);
    }

}
