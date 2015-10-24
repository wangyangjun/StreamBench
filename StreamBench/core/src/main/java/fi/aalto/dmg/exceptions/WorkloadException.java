package fi.aalto.dmg.exceptions;

/**
 * Created by yangjun.wang on 16/10/15.
 */
public class WorkloadException extends Exception {

    private static final long serialVersionUID = 8844396756042772132L;

    public WorkloadException(String message)
    {
        super(message);
    }

    public WorkloadException()
    {
        super();
    }

    public WorkloadException(String message, Throwable cause)
    {
        super(message,cause);
    }

    public WorkloadException(Throwable cause)
    {
        super(cause);
    }

}
