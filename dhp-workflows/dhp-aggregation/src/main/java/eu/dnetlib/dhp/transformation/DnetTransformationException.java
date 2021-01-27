package eu.dnetlib.dhp.transformation;

public class DnetTransformationException extends Exception {

    public DnetTransformationException() {
        super();
    }

    public DnetTransformationException(
            final String message,
            final Throwable cause,
            final boolean enableSuppression,
            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public DnetTransformationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DnetTransformationException(final String message) {
        super(message);
    }

    public DnetTransformationException(final Throwable cause) {
        super(cause);
    }
}
