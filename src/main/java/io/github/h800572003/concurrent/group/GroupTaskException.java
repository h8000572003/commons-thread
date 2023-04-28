package io.github.h800572003.concurrent.group;

public class GroupTaskException extends RuntimeException{

    public GroupTaskException() {
        super();
    }

    public GroupTaskException(String message) {
        super(message);
    }

    public GroupTaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public GroupTaskException(Throwable cause) {
        super(cause);
    }


}
