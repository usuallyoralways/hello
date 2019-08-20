package ac.iie.nnts.pgserver;

import io.hops.exception.StorageException;
import io.hops.exception.TransientStorageException;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

public class HopsSQLExceptionHelper {
    public static StorageException wrap(SQLException e) {
        if (isTransient(e)) {
            return new TransientStorageException(e);
        } else {
            return new StorageException(e);
        }
    }

    private static boolean isTransient(SQLException e) {
        if (e instanceof SQLTransientException) {
            return true;
        } else if (e instanceof SQLRecoverableException) {
            return true;
        } else if (e instanceof SQLNonTransientException) {
            return false;
        }
        return false;
    }
}
