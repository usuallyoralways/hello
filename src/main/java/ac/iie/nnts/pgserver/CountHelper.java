package ac.iie.nnts.pgserver;

import io.hops.exception.StorageException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CountHelper {
    public static final String COUNT_QUERY = "select count(*) from %s";
    public static final String COUNT_QUERY_UNIQUE =
            "select count(distinct %s) from %s";
    public static final String COUNT_WHERE = "select count(*) from %s where %s";

    private static PGServerConnector connector =
            PGServerConnector.getInstance();

    public static int countWhere(String tableName, String condition)
            throws StorageException {
        String query = String.format(COUNT_WHERE, tableName, condition);
        return count(query);
    }

    /**
     * Counts the number of rows in a given table.
     * <p/>
     * This creates and closes connection in every request.
     *
     * @param tableName
     * @return Total number of rows a given table.
     * @throws StorageException
     */
    public static int countAll(String tableName) throws StorageException {
        // TODO[H]: Is it good to create and close connections in every call?
        String query = String.format(COUNT_QUERY, tableName);
        return count(query);
    }

    public static int countAllUnique(String tableName, String columnName)
            throws StorageException {
        String query = String.format(COUNT_QUERY_UNIQUE, columnName, tableName);
        return count(query);
    }

    private static int count(String query) throws StorageException {
        Connection conn = null;
        PreparedStatement s = null;
        ResultSet result = null;
        try {
            conn = connector.obtainSession();
            s = conn.prepareStatement(query);
            result = s.executeQuery();
            if (result.next()) {
                return result.getInt(1);
            } else {
                throw new StorageException(
                        String.format("Count result set is empty. Query: %s", query));
            }
        } catch (SQLException ex) {
            throw new StorageException(ex);
        } finally {
            connector.closeSession(conn,s,result);
        }
    }

    /**
     * Counts the number of rows in a table specified by the table name where
     * satisfies the given criterion. The criterion should be a valid SLQ
     * statement.
     *
     * @param tableName
     * @param criterion
     *     E.g. criterion="id > 100".
     * @return
     */
    public static int countWithCriterion(String tableName, String criterion)
            throws StorageException {
        StringBuilder queryBuilder =
                new StringBuilder(String.format(COUNT_QUERY, tableName)).
                        append(" where ").
                        append(criterion);
        return count(queryBuilder.toString());
    }
}
