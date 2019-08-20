package ac.iie.nnts.pgserver;

import io.hops.exception.StorageException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PGQueryHelper {
    public static final String COUNT_QUERY = "select count(*) from %s";
    public static final String COUNT_QUERY_UNIQUE =
            "select count(distinct %s) from %s";
    public static final String SELECT_EXISTS = "select exists(%s)";
    public static final String SELECT_EXISTS_QUERY = "select * from %s";
    public static final String MIN = "select min(%s) from %s";
    public static final String MAX = "select max(%s) from %s";

    private static PGServerConnector connector = PGServerConnector.getInstance();

    /**
     * Counts the number of rows in a given table.
     * <p/>
     * This creates and closes connection in every request.
     *
     * @param tableName
     * @return Total number of rows a given table.
     * @throws io.hops.exception.StorageException
     */
    public static int countAll(String tableName) throws StorageException {
        // TODO[H]: Is it good to create and close connections in every call?
        String query = String.format(COUNT_QUERY, tableName);
        return executeIntAggrQuery(query);
    }

    public static int countAllUnique(String tableName, String columnName)
            throws StorageException {
        String query = String.format(COUNT_QUERY_UNIQUE, columnName, tableName);
        return executeIntAggrQuery(query);
    }

    /**
     * Counts the number of rows in a table specified by the table name where
     * satisfies the given criterion. The criterion should be a valid SLQ
     * statement.
     *
     * @param tableName
     * @param criterion E.g. criterion="id > 100".
     * @return
     */
    public static int countWithCriterion(String tableName, String criterion)
            throws StorageException {
        StringBuilder queryBuilder =
                new StringBuilder(String.format(COUNT_QUERY, tableName)).
                        append(" where ").
                        append(criterion);
        return executeIntAggrQuery(queryBuilder.toString());
    }

    public static int countUniqueWithCriterion(String tableName, String columnNames, String criterion)
            throws StorageException {
        StringBuilder queryBuilder =
                new StringBuilder(String.format(COUNT_QUERY_UNIQUE, columnNames, tableName)).
                        append(" where ").
                        append(criterion);
        return executeIntAggrQuery(queryBuilder.toString());
    }

    public static boolean exists(String tableName, String criterion)
            throws StorageException {
        StringBuilder query =
                new StringBuilder(String.format(SELECT_EXISTS_QUERY, tableName));
        query.append(" where ").append(criterion);
        return executeBooleanQuery(String.format(SELECT_EXISTS, query.toString()));
    }

    public static int minInt(String tableName, String column, String criterion)
            throws StorageException {
        StringBuilder query =
                new StringBuilder(String.format(MIN, column, tableName));
        query.append(" where ").append(criterion);
        return executeIntAggrQuery(query.toString());
    }

    public static int maxInt(String tableName, String column, String criterion)
            throws StorageException {
        StringBuilder query =
                new StringBuilder(String.format(MAX, column, tableName));
        query.append(" where ").append(criterion);
        return executeIntAggrQuery(query.toString());
    }

    private static int executeIntAggrQuery(final String query)
            throws StorageException {
        return execute(query, new ResultSetHandler<Integer>() {
            @Override
            public Integer handle(ResultSet result) throws SQLException, StorageException {
                if (!result.next()) {
                    throw new StorageException(
                            String.format("result set is empty. Query: %s", query));
                }
                return result.getInt(1);
            }
        });
    }

    private static boolean executeBooleanQuery(final String query)
            throws StorageException {
        return execute(query, new ResultSetHandler<Boolean>() {
            @Override
            public Boolean handle(ResultSet result) throws SQLException, StorageException {
                if (!result.next()) {
                    throw new StorageException(
                            String.format("result set is empty. Query: %s", query));
                }
                return result.getBoolean(1);
            }
        });
    }

    public static int execute(String query) throws StorageException {
        Connection conn = null;
        PreparedStatement s = null;
        try {
            conn = connector.obtainSession();
            s = conn.prepareStatement(query);
            return s.executeUpdate();
        } catch (SQLException ex) {
            throw HopsSQLExceptionHelper.wrap(ex);
        } finally {
            connector.closeSession(conn, s, null);
        }

    }

    public static long maxLong(String tableName, String column) throws StorageException {
        StringBuilder query = new StringBuilder(String.format(MAX, column, tableName));
        return executeLongAggrQuery(query.toString());
    }

    private static long executeLongAggrQuery(final String query) throws StorageException {
        return execute(query, new ResultSetHandler<Long>() {
            @Override
            public Long handle(ResultSet result) throws SQLException, StorageException {
                if (!result.next()) {
                    throw new StorageException(
                            String.format("result set is empty. Query: %s", query)
                    );
                }
                return result.getLong(1);
            }
        });
    }


    public static ResultSet query(final String query) throws StorageException {
        return execute(query, new ResultSetHandler<ResultSet>() {
            @Override
            public ResultSet handle(ResultSet result) throws SQLException, StorageException {
                if (!result.next()) {
                    throw new StorageException(
                            String.format("result set is empty. Query: %s", query)
                    );
                }
                return result;
            }
        });
    }


    public interface ResultSetHandler<R> {
        R handle(ResultSet result) throws SQLException, StorageException;
    }

    public static <R> R execute(String query, ResultSetHandler<R> handler)
            throws StorageException {
        Connection conn = null;
        PreparedStatement s = null;
        ResultSet result = null;
        try {
            conn = connector.obtainSession();
            s = conn.prepareStatement(query);
            result = s.executeQuery();
            return handler.handle(result);
        } catch (SQLException ex) {
            throw HopsSQLExceptionHelper.wrap(ex);
        } finally {
            connector.closeSession(conn, s, result);
        }
    }
}

