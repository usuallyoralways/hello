package ac.iie.nnts.pgserver;

import com.alibaba.druid.pool.DruidDataSource;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Properties;

public class PGServerConnector implements StorageConnector<Connection> {

    static final Log LOG = LogFactory.getLog(PGServerConnector.class);
    private final static PGServerConnector instance = new PGServerConnector();
    private Properties conf;
    private static volatile DruidDataSource connectionPool;
    //private ThreadLocal<Connection> connection = new ThreadLocal<>();

    public static PGServerConnector getInstance(){
        return instance;
    }
    @Override
    public Connection obtainSession() throws StorageException {
        /*Connection conn = connection.get();
        if(conn == null){
            ComboPooledDataSource connectionPool = getConnectionPool();
            try {
                conn = connectionPool.getConnection();
                connection.set(conn);
            }catch (SQLException ex){
                throw HopsSQLExceptionHelper.wrap(ex);
            }
        }
        return conn;*/
        Connection conn = null;
        DruidDataSource druidDataSource = getConnectionPool();
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    private DruidDataSource getConnectionPool() {
        if(connectionPool == null){
            synchronized (this){
                if(connectionPool == null){
                    initializeConnectionPool();
                }
            }
        }
        return connectionPool;
    }

    @Override
    public void setConfiguration(Properties conf) throws StorageException {
        this.conf = conf;
    }

    private void initializeConnectionPool() {
        connectionPool = new DruidDataSource();
        Properties properties = new Properties();

        try{


            properties.load(PGServerConnector.class.getClassLoader().getResourceAsStream("jdbc.properties"));


            System.out.println(properties.toString());


            connectionPool.setUrl(properties.getProperty("url"));
            connectionPool.setDriverClassName(properties.getProperty("driverClassName"));
            connectionPool.setUsername(properties.getProperty("username"));
            connectionPool.setPassword(properties.getProperty("password"));
            connectionPool.setInitialSize(Integer.parseInt(properties.getProperty("initialSize")));
            connectionPool.setMinIdle(Integer.parseInt(properties.getProperty("minIdle")));
            connectionPool.setMaxWait(Integer.parseInt(properties.getProperty("maxWait")));
            connectionPool.setMaxActive(Integer.parseInt(properties.getProperty("maxActive")));
            connectionPool.setTimeBetweenEvictionRunsMillis(Long.parseLong(properties.getProperty("timeBetweenEvictionRunsMillis")));
            connectionPool.setMinEvictableIdleTimeMillis(Long.parseLong(properties.getProperty("minEvictableIdleTimeMillis")));
            connectionPool.setTestWhileIdle(Boolean.parseBoolean(properties.getProperty("testWhileIdle")));
            connectionPool.setValidationQuery(properties.getProperty("validationQuery"));
            connectionPool.setTestOnBorrow(Boolean.parseBoolean(properties.getProperty("testOnBorrow")));
            connectionPool.setTestOnReturn(Boolean.parseBoolean(properties.getProperty("testOnReturn")));
            //connectionPool.setMaxOpenPreparedStatements(Integer.parseInt(properties.getProperty("maxOpenPreparedStatements")));
            connectionPool.setAsyncInit(Boolean.parseBoolean(properties.getProperty("asyncInit")));
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public void closeSession(Connection connection, Statement statement, ResultSet resultSet){
        /*Connection conn = connection.get();
        if(conn != null){
            try {
                conn.close();
                connection.remove();
            }catch (SQLException ex){
                throw HopsSQLExceptionHelper.wrap(ex);
            }
        }*/
        if(connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(statement != null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(resultSet != null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void truncateTable(boolean transactional,String tableName)
        throws StorageException,SQLException{
        truncateTable(true,tableName,-1);
    }

    public static void truncateTable(String tableName,int limit)
        throws StorageException,SQLException{
        truncateTable(true,tableName,-1);
    }

    public static void truncateTable(boolean transactional, String tableName, int limit)
    throws StorageException,SQLException{
        PGServerConnector connector = PGServerConnector.getInstance();
        Connection connection = null;
        try{
            connection = connector.obtainSession();
            if(transactional){
                if(limit > 0){
                   /* PreparedStatement s = connn.prepareStatement("delete from " +
                        tableName + " limit " + limit);*/
                    PreparedStatement s = connection.prepareStatement("TRUNCATE TABLE " + tableName + " CASCADE");
                    s.executeUpdate();
                }else {
                    int nbrows = 0;
                    do{
                        /*PreparedStatement s = connn.prepareStatement(
                                "delete from "+ tableName + " limit 1000");*/
                        PreparedStatement s = connection.prepareStatement("TRUNCATE TABLE " + tableName + " CASCADE");
                        nbrows = s.executeUpdate();
                    } while (nbrows > 0);
                }
            } else {
                PreparedStatement s = connection.prepareStatement("TRUNCATE TABLE " + tableName + " CASCADE");
                s.executeUpdate();
            }
        }finally {
            connector.closeSession(connection,null,null);
        }
    }

    @Override
    public void beginTransaction() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void commit() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean formatAllStorageNonTransactional() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean formatYarnStorageNonTransactional() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean formatHDFSStorageNonTransactional() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean formatStorage() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean formatYarnStorage() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean formatHDFSStorage() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isTransactionActive() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void stopStorage() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readLock() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void writeLock() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readCommitted() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setPartitionKey(Class aClass, Object o) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void dropAndRecreateDB() throws StorageException {
        PGServerConnector connector = PGServerConnector.getInstance();
        Connection conn = null;
        Statement stmt = null;
        try{
            String sql = "";
            String database = conf.getProperty("databaseName");
            try{
                conn = connector.obtainSession();
                stmt = conn.createStatement();

                sql = "DROP DATABASE IF EXISTS " + database;
                LOG.warn("Dropping database "+ database);
                stmt.executeUpdate(sql);
                LOG.warn("Database dropped");
            }catch (Exception e){
                LOG.warn(e);
            }

            try{
                sql = "CREATE DATABASE " + database;
                LOG.warn("Creating database "+database);
                stmt.executeUpdate(sql);
                LOG.warn("Database created");
            }catch (Exception e){
                LOG.warn(e);
            }

            try{
                sql = "use " + database;
                LOG.warn("Selecting database " + database);
                stmt.executeUpdate(sql);
                LOG.warn("Database selected");
            }catch (Exception e){
                LOG.warn(e);
            }

            try{
                ScriptRunner runner = new ScriptRunner(conn, false, false);
                LOG.warn("Importing Database");
                runner.runScript(new BufferedReader(new InputStreamReader(getSchema())));
                LOG.warn("Schema imported");
            }catch (Exception e){
                LOG.warn(e);
            }
        } finally {
            connector.closeSession(conn,stmt,null);
        }
    }

    public InputStream getSchema() throws IOException{
        String configFile = "schema.sql";
        InputStream inputStream = StorageConnector.class.getClassLoader().getResourceAsStream(configFile);
        return inputStream;
    }

    @Override
    public void flush() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getClusterConnectString() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getDatabaseName() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void returnSession(boolean b) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean formatStorage(Class[] classes) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
