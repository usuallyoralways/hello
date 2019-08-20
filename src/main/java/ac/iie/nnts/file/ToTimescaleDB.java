package ac.iie.nnts.file;


import java.io.*;
import java.net.URLDecoder;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;

import ac.iie.nnts.Stream.Data;
import ac.iie.nnts.pgserver.*;
import io.hops.exception.StorageException;

public class ToTimescaleDB {
    //通过文件格式创建数据库
    public static String createTableSQL(String input, String tableName) {
        String SQL = "create table if not exists " + tableName + "(time TIMESTAMPTZ NOT NULL, arrivalTime int, key int NOT NULL ";

        String[] atts = input.split(",");
//        double[] d = new double[atts.length - 1];//第一位是key，后面是属性值

        //最大最小值更新和规范化计算Z-Score
        for (int i = 1; i <= atts.length - 1; i++) {
            SQL += ", Attribution" + String.valueOf(i) + " FLOAT ";
        }
        SQL += ")";
        return SQL;
    }


    public static String insertSQL(String input, String tableName, int time, String timestamp) {

        String SQL = "insert into " + tableName + " values(" +"make_timestamptz("+timestamp+",\'Asia/Shanghai\'),"+String.valueOf(time);
//        String SQL = "insert into " + tableName + " values(" +timestamp+","+String.valueOf(time);
        String[] atts = input.split(",");


        //最大最小值更新和规范化计算Z-Score
        for (int i = 0; i < atts.length; i++) {
            SQL += ", " + atts[i];
        }
        SQL += ");";
        return SQL;

    }

    public static void importData(String filename) throws StorageException, SQLException {
        PGServerConnector connserver = new PGServerConnector();
        Connection conn = connserver.obtainSession();

//        ResultSet set=PGQueryHelper.query("select * from my");
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy,MM,dd,HH,mm,ss.mm");
        String timetime = sdf.format(d);

        try {
            BufferedReader bfr = new BufferedReader(new FileReader(new File(filename)));
            String line = "";
            int time = 1;//毫秒
            ArrayList<double[]> minMax = new ArrayList<>();
            try {


                int attLength = 0;
                String tableName = "null";
                //第一行建立表格

                if ((line = bfr.readLine()) != null) {
                    File tempFile = new File(filename.trim());
                    tableName = tempFile.getName();
                    String tempTableName[] = tableName.split("\\.");

                    tableName = tempTableName[0];
                    String createSQL = createTableSQL(line, tableName);
                    System.out.println("createsql:\t" + createSQL);
                    PGQueryHelper.execute(createSQL);
                    String createHyperTableSQL ="SELECT create_hypertable("+"\'"+tableName+"\',\'time\');";
                    System.out.println("hypertableSQL: "+createHyperTableSQL);
                    try{
                        PGQueryHelper.execute(createHyperTableSQL);
                    }catch (Exception e){
                        ;
                    }
                }
                String[] atts = line.split(",");
                String insertSQL = insertSQL(line, tableName, time,timetime);
                timetime = sdf.format(d.getTime()+1000*time);
                System.out.println(insertSQL);
                PGQueryHelper.execute(insertSQL);

                time++;

                Statement st = conn.createStatement();
                while ((line = bfr.readLine()) != null) {
                    //读第一行时建立表格存入数据

                    atts = line.split(",");

                    insertSQL = insertSQL(line, tableName, time, timetime);
                    timetime = sdf.format(d.getTime()+1000*time);
                    st.addBatch(insertSQL);
                    if (time % 1000 == 0) {
                        st.executeBatch();
                        st.clearBatch();
                        System.out.println(time);
                    }

//                    PGQueryHelper.execute(insertSQL);
                    time++;
                }
                st.executeBatch();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        conn.close();
    }


    public static LinkedList<Data> getData(String tableName) throws StorageException, SQLException {
        String SQL = "select * from " + tableName +";";

        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<LinkedList<Data>>() {
            @Override
            public LinkedList<Data> handle(ResultSet result) throws SQLException, StorageException {
                LinkedList<Data> streams = new LinkedList<>();
                ResultSet resultSet = result;

                ResultSetMetaData rsmd = (ResultSetMetaData) resultSet.getMetaData();
                int n = rsmd.getColumnCount();
                while (resultSet.next()) {
                    Timestamp timestamp = resultSet.getTimestamp("time");
                    int key = resultSet.getInt("key");
                    int time = resultSet.getInt("arrivaltime");
                    double[] d = new double[n - 3];
                    for (int i = 0; i < n - 3; i++) {
                        d[i] = resultSet.getDouble(i + 3);
                    }
                    Data data = new Data(key, d, time,timestamp);

                    System.out.println(data);

                    streams.add(data);
                }
                resultSet.close();
                return streams;
            }
        });

    }

    public static void file2TimescaleDB() throws UnsupportedEncodingException, FileNotFoundException, StorageException, SQLException {



        URLDecoder decoder = new URLDecoder();
        String filename = "D:\\Documents\\vscode\\data\\shuttle.csv";
        String filepath = decoder.decode(filename,"utf-8");
//
//        importData("D:/Documents/李军研究生/CODE/pljava-code/src/main/resources/tao.csv");
        importData(filepath);

        filename = "D:\\Documents\\vscode\\data\\tao.csv";
        filepath = decoder.decode(filename,"utf-8");
        importData(filepath);

        filename = "D:\\Documents\\vscode\\data\\kddcup.csv";
        filepath = decoder.decode(filename,"utf-8");
        importData(filepath);
    }


    public static void main(String[] args) throws SQLException, StorageException, FileNotFoundException, UnsupportedEncodingException {





        file2TimescaleDB();
        System.out.println(PGQueryHelper.maxLong("shuttle","key"));
        LinkedList<Data> streams = getData("shuttle");

        System.out.println(streams.size());

    }
}

