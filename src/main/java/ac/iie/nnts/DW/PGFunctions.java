package ac.iie.nnts.DW;

import ac.iie.nnts.pgserver.PGQueryHelper;
import io.hops.exception.StorageException;
import org.jcp.xml.dsig.internal.dom.DOMUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

public class PGFunctions {

    //??new LinkedList<Element>
//    private long timestamp; //meaning
//    private int value; //meaning;
//    private int key;//????key
//    private long total;
    public static void newElement(String tableName) throws StorageException {
        String SQL = "create table if not exists " + tableName + " ( timestamp bigint, value int, key int,total bigint);";
        PGQueryHelper.execute(SQL);
    }

    public static void insertElement(String tableName, Element element) throws StorageException {

        String SQL = "insert into " + tableName + " values(" + String.valueOf(element.getTimestamp()) +
                "," + element.getValue() + ", " + element.getKey() + ", " + element.getTotal() + ");";
        System.out.println(SQL);
        PGQueryHelper.execute(SQL);

    }


    //??new LinkedList<Element>[]
//    private long timestamp; //meaning
//    private int value; //meaning;
//    private int key;//????key
//    private long total;
    public static void newElements(String tableName, int size) throws StorageException {
        for (int i = 0; i < size; i++) {
            String SQL = "create table if not exists " + tableName + String.valueOf(i) + " ( timestamp bigint, value int, key int,total bigint);";
            PGQueryHelper.execute(SQL);
        }
    }


    //??new LinkedList<OverElement>
//    private long timestamp; //meaning
//    private int key;//????key
    public static void newOverElement(String tableName) throws StorageException {
        String SQL = "create table if not exists " + tableName + " ( timestamp bigint, key int);";
        PGQueryHelper.execute(SQL);
    }

    public static void insertOverElement(String tableName, OverElement overElement) throws StorageException {
        String SQL = "insert into " + tableName + " values(" + String.valueOf(overElement.getTimestamp()) +
                ","  + overElement.getKey() + ");";
        System.out.println(SQL);
        PGQueryHelper.execute(SQL);

    }


    //??new LinkedList<OverElement>[]
//    private long timestamp; //meaning
//    private int key;//????key
    public static void newOverElements(String tableName, int size) throws StorageException {
        for (int i = 0; i < size; i++) {
            String SQL = "create table if not exists " + tableName + String.valueOf(i) + " ( timestamp bigint, key int);";
            PGQueryHelper.execute(SQL);
        }
    }


    //?? LinkedList ?  getFirst()
    public static long getFisrt(String tableName) throws StorageException {
        String SQL = "select min(timestamp) from " + tableName;
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Long>() {
            @Override
            public Long handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getLong(1);
                } else return (long) -1;
            }
        });
    }

    //?? LinkedList ?  getFirst()
    public static OverElement getFisrtOverElement(String tableName) throws StorageException {
        String SQL = "select * from " + tableName;
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<OverElement>() {
            @Override
            public OverElement handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    long timestamp = result.getLong("timestamp");
                    int key = result.getInt("key");//????key
                    return new OverElement(timestamp, key);
                }
                return null;
            }
        });
    }

    public static Element getFisrtElement(String tableName) throws StorageException {
        String SQL = "select * from " + tableName;
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Element>() {
            @Override
            public Element handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    long timestamp = result.getLong("timestamp");
                    int key = result.getInt("key");//????key
                    int value = result.getInt("value");
                    long total = result.getLong("total");
                    return new Element(timestamp, key, value, total);
                }
                return null;
            }
        });
    }


    public static Void remove(String tableName, long timestamp) throws StorageException {
        String SQL = "delete  from " + tableName + " where timestamp = " + timestamp + ";";
        System.out.println(SQL);
        try {
            return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Void>() {
                @Override
                public Void handle(ResultSet result) throws SQLException, StorageException {
                    return null;
                }
            });
        } catch (StorageException e) {
            ;
        }
        return null;
    }

    public static int createParameter(String tableName, int parakey)  {
        String SQL = "create table if not exists " + tableName + " (";
        long MaxTSInSampleWindow = 0; // the biggest TS in the window
        long MinTSInSampleWindow = 0; // the smaller TS in the window
        long totalValue = 0L;
        int expire = 50; //??????
        double currentRelativeError;
        SQL = "CREATE TABLE if not exists " + tableName +
                "(MaxLevelNum int, MaxTSInSampleWindow bigint, MinTSInSampleWindow bigint," +
                " totalValue bigint, expire int, currentRelativeError float,id int primary key,timestamp bigint, value int, key int,total bigint)";
//        System.out.println(SQL);
        try {
            PGQueryHelper.execute(SQL);
            SQL = "insert into " + tableName + " values(" +
                    "41,0,0,0,50,0," + String.valueOf(parakey) + ",0,0,0,0);";
            PGQueryHelper.execute(SQL);
        } catch (StorageException e){
            System.out.println(e.toString());
        }

        return 0;
    }

    public static int getMaxLevelNum(String tableName, int parakey) throws StorageException {
        String SQL = "select MaxLevelNum from " + tableName + " where id = " + String.valueOf(parakey) + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Integer>() {
            @Override
            public Integer handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getInt(1);
                } else return -1;
            }
        });
    }

    public static int setMaxLevelNum(String tableName, int parakey, int theVale) throws StorageException {
        String SQL = "update " + tableName + " SET MaxLevelNum=" + String.valueOf(theVale) + " where id = " + String.valueOf(parakey) + ";";
        PGQueryHelper.execute(SQL);
        return 1;
    }

    public static double getCurrentRelativeError(String tableName, int parakey) throws StorageException {
        String SQL = "select currentRelativeError from " + tableName + " where id = " + String.valueOf(parakey) + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Double>() {
            @Override
            public Double handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getDouble(1);
                } else return -1.0;
            }
        });
    }

    public static int setCurrentRelativeError(String tableName, int parakey, double theVale) throws StorageException {
        String SQL = "update " + tableName + " SET currentRelativeError=" + String.valueOf(theVale) + " where id = " + String.valueOf(parakey) + ";";
        PGQueryHelper.execute(SQL);
        return 1;
    }

    public static long getMaxTSInSampleWindow(String tableName, int parakey) throws StorageException {
        String SQL = "select MaxTSInSampleWindow from " + tableName + " where id = " + String.valueOf(parakey) + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Long>() {
            @Override
            public Long handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getLong(1);
                } else return (long) -1;
            }
        });
    }

    public static int setMaxTSInSampleWindow(String tableName, int parakey, long theVale) throws StorageException {
        String SQL = "update " + tableName + " SET MaxTSInSampleWindow=" + String.valueOf(theVale) + " where id = " + String.valueOf(parakey) + ";";
        PGQueryHelper.execute(SQL);
        return 1;
    }


    public static long getMinTSInSampleWindow(String tableName, int parakey) throws StorageException {
        String SQL = "select MinTSInSampleWindow from " + tableName + " where id = " + String.valueOf(parakey) + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Long>() {
            @Override
            public Long handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getLong(1);
                } else return (long) -1;
            }
        });
    }

    public static int setMinTSInSampleWindow(String tableName, int parakey, long theVale) throws StorageException {
        String SQL = "update " + tableName + " SET MinTSInSampleWindow=" + String.valueOf(theVale) + " where id = " + String.valueOf(parakey) + ";";
        PGQueryHelper.execute(SQL);
        return 1;
    }

    public static long getTotalValue(String tableName, int parakey) throws StorageException {
        String SQL = "select TotalValue from " + tableName + " where id = " + String.valueOf(parakey) + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Long>() {
            @Override
            public Long handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getLong(1);
                } else return (long) -1;
            }
        });
    }

    public static int setTotalValue(String tableName, int parakey, long theVale) throws StorageException {
        String SQL = "update " + tableName + " SET totalValue=" + String.valueOf(theVale) + " where id = " + String.valueOf(parakey) + ";";
        PGQueryHelper.execute(SQL);
        return 1;
    }

    public static int getExpire(String tableName, int parakey) throws StorageException {
        String SQL = "select Expire from " + tableName + " where id = " + String.valueOf(parakey) + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Integer>() {
            @Override
            public Integer handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getInt(1);
                } else return -1;
            }
        });
    }

    public static int setExpire(String tableName, int parakey, long theVale) throws StorageException {
        String SQL = "update " + tableName + " SET Expire=" + String.valueOf(theVale) + " where id = " + String.valueOf(parakey) + ";";
        PGQueryHelper.execute(SQL);
        return 1;
    }

    //??????????
    public static Element getLeft(String tableName, int parakey) throws StorageException {
        String SQL = "select timestamp, value,key,total from " + tableName + " where id = " + String.valueOf(parakey) + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Element>() {
            @Override
            public Element handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    long timestamp = result.getLong("timestamp");
                    int value = result.getInt("value");
                    int key = result.getInt("key");
                    long total = result.getLong("total");
                    return new Element(timestamp, key, value, total);
                }
                return null;
            }
        });
    }

    public static int setLeft(String tableName, int parakey, Element element) throws StorageException {
        String SQL = "update " + tableName + " SET timestamp=" + String.valueOf(element.getTimestamp()) +
                ", value=" + element.getValue() + ", key= " + element.getKey() + ", total= " + element.getTotal() + " where id = " + String.valueOf(parakey) + ";";
        System.out.println(SQL);
        PGQueryHelper.execute(SQL);
        return 1;
    }


    //?? ?????
    public static boolean isEmpty(String tableName) {
        String SQL = "select * from " + tableName + ";";
        try {
            return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Boolean>() {
                @Override
                public Boolean handle(ResultSet result) throws SQLException, StorageException {
                    if (!result.next()) {
                        return true;
                    }
                    return false;
                }
            });
        } catch (StorageException e) {
            return true;
        }
    }

    //??????
    public static int getSize(String tableName) throws StorageException {
        String SQL = "select count(*) from " + tableName + ";";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<Integer>() {
            @Override
            public Integer handle(ResultSet result) throws SQLException, StorageException {
                if (result.next()) {
                    return result.getInt(1);
                } else return 0;
            }
        });
    }

    public static LinkedList<OverElement> getOverElements(String tableName) throws StorageException {
        String SQL = "select * from " + tableName + " order by timestamp asc;";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<LinkedList<OverElement>>() {
            @Override
            public LinkedList<OverElement> handle(ResultSet result) throws SQLException, StorageException {
                LinkedList<OverElement> overElements = new LinkedList<>();
                while (result.next()) {
                    long timestamp = result.getLong("timestamp");
                    int key = result.getInt("key");
                    overElements.addLast(new OverElement(timestamp, key));
                }
                return overElements;
            }
        });
    }

    public static LinkedList<Element> getElements(String tableName) throws StorageException {
        String SQL = "select * from " + tableName + " order by timestamp asc;";
        return PGQueryHelper.execute(SQL, new PGQueryHelper.ResultSetHandler<LinkedList<Element>>() {
            @Override
            public LinkedList<Element> handle(ResultSet result) throws SQLException, StorageException {
                LinkedList<Element> elements = new LinkedList<>();
                while (result.next()) {
                    long timestamp = result.getLong("timestamp");
                    int value = result.getInt("value");
                    int key = result.getInt("key");
                    long total = result.getLong("total");
                    elements.addLast(new Element(timestamp, key, value, total));
                }
                return elements;
            }
        });
    }


    public static void main(String[] args) throws StorageException {
//        System.out.println(isEmpty("shsh"));
//        newElement("shi");
//        newElements("shishi",5);
//
//        newOverElement("shis");
//        newOverElements("shishis",5);
//        createParameter("para",123);
//
//        System.out.println(getFisrt("shi"));
//        System.out.println(remove("shi",getFisrt("shi")));

        System.out.println(getMaxTSInSampleWindow("para", 123));
        System.out.println(getLeft("para", 123));
        System.out.println(getMinTSInSampleWindow("para", 123));
        System.out.println(getExpire("para", 123));
        System.out.println(getTotalValue("para", 123));

        setLeft("para", 123, new Element(123, 234, 34, 123));

        System.out.println(getSize("para") + " para size");


    }

}
