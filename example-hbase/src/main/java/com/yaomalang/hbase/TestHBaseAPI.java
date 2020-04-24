package com.yaomalang.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class TestHBaseAPI {

    private static Configuration config = null;
    private static Connection connection = null;
    private static Admin admin = null;

    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "node001");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        createTable("student", "info", "score");
//        putData();
//        dropTable("student");
//        scanTable("student", null, null);
//        getData("student", "1587709834080", "info", "");
        getData("student", "1587709834080", "info", "name");
        deleteData("student", "1587709834080", "score", "java", 0);

        getData("student", "1587709834080", "score", "java");
        close(connection, admin);
    }

    /**
     *
     * @param tableName
     * @param cols
     * @throws IOException
     */
    public static void createTable(String tableName, String... cols) throws IOException {
        if (tableExists(tableName)) {
            System.out.println("tableName = " + tableName + " is exists.");
            return;
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String col : cols) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(col);
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor);

        System.out.println("Table : " + tableName + "is created:" + tableExists(tableName));
    }

    /**
     * @param tables
     * @throws IOException
     */
    public static void dropTable(String... tables) throws IOException {
        for (String table : tables) {
            if (tableExists(table)) {
                TableName tableName = TableName.valueOf(table);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("droop table [" + table + "] is successful.");
            } else {
                System.out.println("table [" + table + "] not exists.");
            }
        }
    }

    public static void putData() throws IOException {
        String rowKey = String.valueOf(System.currentTimeMillis());
        putData("student", rowKey, "info", "name", "yaomalang");
        putData("student", rowKey, "info", "age", "23");
        putData("student", rowKey, "info", "sex", "male");

        putData("student", rowKey, "score", "java", "50");
        putData("student", rowKey, "score", "sql", "75");
        putData("student", rowKey, "score", "python", "80");
    }

    /**
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param col
     * @param value
     * @throws IOException
     */
    public static void putData(String tableName, String rowKey, String cf, String col, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(cf.getBytes(), col.getBytes(), value.getBytes());

        table.put(put);
    }

    /**
     * @param tableName
     * @param startRowKey
     * @param stopRowKey
     * @throws IOException
     */
    public static void scanTable(String tableName, String startRowKey, String stopRowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (!"".equals(startRowKey) && startRowKey != null)
            scan.withStartRow(startRowKey.getBytes());
        if (!"".equals(stopRowKey) && stopRowKey != null)
            scan.withStopRow(stopRowKey.getBytes());

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            showResult(result);
        }
    }

    /**
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column)) {
            get.addColumn(family.getBytes(), column.getBytes());
        } else if (StringUtils.isNotEmpty(family)) {
            get.addFamily(family.getBytes());
        }
        Result result = table.get(get);
        showResult(result);
    }

    /**
     *
     * @param result
     * @throws UnsupportedEncodingException
     */
    private static void showResult(Result result) throws UnsupportedEncodingException {
        List<Cell> cells = result.listCells();
        if (cells == null) return;
        for (Cell cell : cells) {
            String line = "RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + "\tFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "\tColumn:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "\tValue:" + Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(line);
        }
    }

    /**
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param timestamp
     */
    public static void deleteData(String tableName, String rowKey, String family, String column, long timestamp) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(rowKey.getBytes());
            if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column) && timestamp > 0) {
                delete.addColumns(family.getBytes(), column.getBytes(), timestamp);
            } else if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column)) {
                delete.addColumns(family.getBytes(), column.getBytes());
            }
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean tableExists(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void close(Connection connection, Admin admin) {
        try {
            if (connection != null) connection.close();
            if (admin != null) admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
