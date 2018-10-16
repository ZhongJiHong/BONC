package com.bonc.bdevutils.com.bonc.bdev.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * @Author G.Goe
 * @Date 2018/9/22
 * @Request JDK1.8及以上
 * @Resource https://bryanbende.com/development/2016/03/20/learning-kerberos-and-ugi
 * https://steveloughran.gitbooks.io/kerberos_and_hadoop/content/sections/ugi.html
 * https://www.slideshare.net/je2451/practical-kerberos-with-apache-hbase
 */
public class HbaseUtils {

    private HbaseUtils() {
    }

    private static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
    private static Configuration config = HBaseConfiguration.create();
    private static String krb5ConfFile = "D:\\admin\\krb5.conf";
    private static String hbaseConfFile = "D:\\admin\\hbase-site.xml";

    // 初始化环境代码需要处理
    static {
        System.setProperty("java.security.krb5.conf", krb5ConfFile);
        System.setProperty("sun.security.krb5.debug", "true");

        config.addResource(new Path(hbaseConfFile));
        config.set("hadoop.security.authentication", "kerberos");
    }

    // ------------------------------------------------Hbase操作------------------------------------------------------

    /**
     * 根据userPrincipal和keytabFile获取连接
     *
     * @param userPrincipal
     * @param keytabPath
     * @return
     * @throws IOException
     */
    public static Connection getConnection(String userPrincipal, String keytabPath) throws IOException {

        Connection connection = null;

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(userPrincipal, keytabPath);
        UserGroupInformation.setLoginUser(ugi);

        boolean securityEnabled = UserGroupInformation.isSecurityEnabled();
        // 判断ugi是否开启了安全认证
        if (securityEnabled) {
            System.err.println("配置Kerberos，开启安全认证，进入认证通道获取连接！");
            connection = ugi.doAs(new PrivilegedAction<Connection>() {
                @Override
                public Connection run() {
                    try {
                        return ConnectionFactory.createConnection(config);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), "Obtain securerity connection failed!");
                        return null;
                    }
                }
            });
        } else {
            System.err.println("不配置Kerberos，无法进入认证通道！不通过认证通过连接");
            connection = ConnectionFactory.createConnection(config);
        }
        return connection;
    }

    /**
     * 创建或覆盖表
     *
     * @param admin
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createOrOverwrite(Admin admin, String tableName, String... columnFamily) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            logger.info("Table {} has existed!", tableName);
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf :
                columnFamily) {
            table.addFamily(new HColumnDescriptor(cf));
        }

        admin.createTable(table);
        logger.info("Table {} created Succeed ", tableName);
    }


    /**
     * 删除表
     *
     * @param admin
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(Admin admin, String tableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
    }

    /**
     * 插入Put数据
     *
     * @param connection
     * @param tableName
     * @param put
     */
    public static void addPut(Connection connection, String tableName, Put put) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(put);
    }

    /**
     * 根据表名，rowkey删除
     *
     * @param connection
     * @param tableName
     * @param rowkey
     */
    public static void deletePut(Connection connection, String tableName, String rowkey) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);

    }

    /**
     * 根据表名，列簇，行键获取数据
     *
     * @param connection
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param column
     * @return
     */
    public static String getData(Connection connection, String tableName, String rowkey, String columnFamily, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

        Result result = table.get(get);

        // 判断结果是否为空
        if (result.isEmpty()) {
            logger.info("The Result is null");
            return null;
        }

        return new String(CellUtil.cloneValue(result.listCells().get(0)));

    }


    /**
     * 关闭连接，释放资源
     *
     * @param connection
     * @param admin
     */
    public static void close(Connection connection, Admin admin) {

        // 关闭 admin
        if (null != admin) {
            try {
                admin.close();
            } catch (IOException e) {
                logger.warn("conntion close Failed!");
            } finally {
                admin = null;
            }
        }

        // 关闭connection
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.warn("connection close Failed!");
            } finally {
                connection = null;
            }
        }
    }


    // -------------------------授权授权----------------------------------------------------------

    /**
     * 在当前认证过的用户下，在不超出当前用户权限的情况下对其他用户进行授权。覆盖
     *
     * @param connection
     * @param tableName
     * @param username
     * @param columnFamily
     * @param column
     * @param actions
     * @throws Throwable
     */
    public static void grantOnTable(Connection connection, String tableName, String username, String columnFamily, String column,
                                    Permission.Action... actions) throws Throwable {

        // 需要处理
        AccessControlClient.grant(connection, TableName.valueOf(tableName), username, Bytes.toBytes(columnFamily), Bytes.toBytes(column), actions);
    }

    /**
     * 在Table级别上，移除某人的权限
     *
     * @param connection
     * @param tableName
     * @param username
     * @param columnFamily
     * @param column
     * @param actions
     * @throws Throwable
     */
    public static void revokeOnTable(Connection connection, String tableName, String username, String columnFamily, String column, Permission.Action... actions) throws Throwable {

        // 获取匹配到的表的用户权限
        /*List<UserPermission> perms = client.getUserPermissions(connection, "user");
        for (UserPermission up :
                perms) {
            System.err.println(new String(up.getUser()) + ":" + Arrays.toString(up.getActions()));
        }*/
        AccessControlClient.revoke(connection, TableName.valueOf(tableName), username, Bytes.toBytes(columnFamily), Bytes.toBytes(column), actions);
    }
}
