package com.bonc.bdevutils.com.bonc.bdev.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @Author G.Goe
 * @Date 2018/10/16
 * @Request
 * @Resource
 */
public class HBaseOpTest {

    private Connection connection;

    @Before
    public void setUp() throws Exception {
        // connection = HbaseUtils.getConnection("hadoop/bonc001@BONC.COM", "D:\\admin\\hbase.keytab");

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("D:\\admin\\nonauth\\hbase-site.xml");
        configuration.set("hbase.zookeeper.quorum", "hadoop001,hadoop002,hadoop003");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        HbaseConnectionFactory connectionFactory = new HbaseConnectionFactory(configuration);
        connection = connectionFactory.getNonAuthenticationConnection();

        /*System.setProperty("java.security.krb5.conf", "D:\\admin\\krb5.conf");
        // System.setProperty("sun.security.krb5.debug", "true");
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("D:\\admin\\hbase-site.xml"));
        configuration.set("hadoop.security.authentication", "kerberos");
        HbaseConnectionFactory connectionFactory = new HbaseConnectionFactory(configuration);
        connection = connectionFactory.getAuthenticationConnection("hadoop/bonc001@BONC.COM",
                "D:\\admin\\hbase.keytab");*/
    }

    @Test
    @Ignore
    public void createTable() {
        Boolean result = HBaseOp.createTable(connection, "dev_user", "cf1,cf2".split(","));
        Assert.assertTrue(result);
    }

    @Test
    @Ignore
    public void putRow() {
        Put put01 = new Put(Bytes.toBytes("rowkey111"));
        put01.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("bai01"));
        put01.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(22));
        put01.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("gender"), Bytes.toBytes("女"));
        put01.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("image"), Bytes.toBytes("照片"));
        put01.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("address"), Bytes.toBytes("东园"));
        put01.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("signature"), Bytes.toBytes("年轻"));
        boolean result = HBaseOp.putRow(connection, "dev_user", put01);
        Assert.assertTrue(result);
    }

    @Test
    @Ignore
    public void existsRow() throws IOException {
        // boolean result = HBaseOp.existsRow(connection, "dev_user", "rowkey01");
        boolean result = HBaseOp.existsRow(connection, "test1", "test");
        Assert.assertTrue(result);
    }

    @Test
    @Ignore
    public void getRow() {
        Get get = new Get(Bytes.toBytes("rowkey111"));
        Result result = HBaseOp.getRow(connection, "dev_user", get);
        byte[] bytes = result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("signature"));
        String signature = new String(bytes);
        Assert.assertEquals("年轻", signature);
    }

    // ----------------------- Scanner 过滤器 --------------------------
    @Test
    @Ignore
    public void scanner() {

        FilterList filterList = new FilterList();
        // rowkey过滤器
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^rowkey.1$"));
        Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("cf2"));
        filterList.addFilter(rowFilter);
        filterList.addFilter(familyFilter);

        ResultScanner scanner = HBaseOp.scanner(connection, "dev_user", filterList);
        Iterator<Result> iterator = scanner.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            Result next = iterator.next();
            System.err.println(next.toString());
        }
        Assert.assertEquals(2, count);
    }


    @Test
    @Ignore
    public void incrementColumnValue() {
        /**
         * 如果该列的值不存在，则返回num值
         * 如果该列的值存在，并且是big-debian long型，则返回num+该列的值
         * 如果该列的值存在，不是big-debian long型，则抛出异常。
         * 写入的值读取出的字符非可视字符
         */
        long value = HBaseOp.incrementColumnValue(connection, "dev_user", "rowkey111", Bytes.toBytes("cf2"), Bytes.toBytes("count"), 5);
        Assert.assertEquals(10, value);
    }

    @Test
    @Ignore // 删除操作，最后测试
    public void deleteQualifier() {
        boolean result = HBaseOp.deleteQualifier(connection, "dev_user", "rowkey111", "cf2", "signature");
        Assert.assertTrue(result);
    }

    @Test
    @Ignore // 最后测试
    public void deleteColumnFamily() {
        boolean result = HBaseOp.deleteColumnFamily(connection, "dev_user", "cf2");
        Assert.assertTrue(result);
    }

    @Test
    @Ignore // 最后测试
    public void deleteTable() {
        boolean result = HBaseOp.deleteTable(connection, "dev_user");
        Assert.assertTrue(result);
    }
}