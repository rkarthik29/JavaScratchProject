package com.clouder.rg.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.simple.JSONObject;

public class HiveConnect {
    static boolean wait_on_this = true;
    static AtomicInteger counter = new AtomicInteger(0);
    static HiveConnect hiveConnect = null;
    static int loop_count;
    static long testStartTime;
    List<ConnectionThread> threads = new ArrayList<ConnectionThread>();
    static List<List<Long>> metrics = new ArrayList<List<Long>>();

    public static void main(String[] args) {
        testStartTime = System.currentTimeMillis();
        HiveConnect.loop_count = 20;
        hiveConnect = new HiveConnect();
        hiveConnect.start();
        // no of querys to run

        System.out.println("started");

        // hiveConnect.notifyAll();
        // synchronized (hiveConnect) {
        // System.out.println("notifying");
        // hiveConnect.notifyAll();
        // }
        wait_on_this = false;
        while (counter.intValue() < (loop_count)) {

        }
        Map<String, List<List<Long>>> data = new HashMap<String, List<List<Long>>>();
        data.put("metrics", metrics);
        JSONObject jsonObj = new JSONObject(data);
        System.out.println(jsonObj.toJSONString());

    }

    HiveConnect() {
        // Connection conn;
        try {
            for (int i = 0; i < loop_count; i++) {
                ConnectionThread t = new ConnectionThread((long) i);
                System.out.println("starting thread " + t.getId());
                Thread.sleep(100);
                threads.add(t);
            }

            // for (ConnectionThread connectionThread : threads) {
            // connectionThread.join();
            //
            // }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void start() {
        try {
            for (ConnectionThread connectionThread : threads) {
                connectionThread.start();
                // connectionThread.join();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    class ConnectionThread extends Thread {

        private Long threadId;

        ConnectionThread(Long threadId) {
            this.threadId = threadId;
        }

        @Override
        public void run() {
            Connection conn;
            // while (wait_on_this) {
            //
            // }
            // HiveConnect.metrics.put(threadId,);
            ArrayList<Long> timeList = new ArrayList<Long>();
            timeList.add(threadId);
            timeList.add(HiveConnect.testStartTime);
            // String JDBC_DB_URL =
            // "jdbc:hive2://hdp31rkn1.field.hortonworks.com:2181,hdp31rkn2.field.hortonworks.com:2181,hdp31rkn0.field.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive";
            String JDBC_DB_URL = "jdbc:hive2://hdp31rkn3.field.hortonworks.com:10500/tpcds_bin_partitioned_orc_10";
            try {
                // synchronized (HiveConnect.hiveConnect) {
                // HiveConnect.hiveConnect.wait();
                // }
                System.out.println("connecting");
                Class.forName("org.apache.hive.jdbc.HiveDriver");
                long startTime = System.currentTimeMillis();
                timeList.add(startTime);
                conn = DriverManager.getConnection(JDBC_DB_URL);
                long connTime = System.currentTimeMillis();
                timeList.add(connTime);
                Statement stmt = conn.createStatement();
                String query = "select  " + "i_brand_id brand_id, i_brand brand, "
                        + "sum(ss_ext_sales_price) ext_price from date_dim, store_sales,item "
                        + "where d_date_sk = ss_sold_date_sk " + "and ss_item_sk = i_item_sk "
                        + "and i_manager_id=13 and d_moy=11 " + "and d_year=1999 group by i_brand, i_brand_id "
                        + "order by ext_price desc, i_brand_id limit 100";
                ResultSet rs = stmt.executeQuery(query);
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                System.out.println("query done");
                timeList.add(System.currentTimeMillis());
                timeList.add((long) count);
                metrics.add(timeList);
                conn.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            counter.getAndIncrement();
        }
    }
}
