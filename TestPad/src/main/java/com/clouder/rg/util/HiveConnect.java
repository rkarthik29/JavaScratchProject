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
    static List<String> query = new ArrayList<String>();
    static boolean wait_on_this = true;
    static AtomicInteger counter = new AtomicInteger(0);
    static HiveConnect hiveConnect = null;
    static int loop_count;
    static long testStartTime;
    List<ConnectionThread> threads = new ArrayList<ConnectionThread>();
    static List<List<Long>> metrics = new ArrayList<List<Long>>();

    public static void main(String[] args) {
        try {
            query.add("select " + "i_brand_id brand_id, i_brand brand, "
                    + "sum(ss_ext_sales_price) ext_price from date_dim,store_sales,item "
                    + "where d_date_sk = ss_sold_date_sk " + "and ss_item_sk =i_item_sk "
                    + "and i_manager_id=13 and d_moy=11 " + "and d_year=1999 group by i_brand, i_brand_id "
                    + "order by ext_price desc, i_brand_id limit 100");
            query.add(
                    "select cc_call_center_id Call_Center, cc_name Call_Center_Name, cc_manager Manager, sum(cr_net_loss) Returns_Loss from call_center, catalog_returns, date_dim, customer, customer_address, customer_demographics, household_demographics where cr_call_center_sk       = cc_call_center_sk and     cr_returned_date_sk     = d_date_sk and     cr_returning_customer_sk= c_customer_sk and     cd_demo_sk              = c_current_cdemo_sk and     hd_demo_sk              = c_current_hdemo_sk and     ca_address_sk           = c_current_addr_sk and     d_year                  = 2000 and     d_moy                   = 12 and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown') or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree')) and     hd_buy_potential like 'Unknown%'and     ca_gmt_offset           = -7 group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status order by sum(cr_net_loss) desc");
            query.add(
                    "select  a.ca_state state, count(*) cnt from customer_address a ,customer c ,store_sales s ,date_dim d ,item i where       a.ca_address_sk = c.c_current_addr_sk and c.c_customer_sk = s.ss_customer_sk and s.ss_sold_date_sk = d.d_date_sk and s.ss_item_sk = i.i_item_sk and d.d_month_seq = (select distinct (d_month_seq) from date_dim where d_year = 2002 and d_moy = 3 ) and i.i_current_price > 1.2 * (select avg(j.i_current_price) from item j where j.i_category = i.i_category) group by a.ca_state having count(*) >= 10 order by cnt, a.ca_state limit 100");
            testStartTime = System.currentTimeMillis();
            try {
                HiveConnect.loop_count = Integer.parseInt(args[0]);
            } catch (Exception e) {
                HiveConnect.loop_count = 10;
            }
            hiveConnect = new HiveConnect();
            hiveConnect.start();
            // no of querys to run
            Thread.sleep(100);
            System.out.println("starting to query");

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
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    HiveConnect() {
        // Connection conn;
        try {
            for (int i = 0; i < HiveConnect.loop_count; i++) {
                ConnectionThread t = new ConnectionThread((long) i);
                System.out.println("starting thread " + t.getId());
                // Thread.sleep(100);
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
                Thread.sleep(100);
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
            // "jdbc:hive2://hdp31rkn1.field.hortonworks.com:2181,"
            // + "hdp31rkn2.field.hortonworks.com:2181," +
            // "hdp31rkn0.field.hortonworks.com:2181/;"
            // + "serviceDiscoveryMode=zooKeeper;" +
            // "zooKeeperNamespace=hiveserver2-interactive";
            String JDBC_DB_URL = "jdbc:hive2://hdp31rkn3.field.hortonworks.com:10500" + "/tpcds_bin_partitioned_orc_10";
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
                // String query = "select current_timestamp";

                // while (wait_on_this) {
                //
                // }
                int id = threadId.intValue() % 3;
                ResultSet rs = stmt.executeQuery(query.get(id));
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                System.out.println("query done");
                timeList.add(System.currentTimeMillis());
                timeList.add((long) count);
                timeList.add((long) id);
                metrics.add(timeList);
                conn.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            counter.getAndIncrement();
        }
    }
}
