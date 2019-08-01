package com.clouder.rg.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.json.simple.JSONObject;

public class GenerateData {

    public static void main(String[] args) {
        System.out.print(GenerateCustomer("12345"));
    }

    static String[] first_names = { "Alexander", "Theoder", "Franklin", "George", "Richard", "Jimmy", "Abraham",
            "William", "Ronald", "John", "Dwight", "Harry" };
    static String[] last_names = { "Hamilton", "Roosevelt", "Washington", "Bush", "Nixon", "Carter", "Lincoln",
            "Clinton", "Kennedy", "Reagen", "Adams", "Eisenhower", "Truman" };
    static String[] city = { "Orlando", "Atlanta", "New York", "Fairfax", "Memphis", "Lousiville", "Wayne", "Nashville",
            "Los Angeles", "Seattle" };
    static String[] state = { "Florida", "Georgia", "New York", "Virginia", "Tennessee", "Kentucky", "New Jersey",
            "Tennessee", "California", "Washington" };

    public static String GenerateCustomer(String customerId) {

        JSONObject obj = new JSONObject();
        List<String> firstNames = Arrays.asList(first_names);
        List<String> lastNames = Arrays.asList(last_names);
        List<String> cities = Arrays.asList(city);
        List<String> states = Arrays.asList(state);

        Random random = new Random();

        String phone = (random.nextInt(7) + 2) + "" + (random.nextInt(9)) + "" + (random.nextInt(9)) + ""
                + (random.nextInt(7) + 2) + "" + (random.nextInt(9)) + "" + (random.nextInt(9)) + ""
                + (random.nextInt(9)) + "" + (random.nextInt(9)) + "" + (random.nextInt(9)) + "" + (random.nextInt(9));

        obj.put("phone", phone);
        obj.put("city", cities.get(random.nextInt(cities.size())));
        obj.put("state", states.get(random.nextInt(states.size())));
        obj.put("first_name", firstNames.get(random.nextInt(firstNames.size())));
        obj.put("last_name", lastNames.get(random.nextInt(lastNames.size())));
        obj.put("country", "USA");
        obj.put("update_dt", System.currentTimeMillis());
        obj.put("customerid", customerId);

        return obj.toJSONString();
    }
}
