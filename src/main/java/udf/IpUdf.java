package udf;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@UDFType(deterministic = false, stateful = true)
public class IpUdf extends UDF {

    static final String IP_FILE = "data.csv";
    private static CodeToIp CODE_TO_IP;

    public String evaluate(String input) {
        try {
            System.out.println("Value to parse: " + input);
            if (CODE_TO_IP == null) {
                CODE_TO_IP = init();
            }
            System.out.println("CodeToIp initialized");
            return CODE_TO_IP.getGeoId(input.trim());
        } catch (Throwable e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static CodeToIp init() {
        CodeToIp map = new CodeToIp();
        try {
            URL url = Resources.getResource(IP_FILE);
            System.out.println(url);
            List<String> lines = Resources.readLines(url, Charsets.UTF_8);
            System.out.println("lines parsed:" + lines.size());
            int index = 0;
            for (String line : lines) {
                String[] parts = line.split(",");
                if (parts.length >= 6) {
                    SubnetUtils utils = new SubnetUtils(parts[0].trim());
                    long begin = ipToLong(utils.getInfo().getLowAddress()) - 1;
                    long end = ipToLong(utils.getInfo().getHighAddress()) + 1;
                    map.addNetwork(begin, end, parts[1].trim());
                }
                index++;
                if (index % 1000 == 0) {
                    System.out.println("lines parsed:" + index);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Throwables.propagate(e);
        }
        return map;
    }

    static long ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");
        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {

            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }
        return result;
    }

    public static class CodeToIp {

        private final TreeMap<Long, Entry> map = new TreeMap<>();

        void addNetwork(long startIp, long endIp, String geoId) {
            map.put(startIp, new Entry(endIp, geoId));
        }

        public String getGeoId(String ip) {
            long parsed = ipToLong(ip);
            Map.Entry<Long, Entry> entry = map.floorEntry(parsed);
            if (entry != null && parsed <= entry.getValue().end) {
                return new String(entry.getValue().geoId);
            } else {
                return null;
            }
        }
    }

    private static class Entry {

        private final long end;
        private final byte[] geoId;

        Entry(long end, String geoId) {
            this.end = end;
            this.geoId = geoId.getBytes();
        }
    }
}
