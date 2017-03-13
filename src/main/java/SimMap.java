import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;
import java.net.URL;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Created by liuxiaojun on 2017/1/10.
 */
public class SimMap extends Mapper<LongWritable, Text, Text, Text> {
    private static final int wantedLength = 12;
    public static String timeHour;
    public static HashMap<String, String> apMacCityCode = new HashMap<>();
    public static HashMap<String, String> apMacOrgId = new HashMap<>();

    public void apMacCityFileInit(Context context) throws IOException{
        FileReader reader = new FileReader("ap_city.txt");
        BufferedReader br = new BufferedReader(reader);
        String s1 = null;
        while ((s1 = br.readLine()) != null) {
            if ("".equals(s1)) continue;
            String[] arr = s1.split(",");
            if (arr.length == 2) {
                apMacCityCode.put(arr[0], arr[1]);
            }
        }
        br.close();
        reader.close();
    }

    public void apMacOrgIdInit(Context context) throws IOException{
        FileReader reader = new FileReader("apmac_orgId.txt");
        BufferedReader br = new BufferedReader(reader);
        String s1 = null;
        while ((s1 = br.readLine()) != null) {
            if ("".equals(s1)) continue;
            String[] arr = s1.split(",");
            if (arr.length == 2) {
                apMacOrgId.put(arr[0], arr[1]);
            }
        }
        br.close();
        reader.close();
    }

    protected void setup(Mapper.Context context)
            throws IOException, InterruptedException {
        timeHour = context.getConfiguration().get("timeHour");
        apMacCityFileInit(context);
        apMacOrgIdInit(context);
    }

    Text keyText = new Text();
    Text valueText = new Text();

    public static boolean isNumeric(String str){
        if (str == null || str.length() == 0)
            return false;
        for(int i=str.length();--i>=0;){
            int chr=str.charAt(i);
            if(chr<48 || chr>57){
                return false;
            }
        }
        return true;
    }

    public static HashMap<String, String> result = new HashMap<>();

    public static String getCityCode(String apMac){
        String cityCode = "000000";
        if (apMacCityCode.containsKey(apMac)){
            cityCode = apMacCityCode.get(apMac);
        }
        return cityCode;
    }

    public static String getOrgName(String apMac){
        String orgName = "";
        if (apMacOrgId.containsKey(apMac)){
            orgName = apMacOrgId.get(apMac);
            if (orgName == "NULL" || orgName.trim().length() == 0 ){
                orgName = "";
            }
        }
        return orgName;
    }

    public static HashMap<String, String> expandLine(String collectLine, String topic
    ) throws IOException, InterruptedException {
        String[] source_list = collectLine.split(",", -1);
        int arraylength = source_list.length;
        if (collectLine.trim().length() == 0 || arraylength < 6) {
            System.out.println("arraylength < 6");
            System.out.println("collectLine===>"+collectLine);
            return result;
        }

        String raw_url = source_list[4];
        if (raw_url.contains("://") || raw_url.contains(",")) {
            try {
                raw_url = URLEncoder.encode(raw_url, "utf-8");
            } catch (UnsupportedEncodingException e) {
                System.out.println(":// encode");
                System.out.println("collectLine===>"+collectLine);
                return result;
            }
        }

        String decode_url = "";
        decode_url = URLDecoder.decode(raw_url, "utf-8");

        String[] expanded_list = new String[wantedLength];
        expanded_list[0] = source_list[0];
        expanded_list[1] = source_list[1];
        expanded_list[2] = source_list[2];
        expanded_list[3] = source_list[3];
        expanded_list[4] = raw_url;
        expanded_list[5] = source_list[5];

        URL url = new URL(decode_url);
        expanded_list[6] = url.getProtocol(); //协议
        expanded_list[7] = url.getHost(); //host
        expanded_list[8] = url.getPath().toString().replace(",",""); // 路径

        String getQuery = "";
        if (url.getQuery() != null){
            getQuery = url.getQuery().replace(",","");
        }
        expanded_list[9] = getQuery; // 请求参数

        expanded_list[10] = getCityCode(source_list[2]); //cityCode
        expanded_list[11] = getOrgName(source_list[2]);  //lineId  apMacOrgId

        HashMap<String, String> result = new HashMap<String, String>();
        result.put("value", org.apache.hadoop.util.StringUtils.join(",", expanded_list));

        String yearStr = timeHour.substring(0,4);
        String monthStr = timeHour.substring(4,6);
        String dateStr = timeHour.substring(6,8);
        String hourStr = timeHour.substring(8,10);

        result.put("outPath", String.format("/expand_prod/%s/%s/%s/%s/%s", topic, yearStr, monthStr, dateStr, hourStr));
        return result;
    }

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] pathParts = ((FileSplit) context.getInputSplit()).getPath().toString().split("/", -1);
        HashMap<String, String> expanded;
        String topic = "internet";
        expanded = expandLine(value.toString(),topic); //此处测试是internet etl

        if (expanded.containsKey("outPath")) {
            keyText.set(expanded.get("outPath"));
            valueText.set(expanded.get("value"));
            context.write(keyText, valueText);
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
    }

    public static void main(String[] args) {
    }
}
