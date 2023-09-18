package com.account.analyzer.hadoop.udf.function;

import com.cfzq.hive.util.DFSUtil;
import com.cfzq.hive.util.Configure;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.*;

/**
 * 拉链表补数据UDF
 * @author chenjc23273
 */
@Description(name = "hiveudf.FillDataByClosedayUDF")
public class FillDataByClosedayUDF extends UDF {

    private static final Logger log = LoggerFactory.getLogger(FillDataByClosedayUDF.class);
    private static final String BEGIN_DATE_DEFAULT = "20170901";
    private static final String END_DATE_DEFAULT = "20180901";
    //字段间分割串
    private static final String C_SPLIT = "!#";
    //数据行分割串
    private static final String R_SPLIT = "!@#";
    private static final String SORT_DESC = "desc";
    private static final String SORT_ASC = "asc";
    private static final String SORT_DEFAULT = SORT_ASC;
    private static final String ERROR_CODE = "-1";
    private static final int TRADE_DAY_MAP_MAX_SIZE = 255;

    // public static final String CLOSE_PATH = "input/tables/closedate";
    //所有交易日集合
    private static List<String> allCloseDateList = new ArrayList<String>();
    //存放维度内交易日集合，防止重复占用资源 例如：key为“20170901-20180901”，value为存放这个时间维度内的交易日集合
//    private static Map<String,List<String>> tradeDayListMap = new ConcurrentHashMap<String,List<String>>();


static {
    InputStream in = null;
    try {
        // FileSystem fs = FileSystem.get(URI.create(Configure.CLOSE_PATH), new Configuration());
        
        // Path paths = new Path(Configure.CLOSE_PATH);
        // if (!fs.exists(paths)) {
        // 		throw new Exception(paths + " not exists!");
        // }
        // if (fs.isDirectory(paths)) {
        //     FileStatus[] status = fs.listStatus(paths);
        //     for (FileStatus file : status) {
        //         if (file.getPath().getName().endsWith(".crc")) {
        //             continue;
        //         }
        //         in = fs.open(new Path(file.getPath().toString()));
        //         closeDateList.addAll(IOUtils.readLines(in));
        //     }
        // } else {
        //     in = fs.open(new Path(Configure.CLOSE_PATH));
        //     closeDateList.addAll(IOUtils.readLines(in));
        // }
        DFSUtil dfs = new DFSUtil();
        in =  dfs.getInputStream(Configure.CLOSE_PATH);
        Charset charset = Charset.forName("utf8");
        allCloseDateList.addAll(IOUtils.readLines(in, charset));
        } catch (Exception e) {
            log.error("CloseDateUtil init failed", e);
        } finally {
            if (in != null)
                IOUtils.closeQuietly(in);
        }
    }

    public static Text evaluate(final Text text) {
        return evaluate(text, new Text(BEGIN_DATE_DEFAULT), new Text(END_DATE_DEFAULT));
    }

    public static Text evaluate(final Text text, final Text beginDateText, final Text endDateText) {
        return evaluate(text, beginDateText, endDateText, new Text("0"), new Text("1"));
    }

    public static Text evaluate(final Text text, final Text beginDateText, final Text endDateText,
                                             final Text initDateIndexText, final Text partInitDateIndexText) {
        return evaluate(text, beginDateText, endDateText, initDateIndexText, partInitDateIndexText, null, null, null);
    }

    public static Text evaluate(final Text text, final Text beginDateText, final Text endDateText,
                                             final Text initDateIndexText, final Text partInitDateIndexText,
                                             final Text tagNameText, final Text beginAmountIndexText, final Text currentAmountIndexText) {
        return evaluate(text, beginDateText, endDateText, initDateIndexText, partInitDateIndexText,
                tagNameText, beginAmountIndexText, currentAmountIndexText, new Text(SORT_DEFAULT));
    }

    public static Text evaluate(final Text text, final Text beginDateText, final Text endDateText,
                                             final Text initDateIndexText, final Text partInitDateIndexText,
                                             final Text tagNameText, final Text beginAmountIndexText, final Text currentAmountIndexText,
                                             final Text sortText) {

        try {
//            System.out.println("-------------进入FillDataByClosedayUDF");
//            log.info("-------------------进入FillDataByClosedayUDF");
            if(null == text || "".equals(text.toString())){
                log.info("CopyDataByClosedayUDF 入参字符串为null或为空串");
                return new Text(ERROR_CODE);
            }

            String beginDate = textToString(beginDateText, BEGIN_DATE_DEFAULT);
            String endDate = textToString(endDateText, END_DATE_DEFAULT);
            String sort = textToString(sortText, SORT_DEFAULT);
            //获取时间维度内的交易日
            List<String> intervalTradeDate = getTradeDateByIntervalDate(allCloseDateList, beginDate, endDate, sort);

            String[] rows = text.toString().split(R_SPLIT);
            String initDateIndex = textToString(initDateIndexText, ERROR_CODE);
            String partInitDateIndex = textToString(partInitDateIndexText, ERROR_CODE);
            String tagName = textToString(tagNameText, ERROR_CODE);
            String beginAmountIndex = textToString(beginAmountIndexText, ERROR_CODE);
            String currentAmountIndex = textToString(currentAmountIndexText, ERROR_CODE);
            //如果init_date和part_init_date都没有 表示该数据没有时间分割，不需要补数据，直接return
            if(ERROR_CODE.equals(initDateIndex) && ERROR_CODE.equals(partInitDateIndex)){
                return text;
            }

            // 根据所有行数集合 存储到map里
            Map<String,String> columnsMap = createColumnsMapByRows(rows, initDateIndex, partInitDateIndex);

            //对数据进行补全
            List<String> finalList = fillData(columnsMap, intervalTradeDate, initDateIndex, partInitDateIndex, tagName, beginAmountIndex, currentAmountIndex,sort);

            //把集合转成字符串，用“！@#”拼接
            String finalStr = list2String(finalList);

            return new Text(finalStr);
        } catch (Exception e) {
            e.printStackTrace();
            log.debug("CopyDataByClosedayUDF error", e);
            return new Text(ERROR_CODE);
        }
    }

    /**
     * 对数据集合进行补全
     * @param columnsMap 已有的数据集合
     * @param tradeDateList 交易日集合
     * @param initDateIndex
     * @param partInitDateIndex
     * @return
     */
    private static List<String> fillData(Map<String,String> columnsMap, List<String> tradeDateList,
                                                String initDateIndex, String partInitDateIndex,
                                         String tagName, String beginAmountIndex, String currentAmountIndex,String sort){

        List<String> list = new LinkedList<String>();
        for (int i = 0; i < tradeDateList.size(); i++) {
            String tradeDate = tradeDateList.get(i);
            if(i==0) {
                if(null!=columnsMap.get(tradeDate)){
                    list.add(columnsMap.get(tradeDate));
                }
                continue;
            }
            String lastTradeDate = tradeDateList.get(i-1);
            if(!columnsMap.containsKey(tradeDate) && columnsMap.containsKey(lastTradeDate)){
                String lastColumnsStr = columnsMap.get(lastTradeDate);
                String[] columns = lastColumnsStr.split(C_SPLIT).clone();
                if(!ERROR_CODE.equals(initDateIndex)){
                    columns[Integer.valueOf(initDateIndex)] = tradeDate;
                }
                if(!ERROR_CODE.equals(partInitDateIndex)){
                    columns[Integer.valueOf(partInitDateIndex)] = tradeDate;
                }
                //对字段进行特殊处理
                columns = specialColumnFilter(columns,tagName,beginAmountIndex,currentAmountIndex,sort);
                //数组转字符串
                String row = array2String(columns);
                if(null!=row){
                    columnsMap.put(tradeDate, row);
                    list.add(row);
                }
            }else{
                if(columnsMap.containsKey(tradeDate)){
                    list.add(columnsMap.get(tradeDate));
                }
            }
        }
//        return columnsMap;
        return list;
    }

    /**
     * 对标签进行特殊处理，比如补全数据的时候，T日的begin_amount等于T-1的current_amount而不是T-1日的begin_amount
     * @param columns
     * @param tagName
     * @param specialColumnIndex1
     * @param specialColumnIndex2
     * @return
     */
    private static String[] specialColumnFilter(String[] columns, String tagName,
                                                String specialColumnIndex1, String specialColumnIndex2, String sort){

        if(ERROR_CODE.equals(tagName) || ERROR_CODE.equals(specialColumnIndex1) || ERROR_CODE.equals(specialColumnIndex2)) {
            return columns;
        }

        //datasecumshare清洗
        if(tagName.equals(TagNameEnum.DATABANKMSHARE.getName()) || tagName.equals(TagNameEnum.DATASECUMSHARE.getName())){

            if(SORT_DESC.equals(sort)){
                String begin_amount = columns[Integer.valueOf(specialColumnIndex1)];
                if(new BigDecimal(0).compareTo(new BigDecimal(begin_amount))!=0){
                    columns[Integer.valueOf(specialColumnIndex2)] = begin_amount;
                }else{
                    return null;
                }
            }else{
                String current_amount = columns[Integer.valueOf(specialColumnIndex2)];
                if(new BigDecimal(0).compareTo(new BigDecimal(current_amount))!=0){
                    columns[Integer.valueOf(specialColumnIndex1)] = current_amount;
                }else{
                    return null;
                }
            }
        }
        //datastock清洗
        if(tagName.equals(TagNameEnum.DATASTOCK.getName())){
            String correct_amount = columns[Integer.valueOf(specialColumnIndex1)];
            String current_amount = columns[Integer.valueOf(specialColumnIndex2)];
            String exchange_type = columns[5];
            if("G".equals(exchange_type) || "S".equals(exchange_type)){
                if(new BigDecimal(correct_amount).compareTo(new BigDecimal(0))==0 && new BigDecimal(current_amount).compareTo(new BigDecimal(0))==0){
                    return null;
                }
            }else{
                if(new BigDecimal(correct_amount).add(new BigDecimal(current_amount)).compareTo(new BigDecimal(0))==0){
                    return null;
                }
            }
        }
        //dataofstock清洗
        if(tagName.equals(TagNameEnum.DATAOFSTOCK.getName())){

            if(SORT_DESC.equals(sort)){
                String begin_share = columns[Integer.valueOf(specialColumnIndex1)];
                if(new BigDecimal(begin_share).compareTo(new BigDecimal(0))==0){
                    return null;
                }else{
                    columns[Integer.valueOf(specialColumnIndex2)] = begin_share;
                }
            }else{
                String current_share = columns[Integer.valueOf(specialColumnIndex2)];
                if(new BigDecimal(current_share).compareTo(new BigDecimal(0))==0){
                    return null;
                }else{
                    columns[Integer.valueOf(specialColumnIndex1)] = current_share;
                }
            }
        }
        //datafund清洗
        if(tagName.equals(TagNameEnum.DATAFUND.getName())){
            //position_str字段
            columns[Integer.valueOf(specialColumnIndex1)] = "X";
        }
        return columns;
    }

    /**
     * 将字段数组用“！#”拼上 转成字符串
     * @param columns
     * @return
     */
    private static String array2String(String[] columns){
        if(null==columns) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (String str : columns){
            sb.append(str).append(C_SPLIT);
        }
        return sb.toString().substring(0,sb.length()-2);
    }

    /**
     * 把list转成字符串，用“！@#”隔开
     * @param list
     * @return
     */
    private static String list2String(List<String> list){
        StringBuffer sb = new StringBuffer();
        for (String str : list){
            sb.append(str).append(R_SPLIT);
        }
        return sb.toString().substring(0, sb.length()-3);
    }

    /**
     * 根据传进来的数据行集合，将其分割成map，好用于后续进行补全
     * @param rows  存放着所有行数集合
     * @param initDateIndex         init_date存在该表第几个位置
     * @param partInitDateIndex     part_init_date存在该表的第几个位置
     * @return Map key-value（日期-日期对应的那天字符串数据）
     */
    private static Map<String,String> createColumnsMapByRows(String[] rows, String initDateIndex, String partInitDateIndex){
        //存放 key-value（日期-日期对应的那天字符串数据）
        Map<String,String> columnsMap = new HashMap<String,String>();
        for (String columns : rows){

            String[] column = columns.split(C_SPLIT);
            String init_date = "";
            String part_init_date = "";
            if(!ERROR_CODE.equals(initDateIndex)){
                init_date = column[Integer.valueOf(initDateIndex)];
            }
            if (!ERROR_CODE.equals(partInitDateIndex)){
                part_init_date = column[Integer.valueOf(partInitDateIndex)];
            }
            if(!"".equals(part_init_date)){
                columnsMap.put(part_init_date, columns);
            }else{
                //如果时间为空串，那么当这条数据不存在
                if("".equals(init_date)){
                    continue;
                }
                columnsMap.put(init_date, columns);
            }
        }
        return columnsMap;
    }

    /**
     * 把Text转成String 如果Text为空或者为null 用def赋值
     * @param text
     * @param def
     * @return
     */
    public static String textToString(Text text, String def){
        return (text==null || "".equals(text.toString())) ? def:text.toString();

    }

    /**
     * 根据交易日集合、开始时间、结束时间 获取时间范围内的所有交易日集合
     * @param allCloseDateList
     * @param beginDate
     * @param endDate
     * @return
     */
    public static List<String> getTradeDateByIntervalDate(List<String> allCloseDateList, String beginDate, String endDate, String sort){

//        if(tradeDayListMap.size()>=TRADE_DAY_MAP_MAX_SIZE){
////            tradeDayListMap.clear();
////        }

//        if(tradeDayListMap.containsKey(beginDate + "-" + endDate + "-" + sort)){
//            return  tradeDayListMap.get(beginDate + "-" + endDate + "-" + sort);
//        }else{
            List<String> returnList = new ArrayList<String>();
            for (String date : allCloseDateList){
                if (Integer.valueOf(date)>=Integer.valueOf(beginDate) && Integer.valueOf(date)<=Integer.valueOf(endDate)){
                    returnList.add(date);
                }
            }
//            tradeDayListMap.put(beginDate + "-" + endDate + "-" + sort, returnList);
            if(SORT_DESC.equals(sort)){
                Collections.reverse(returnList);
//                tradeDayListMap.put(beginDate + "-" + endDate + "-" + sort, returnList);
            }
            return returnList;
//        }
    }

    /**
     * 特殊处理的清洗
     */
    public enum TagNameEnum{
        DATASECUMSHARE("datasecumshare"),
        DATABANKMSHARE("databankmshare"),
        DATASTOCK("datastock"),
        DATAFUND("datafund"),
        DATAOFSTOCK("dataofstock");
        private final String name;
        private TagNameEnum(String name){
            this.name = name;
        }
        public String getName() {
            return name;
        }
    }
}
