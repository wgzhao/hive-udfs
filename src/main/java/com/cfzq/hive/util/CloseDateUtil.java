package com.cfzq.hive.util;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.cfzq.hive.util.Configure;

public class CloseDateUtil {
	static final Log LOG = LogFactory.getLog(CloseDateUtil.class.getName());
	
	public static List<String> closeDateList = new ArrayList<String>();
	
	// public static final String PATH="input/tables/closedate";
	
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
				closeDateList.addAll(IOUtils.readLines(in, charset));
		    } catch (Exception e) {
		    		LOG.error("CloseDateUtil init failed", e);
		    } finally {
		    		if (in != null)
		    			IOUtils.closeQuietly(in);
		    }
    }

    public static void getCloseDate(){
        try {
            DFSUtil hf = new DFSUtil();
            java.util.Properties properties = System.getProperties();
            properties.setProperty("HADOOP_USER_NAME","hdfs");
            System.setProperties(properties);
            closeDateList = hf.readLines(Configure.CLOSE_PATH);
            if(closeDateList == null || closeDateList.size() == 0){
                throw new Exception("未读取到close文件，或close文件为空");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static boolean isCloseDate(String date) {
        return closeDateList.contains(date);
    }

    public static String getNextExchangeDay(String date) {
        String next_date = nextDay(date);
        while (!isCloseDate(next_date)) {
            next_date = nextDay(next_date);
        }
        return next_date;
    }

    public static String getLastExchangeDay(String date) {
        String last_date = lastDay(date);
        while (!isCloseDate(last_date)) {
            last_date = lastDay(last_date);
        }
        return last_date;
    }

//	public static String firstDayByIntervel(String date,int interval_type){
//        Date end_date = DateHelper.getDate(date, DateHelper.YYYYMMDD);
//        return firstDayByIntervel(end_date,interval_type);
//    }
    
    public static String getPeriodExchangeDay(String date, int day) throws HiveException {
    	String last_date = date;
        while (!closeDateList.contains(last_date)) {
            last_date = lastDay(last_date);
            if (last_date.compareTo(closeDateList.get(0)) < 0) {
    			throw new HiveException("index out of closeDateList[" + last_date + "]");
    		}
        }
        int i = closeDateList.indexOf(last_date);
       
        return closeDateList.get(i + day);
    }
    
    public static String getFirstPeriodExchangeDay(String date, int day) {
        String last_date = addDate(date, day);
        while (!closeDateList.contains(last_date)) {
            if (day < 0) {
                if (closeDateList.isEmpty() || closeDateList.get(closeDateList.size() - 1).compareTo(last_date) < 0) {
                    return null;
                }
                last_date = nextDay(last_date);
            } else {
                if (closeDateList.isEmpty() || closeDateList.get(0).compareTo(last_date) > 0) {
                    return null;
                }
                last_date = lastDay(last_date);
            }
        }
        return last_date;
    }
    
    /**
     * type:
     *   1 = week
     *   2 = month
     *   3 = year
     * @throws HiveException 
     */
    public static String getFirstCalendarExchangeDay(String date, int type) throws HiveException {
		String last_date = getFirstCalendarDay(date, type);
	    while (!closeDateList.contains(last_date)) {
	    		last_date = nextDay(last_date);
	    		if (last_date.compareTo(closeDateList.get(closeDateList.size() - 1)) > 0) {
	    			throw new HiveException("index out of closeDateList[" + last_date + "]");
	    		}
	    }
	    return last_date;
	}
    
    /**
     * type:
     *   1 = week
     *   2 = month
     *   3 = year
     */
    public static String getFirstCalendarDay(String date, int type) {
    		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			c.setTime(sdf.parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		if (type == 1) {
			// c.add(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY ? -6 : Calendar.MONDAY - c.get(Calendar.DAY_OF_WEEK));
			c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		} else if (type == 2) {
			c.set(Calendar.DAY_OF_MONTH, 1);
		} else if (type == 3) {
			c.set(Calendar.DAY_OF_YEAR, 1);
		}
		return sdf.format(c.getTime());
    }
    
    /**
     * type:
     *   1 = week
     *   2 = month
     *   3 = year
     * @throws HiveException 
     */
    public static String getLastCalendarExchangeDay(String date, int type) throws HiveException {
		String last_date = getLastCalendarDay(date, type);
	    while (!closeDateList.contains(last_date)) {
	    		last_date = lastDay(last_date);
	    		if (last_date.compareTo(closeDateList.get(0)) < 0) {
	    			throw new HiveException("index out of closeDateList[" + last_date + "]");
	    		}
	    }
	    return last_date;
	}
    
    /**
     * type:
     *   1 = week
     *   2 = month
     *   3 = year
     */
    public static String getLastCalendarDay(String date, int type) {
    		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			c.setTime(sdf.parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		if (type == 1) {
			// c.add(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY ? -6 : Calendar.MONDAY - c.get(Calendar.DAY_OF_WEEK));
			c.set(Calendar.DAY_OF_WEEK, c.getActualMaximum(Calendar.DAY_OF_WEEK));
		} else if (type == 2) {
			c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
		} else if (type == 3) {
			c.set(Calendar.DAY_OF_YEAR, c.getActualMaximum(Calendar.DAY_OF_YEAR));
		}
		return sdf.format(c.getTime());
    }

    public static long abs(long l1, long l2) {
    		return l1 > l2 ? l1 - l2 : l2 - l1;
    }
    public static long hsDateDiff(String date1, String date2) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");	
		try {
			Date d1 = sdf.parse(date1);
			Date d2 = sdf.parse(date2);
			
			return TimeUnit.MILLISECONDS.toDays(abs(d1.getTime(), d2.getTime()));
		} catch (ParseException e) {
			return -1;
		}
	}
    
    public static String toHsDate(String date) {
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");		
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
		try {
			return sdf2.format(sdf.parse(date));
		} catch (ParseException e) {
			return null;
		}
	}
    
    public static String fromHsDate(String date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");		
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		try {
			return sdf2.format(sdf.parse(date));
		} catch (ParseException e) {
			return null;
		}
	}
    
    public static String addDate(String date, int days) {
    		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			c.setTime(sdf.parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		c.add(Calendar.DAY_OF_MONTH, days);
		return sdf.format(c.getTime());
    }
    
    public static String nextDay(String date) {
    		return addDate(date, 1);
    }
    
    public static String lastDay(String date) {
    		return addDate(date, -1);
    }
    
    public static String getPrevCloseDay(String date, String type) {
    		String newDate = null;
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			c.setTime(sdf.parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
       
		if (type.toString().equals("month")) {
			c.add(Calendar.MONTH, -1);
		} else if (type.toString().equals("quoter")) {
			c.add(Calendar.MONTH, -3);    			
		} else if (type.toString().equals("halfyear")) {
			c.add(Calendar.MONTH, -6);
		} else if (type.toString().equals("year")) {
			c.add(Calendar.YEAR, -1);
		}
		newDate = sdf.format(c.getTime());    
	    return  CloseDateUtil.getNextExchangeDay(newDate);
    }
    
//    public static String firstDayByIntervel(Date date,int interval_type){
//        Calendar cal = Calendar.getInstance();
//        switch (interval_type){
//            case 1:
//                cal = DateHelper.addMonth(date, -1); break;
//            case 2:
//                cal = DateHelper.addMonth(date, -3); break;
//            case 3:
//                cal = DateHelper.addMonth(date, -6); break;
//            case 4:
//                cal = DateHelper.addMonth(date, -12); break;
//            case 5:
//                cal = DateHelper.addWeek(date, -1); break;
//			case 9:
//				cal = DateHelper.getFristDayOfYear(date); break;
//            default:
//                return "0";
//        }
//        return DateHelper.formatDate(cal.getTime(), DateHelper.YYYYMMDD);
//    }

    public static void checkHsDate(String s) throws HiveException {
        if (!s.matches("((((19|20)\\d{2})(0?[13578]|1[02])(0?[1-9]|[12]\\d|3[01]))|(((19|20)\\d{2})(0?[469]|11)(0?[1-9]|[12]\\d|30))|(((19|20)\\d{2})0?2(0?[1-9]|1\\d|2[0-8]))|((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))0?2(0?[1-9]|[12]\\d)))$")) {
            throw new HiveException(String.format("[%s]不符合yyyyMMdd日期格式", s));
            // System.out.println(s);
        }
    }

    public static void checkNormalDate(String s) throws HiveException {
        if (!s.matches("((((19|20)\\d{2})-(0?(1|[3-9])|1[012])-(0?[1-9]|[12]\\d|30))|(((19|20)\\d{2})-(0?[13578]|1[02])-31)|(((19|20)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))-0?2-29))$")) {
            throw new HiveException(String.format("[%s]不符合yyyy-MM-dd日期格式", s));
            // System.out.println(s);
        }
    }

    public static int getCountExchangeDay(String date1, String date2) {
        String last_date = date1;
        int count = 0;
        while (last_date.compareTo(date2) <= 0) {
            if (closeDateList.contains(last_date)) {
                count ++;
            }
            last_date = nextDay(last_date);
        }

        return count;
    }

    public static void main(String[] args) {
        getCloseDate();
    }
}
