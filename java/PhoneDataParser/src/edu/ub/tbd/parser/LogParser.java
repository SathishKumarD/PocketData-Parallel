/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ub.tbd.parser;

import edu.ub.tbd.beans.LogLineBean;
import edu.ub.tbd.constants.AppConstants;
import edu.ub.tbd.entity.Analytics;
import edu.ub.tbd.entity.App;
import edu.ub.tbd.entity.Sql_log;
import edu.ub.tbd.entity.UnParsedSQLs;
import edu.ub.tbd.entity.Unparsed_log_lines;
import edu.ub.tbd.service.PersistanceFileService;
import edu.ub.tbd.service.PersistanceService;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author sathish
 */
public class LogParser implements Runnable {

    private final String sourceDir;
    private final String fileExtension;
    private final HashMap<String, String> appNameLookUpMap = new HashMap<>();

    private JSONParser parser = new JSONParser();

    //Cached data to be persisted during app shutdown
    //Use find on this regx and get guid from group 1
    //All Persistance Services below. They must be closed during shutdown => in shutDown()
    private final PersistanceService ps_UnparsedLogLines;
    private final PersistanceService ps_SqlLog ;
    private final PersistanceService ps_Analytics ;
    /**
     * @deprecated
     */
    private final PersistanceService ps_UnParsedSQLs ;

    private String longestLine = null; //TODO: <Sankar> Remove in final release
    private int longestLineLength = 0; //TODO: <Sankar> Remove in final release

    private int _userID = 0;
    private HashMap<String, App> appsMap = new HashMap<>();
      
    public LogParser(String _sourceDir, int _userID, String _fileExtension,HashMap<String, App> appsMap) throws Exception {

        this.sourceDir = _sourceDir;
        this.fileExtension = _fileExtension;
        this.appsMap =appsMap;
        this._userID = _userID;
        this.ps_UnparsedLogLines = new PersistanceFileService(AppConstants.DEST_FOLDER,
                AppConstants.OUTPUT_FILE_VALUE_SEPERATOR, Unparsed_log_lines.class, _userID);

        this.ps_SqlLog = new PersistanceFileService(AppConstants.DEST_FOLDER,
                AppConstants.OUTPUT_FILE_VALUE_SEPERATOR, Sql_log.class, _userID);

        this.ps_Analytics = new PersistanceFileService(AppConstants.DEST_FOLDER,
                AppConstants.OUTPUT_FILE_VALUE_SEPERATOR, Analytics.class, _userID);

        this.ps_UnParsedSQLs = new PersistanceFileService(AppConstants.DEST_FOLDER,
                AppConstants.OUTPUT_FILE_VALUE_SEPERATOR, UnParsedSQLs.class, _userID);
    }

    public void parseLogsAndWriteFile() throws Exception {
        ArrayList<String> logFilesToProcess = getLogFilesToProcessFromBaseGZ();
        int counter = 0;
        for (String filePath : logFilesToProcess) {
            //System.out.println(filePath);
            counter += 1;
            //TODO: <Satish> remove this. For testing purpose I am just parsing two files.
            //if(counter < 2)
            parseSingleLogFile(filePath);

        }
    }

    /**
     * Reads the base GZip file and returns only the file paths of the Log GZ
     * files in it
     *
     * @return the log.GZ file locations to be processed
     */
    private ArrayList<String> getLogFilesToProcessFromBaseGZ() {
        ArrayList<String> filesList = new ArrayList<>();
        File[] files = new File(sourceDir).listFiles();
        iterateFolders(files, filesList);
        return filesList;
    }

    /**
     * Helper method used in
     * {@link LogParser#getLogFilesToProcessFromBaseGZ() getLogFilesToProcessFromBaseGZ()}
     *
     * @param files
     * @param filesList Output Object
     */
    private void iterateFolders(File[] files, ArrayList<String> filesList) {
        for (File file : files) {
            String name = file.getName();
            if (file.isDirectory()) {
                iterateFolders(file.listFiles(), filesList); // Calls same method again.
            } else if (name.endsWith(fileExtension) && file.getAbsolutePath().contains("SQLite-Query-PhoneLab")) {
                filesList.add(file.getAbsolutePath());
            }
        }
    }

    private void parseSingleLogFile(String _sourceFile) throws Exception {
        BufferedReader br = null;
        String logLine = null;
        int lineNumber = 0;

        try {

            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(_sourceFile));
            br = new BufferedReader(new InputStreamReader(gzip));

            //System.out.println(sourceFile);
            while ((logLine = br.readLine()) != null) {
                lineNumber++;
                String raw_sql = "";
                try {
                    LogLineBean logLineBean = new LogLineBean(logLine.split(AppConstants.SRC_LOG_FILE_VALUE_SEPERATOR));

                    //TODO: <Sankar> Check how WARN and other levels function
                    if (LogLevel.INFO.equals(logLineBean.getLog_level())) { //Non-INFO log level has no JSON to process

                        JSONObject JSON_Obj = (JSONObject) parser.parse(logLineBean.getLog_msg());

                        switch ((String) JSON_Obj.get("Action")) {
                            case "APP_NAME": {
                                String appName = (String) JSON_Obj.get("AppName");
                                String key = logLineBean.get_app_key();
                                this.appNameLookUpMap.put(key, appName);
                                synchronized (this) {
                                    if (!appsMap.containsKey(appName)) {
                                        appsMap.put(appName, new App(appName));
                                    }
                                }
                            }
                            break;
                            case "SCHEMA":
                            case "TABLES": //TODO: <Professor> What are Actions : ["SCHEMA", "TABLES"]?
                            //TODO: <Sankar> Actions : ["SCHEMA", "TABLES"] is not into Analytics table yet
                            {
                                //This is a valid SQL log entry carrying line which needs to go to both sql_log & analytics
                                Sql_log sql_log = extractLog(logLineBean);
                                //ps_SqlLog.write(sql_log);

                                raw_sql = (String) JSON_Obj.get("Results");
                            }
                            break;
                            default:
                                //This is a valid SQL log entry carrying line which needs to go to both sql_log & analytics
                                Sql_log sql_log = extractLog(logLineBean);
                                //ps_SqlLog.write(sql_log);
                                raw_sql = (String) JSON_Obj.get("Results");

                                AnalyticsGen analyticsGen = new AnalyticsGen(JSON_Obj, sql_log, logLineBean);
                                ArrayList<Analytics> list_analytics = analyticsGen.generate();
                                for (Analytics analytics : list_analytics) {
                                    ps_Analytics.write(analytics);
                                }
                                //TODO : <Sankar> Implement all the pausible ACTION and throw the Assertion for default
                            //throw new AssertionError();
                        }

                    } else {
                        //logtUnparsableLine(_sourceFile, lineNumber, logLineBean.getLog_msg(),"LogLevel = " + logLineBean.getLog_level());
                    }

                } catch (ParseException e) {
                    //System.out.println("LogLine : " + logLine);
                    //logUnparsableLine(_sourceFile, lineNumber, logLine);
                } catch (ArrayIndexOutOfBoundsException e) {
                    //System.out.println("LogLine : " + logLine);
                    //logtUnparsableLine(_sourceFile, lineNumber, logLine, "ArrayIndexOutOfBoundsException : Invalid number of columns in the Line");
                } catch (net.sf.jsqlparser.parser.ParseException e) {
                    //logUnparsedSQLs(_sourceFile, lineNumber, raw_sql);
                } catch (Throwable e) {
                   // System.out.println("WEIRD_EXCEPTION | SQL: " + raw_sql);
//                    e.printStackTrace();
                   // logUnparsedSQLs(_sourceFile, lineNumber, raw_sql); //TODO: <Sankar> Remove this catch and leave the below catch Throwable after most of the issues are fixed
                } finally {
                    parser.reset();
                }
                
            }
        } catch (Throwable e) {
            System.out.println("Unknown Exception/Error | LogLine : " + logLine);
            throw e;
        } finally {
            if (br != null) {
                br.close();
            }
        }
        System.out.println(Integer.toString(this._userID)+ "  "+ _sourceFile+ " completed.");
    }

    private Sql_log extractLog(LogLineBean _logLineBean) throws Exception {

        int app_id = -1; //If no app_id found this will be -1
        String key = _logLineBean.get_app_key();

        String appName = "";
        if ((appName = this.appNameLookUpMap.get(key)) != null) {
            // I have an app name
            synchronized(this){
            app_id = appsMap.get(appName).getApp_id();
            }
        }

        Sql_log sql_log = new Sql_log(_userID, app_id, _logLineBean.getLog_msg());
        return sql_log;
    }

    /**
     * Extracts the user info from the filePath provided and adds to
     * {@link LogParser#usersMap usersMap}<br>
     * User info is present at {@link LogParser#sourceDir sourceDir}+File.separator+"logcat"+File.separator+<b>[user_guid]</b>+File.separator+
     * ...<p>
     * Example: <br>
     * _filePath =
     * /.../logcat/<b>0ee9cead2e2a3a58a316dc27571476e8973ff944</b>/tag/SQLite-Query-PhoneLab/2015/03/22.out.gz
     * <br>
     *
     * @param _filePath absolute path of the log file being processed
     */
    private void logtUnparsableLine(String _file_location, int _file_line_number,
            String _raw_data, String _exception_trace) throws Exception {

        Unparsed_log_lines unparsedLogLine = new Unparsed_log_lines(
                _file_location, _file_line_number, _raw_data, _exception_trace);

        ps_UnparsedLogLines.write(unparsedLogLine);
    }

    private void logUnparsableLine(String _file_location, int _file_line_number,
            String _raw_data) throws Exception {
        logtUnparsableLine(_file_location, _file_line_number, _raw_data, " ");
    }

    private void logUnparsedSQLs(String _file_location, int _file_line_number,
            String _raw_sql) throws Exception {
        ps_UnParsedSQLs.write(new UnParsedSQLs(_file_location, _file_line_number, _raw_sql));
    }

    /**
     * This method will persist the cached data : <br>
     * usersMap, unparsed_log_lines_list, App, User_app_process,
     */
    private void persistAppData() throws Exception {
        
          //Sorting is good here to insert in the ordered manner. Not Mandatory

        PersistanceService ps = new PersistanceFileService(AppConstants.DEST_FOLDER,
                AppConstants.OUTPUT_FILE_VALUE_SEPERATOR, App.class, _userID);
        ArrayList<App> sortedApps = getSortedApps();
        for (App app : sortedApps) {
            ps.write(app);
        }
        ps.close();
    }

    
    
    private ArrayList<App> getSortedApps()
    {
        ArrayList<App> sortedApps = null;
        synchronized(this){
        sortedApps = new ArrayList<>(appsMap.size());
        sortedApps.addAll(appsMap.values());
        }
        Collections.sort(sortedApps); 
        return sortedApps;
        
    }

    public void shutDown() throws Exception {
        ps_UnparsedLogLines.close();
        ps_SqlLog.close();
        ps_Analytics.close();
        ps_UnParsedSQLs.close();
        System.out.println("PRAGMA COUNT = " + AnalyticsGen.PRAGMA_COUNT);
    }

    @Override
    public void run() {
        try {
            parseLogsAndWriteFile();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    /*
     * ##############################################################################
     *   All the below are back-up methods to are TO-BE deleted when decided unfit
     * ##############################################################################
     */
    /**
     * @deprecated @param _sourceFile
     * @throws Exception
     */
    private void findLongestLine(String _sourceFile) throws Exception {
        String logLine = null;

        GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(_sourceFile));
        BufferedReader br = new BufferedReader(new InputStreamReader(gzip));

        while ((logLine = br.readLine()) != null) {
            int currLineLength = logLine.length();
            if (currLineLength > longestLineLength) {
                longestLineLength = currLineLength;
                longestLine = logLine;
            }
        }
        br.close();
    }

}
