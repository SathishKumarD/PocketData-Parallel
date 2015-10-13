/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ub.tbd.parser;

import edu.ub.tbd.constants.AppConstants;
import edu.ub.tbd.entity.App;
import edu.ub.tbd.entity.User;
import edu.ub.tbd.service.PersistanceFileService;
import edu.ub.tbd.service.PersistanceService;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 *
 * @author sathish
 */
public class Parallelize {

    private Pattern regx_userGUIDInFilePath = null;
    private final HashMap<String, User> usersMap = new HashMap<>();
    private final String sourceDir;
    private final String fileExtension;
    private HashMap<String, App> appsMap = new HashMap<>();
    private ArrayList<App> consolidatedAppNames = null;

    public Parallelize(String _sourceDir, String _fileExtension) {
        this.sourceDir = _sourceDir;
        this.fileExtension = _fileExtension;
        consolidatedAppNames = new ArrayList<>();
    }

    public void RunParserParallely() {
        ExecutorService taskExecutor = Executors.newFixedThreadPool(11);
        ArrayList<User> sortedUsers = getSortedUsers();

        for (User u : sortedUsers) {
            LogParser lp = getLogParser(u);
            taskExecutor.execute(lp);
        }
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(1, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.err.println("");
        }
    }



    public void persistCacheData() throws Exception {
        System.out.println("Persisting usersMap....");
        persistUserData();

        System.out.println("Persisting appsMap....");
        persistAppData();

    }

    private void persistUserData() throws Exception {

        ArrayList<User> sortedUsers = getSortedUsers();  //Sorting is good here to insert in the ordered manner. Not Mandatory

        PersistanceService ps = new PersistanceFileService(AppConstants.DEST_FOLDER,
                AppConstants.OUTPUT_FILE_VALUE_SEPERATOR, User.class, 0);

        for (User user : sortedUsers) {
            ps.write(user);
        }
        ps.close();
    }

    private void persistAppData() throws Exception {

        Collections.sort(consolidatedAppNames);   //Sorting is good here to insert in the ordered manner. Not Mandatory

        PersistanceService ps = new PersistanceFileService(AppConstants.DEST_FOLDER,
                AppConstants.OUTPUT_FILE_VALUE_SEPERATOR, App.class, 0);

        for (App app : consolidatedAppNames) {
            ps.write(app);
        }
        ps.close();
    }

    public void shutDown() throws Exception {
        persistCacheData();

    }

    private ArrayList<User> getSortedUsers() {
        HashMap<String, User> usersMap = getUserGuids();
        ArrayList<User> sortedUsers = new ArrayList<>(usersMap.size());
        sortedUsers.addAll(usersMap.values());
        Collections.sort(sortedUsers);
        return sortedUsers;

    }

    private LogParser getLogParser(User u) {
        LogParser lp = null;
        try {
            lp = new LogParser(this.sourceDir  + File.separator+ u.getGuid(), u.user_id, this.fileExtension,appsMap);
        } catch (Exception ex) {
            if (lp != null) {
                try {
                    System.err.println("Exception in directory:" + this.sourceDir);
                    lp.shutDown();
                } catch (Exception e) {
                    System.err.println("Unable to shutDown LogParser");
                    ex.printStackTrace();
                }
            }
        }
        return lp;
    }

    private HashMap<String, User>  getUserGuids() {      
        File[] files = new File(this.sourceDir).listFiles();
        for (File file : files) {
            if (!file.isFile() ) {
                String user_guid = file.getName();
                if(!usersMap.containsKey(user_guid)){
                usersMap.put(user_guid, new User(user_guid));
            }
            }
        }
        return usersMap;
    }

}
