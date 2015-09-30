/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ub.tbd;

import edu.ub.tbd.constants.AppConstants;
import edu.ub.tbd.parser.LogParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

/**
 * This is the driver class of the Phone Data Parser when invoked using jar or command line
 * 
 * @author sathish
 */
public class Main {

    /**
     * @param args the command line arguments [-v|V] [-help] [-p "mode"]
     */
    public static void main(String[] args) {
        LogParser logParser = null;
        try {
            if(startUp()){
                logParser = new LogParser(AppConstants.SRC_DIR, AppConstants.SRC_LOG_FILE_EXT);
                logParser.parseLogsAndWriteFile();
                
                try {
                    shutDown(logParser);
                } catch (Exception e) {
                    System.out.println("Improper shutdown of the application");
                    e.printStackTrace();
                }
            } else {
                System.out.println("Start up of the application failed");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            if(logParser != null){
                try {
                    logParser.shutDown();
                } catch (Exception ex) {
                    System.out.println("Unable to shutDown LogParser");
                    ex.printStackTrace();
                }
            }
            
        }
    }
    
    /**
     * Makes sure the required folder setup for the source and destination file exist. If destination file absent, this creates it
     * 
     * @return true  - if start up is successful <br> 
     *         false - if start up fails. Application should terminate if false
     * 
     * @throws java.lang.Exception 
     */
    public static boolean startUp() throws Exception{
        boolean out = false;
        File destinationFolder = new File(AppConstants.DEST_FOLDER);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            if(destinationFolder.exists()){
                String [] filesInOUTFolder = destinationFolder.list(
                        (File _dir, String _fileName) -> _fileName.endsWith(AppConstants.OUTPUT_FILE_EXT));
                if(filesInOUTFolder != null && filesInOUTFolder.length > 0){
                    System.out.println("Destination folder \"" + AppConstants.DEST_FOLDER + "\" already has prior run files. \nWould you like to append the new run? \nY --> Continue\nN --> Abort");
                    String input = null;
                    while((input = br.readLine()) != null){
                        if("Y".equalsIgnoreCase(input)){
                            out = true;
                            break;
                        } else if ("N".equalsIgnoreCase(input)){
                            out = false;
                            break;
                        } else {
                            System.out.println("What did you say??? Enter (Y/N)");
                        }
                    }
                } else {
                    out = true;
                }
            } else {
                destinationFolder.mkdirs();
                out = true;
            }
        } catch (Exception e) {
            throw e;
        } finally {
            br.close();
        }
        return out;
    }
    
    public static void shutDown(LogParser _logParser) throws Exception{
        _logParser.shutDown();
    }
    
    /*
     * ##############################################################################
     *   All the below are back-up methods to are TO-BE deleted when decided unfit
     * ##############################################################################
     */
    
    /**
     * @deprecated 
     * @param files list of files
     */
    public static void showFiles(File[] files) {
        for (File file : files) {
            if (file.isDirectory()) {
                System.out.println("Directory: " + file.getName());
                showFiles(file.listFiles()); // Calls same method again.
            } else {
                System.out.println("File: " + file.getName());
            }
        }
    }
    
}