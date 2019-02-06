package com.albertsons.itds.pexec.sharepoint;

import com.microsoft.graph.concurrency.ChunkedUploadProvider;
import com.microsoft.graph.concurrency.IProgressCallback;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.models.extensions.*;
import com.microsoft.graph.requests.extensions.GraphServiceClient;

import java.net.URLEncoder;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.StringTokenizer;

/**
 * Created by exa00082 on 30/1/19.
 */
public class SharePointUploader {

    public static final String targetSiteId="rxsafeway.sharepoint.com,bd59092b-75e8-4be4-a373-3facc3a9f3f9,79899ad9-f52c-45f3-aac2-71662b86bcf5";

    public static final String itemId="01HCIOGEXVAWVYPA5FAVHJ3N3NCCMFM3I6";

    private static final Logger LOGGER = Logger.getLogger(SharePointUploader.class.getName());

    public static SharePointAuthenticationProvider authenticationProvider;

    public static IGraphServiceClient graphClient;

    public static void setGraphClient(){

          graphClient = GraphServiceClient
                        .builder()
                        .authenticationProvider(authenticationProvider).buildClient();
    }
    public static void sharePointHandler(String folderName,File filename){

        if(filename.length()<=0)
        {
            LOGGER.log(Level.INFO,"File size should be grater than 0. Skipping file "+filename.getName());
            return;
        }


        InputStream fileInputStream=null;
        try {
            fileInputStream = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int size=0;
        try {
            size=fileInputStream.available();
        } catch (IOException e) {
            e.printStackTrace();
        }

        IProgressCallback<DriveItem> callback = new IProgressCallback<DriveItem>() {
            public void progress(long l, long l1) {


            }

            public void success(DriveItem driveItem) {

                LOGGER.log(Level.INFO,"Successfully Uploaded "+filename.getName() + " to "+ folderName);
            }

            public void failure(ClientException e) {

                throw e;
            }
        };

        UploadSession uploadSession = null;
        try {
            uploadSession = graphClient.sites(targetSiteId)
                    .drive().items(itemId).itemWithPath(URLEncoder.encode(folderName.trim()+"/"+filename.getName().trim(),"UTF-8").replaceAll("\\+", "%20")).createUploadSession(new DriveItemUploadableProperties())
                    .buildRequest().post();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        ChunkedUploadProvider<DriveItem> chunkedUploadProvider = new ChunkedUploadProvider<DriveItem>(
                uploadSession, graphClient, fileInputStream, size, DriveItem.class);

        try {
            chunkedUploadProvider.upload(callback);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static boolean isSourceFile(String sourcePath){

        File file = new File(sourcePath);

        return file.isFile();
    }

    public static void main(String[] args) {

        String username=args[0];
        String password=args[1];
        authenticationProvider= new SharePointAuthenticationProvider(username,password);
        setGraphClient();
        String batchID=args[2];
        String sourceFolder=args[3];
        String folderName="";
       // System.out.println(batchID);
       // System.out.println(sourceFolder);
        StringTokenizer stringTokenizer = new StringTokenizer(sourceFolder,"/");
        while (stringTokenizer.hasMoreTokens()){
            folderName=stringTokenizer.nextToken();
        }

        if(isSourceFile(sourceFolder))
            uploadToSharePointFile(sourceFolder,batchID);
        else
            uploadToFolderSharePoint(sourceFolder,batchID+"/"+folderName);
    }

    public static void uploadToSharePointFile(String filePath, String folderName){


        sharePointHandler(folderName,new File(filePath));
    }

    public static void uploadToFolderSharePoint(String sourceFolder, String folderName) {


        Collection<File> all = new ArrayList<>();
        addTree(new File(sourceFolder), all);
//        System.out.print("size"+all.size());
        for(File currentFile:all){

            sharePointHandler(folderName,currentFile);
//            System.out.println("FOlder "+folderName);
//            System.out.println("File "+currentFile);
        }
    }

    static void addTree(File file, Collection<File> all) {
        File[] children = file.listFiles();

        //System.out.println("child"+file);

        if (children != null) {
            for (File child : children) {
                if(child.isFile())
                all.add(child);
                addTree(child, all);
            }
        }
    }
}
