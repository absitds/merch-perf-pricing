package com.albertsons.itds.pexec.sharepoint;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.http.IHttpRequest;

import javax.naming.ServiceUnavailableException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by exa00082 on 30/1/19.
 */
public class SharePointAuthenticationProvider implements IAuthenticationProvider{

    private final static String AUTHORITY = "https://login.microsoftonline.com/common/";
    private final static String CLIENT_ID = "df9c33f7-4b15-4347-9cc8-eb89c48240b3";

     String username;
     String password;

    public SharePointAuthenticationProvider(String username, String password){

        this.username=username;
        this.password=password;
    }

    @Override
    public void authenticateRequest(IHttpRequest iHttpRequest) {
        BufferedReader br = new BufferedReader(new InputStreamReader(
                System.in));

        // Request access token from AAD
        AuthenticationResult result = null;
        try {
            result = getAccessTokenFromUserCredentials(
                    username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Get user info from Microsoft Graph
        //getUserInfoFromGraph(result.getAccessToken());
        //System.out.print(result.getAccessToken());
        iHttpRequest.addHeader("Authorization", "Bearer " + result.getAccessToken());
        iHttpRequest.addHeader("Accept","application/json");
        iHttpRequest.addHeader("Content-Type", "application/json; charset=utf-8");
    }


    private static AuthenticationResult getAccessTokenFromUserCredentials(
            String username, String password) throws Exception {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(AUTHORITY, false, service);
            Future<AuthenticationResult> future = context.acquireToken(
                    "https://graph.microsoft.com", CLIENT_ID, username, password,
                    null);
            result = future.get();
        } finally {
            service.shutdown();
        }

        if (result == null) {
            throw new ServiceUnavailableException(
                    "authentication result was null");
        }
        return result;
    }
}
