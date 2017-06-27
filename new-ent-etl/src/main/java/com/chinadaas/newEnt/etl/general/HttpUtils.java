package com.chinadaas.newEnt.etl.general;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;

public class HttpUtils {

    private static CloseableHttpClient httpClient = HttpClients.createDefault();

    public static String get(String url) {
        HttpGet httpGet = new HttpGet(url);
        try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
            HttpEntity entity = httpResponse.getEntity();
            InputStream inputStream = entity.getContent();
            String result = IOUtils.toString(inputStream, "UTF-8");
            inputStream.close();
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
