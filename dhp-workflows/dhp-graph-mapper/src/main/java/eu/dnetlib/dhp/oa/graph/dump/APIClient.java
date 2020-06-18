package eu.dnetlib.dhp.oa.graph.dump;

import java.io.*;
import java.net.MalformedURLException;

import com.cloudera.org.apache.http.HttpResponse;
import com.cloudera.org.apache.http.client.HttpClient;
import com.cloudera.org.apache.http.client.methods.HttpPost;
import com.cloudera.org.apache.http.entity.StringEntity;
import com.cloudera.org.apache.http.impl.client.DefaultHttpClient;
import com.cloudera.org.apache.http.util.EntityUtils;
import com.google.gson.Gson;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.ZenodoModel;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;


public class SendToZenodo implements Serializable {




    String urlString;


    public SendToZenodo(String urlString) throws MalformedURLException {


        this.urlString = urlString ;
    }

    public void connect() throws IOException {

        String json = "{}";

        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(urlString);

        StringEntity input = new StringEntity(json);
        post.setEntity(input);
        post.addHeader("Content-Type", "application/json");

        HttpResponse response = client.execute(post);
        System.out.println(response.getStatusLine());
        System.out.println(response.getEntity().getContent().toString());

        json = EntityUtils.toString(response.getEntity());

        ZenodoModel newSubmission = new Gson().fromJson(json, ZenodoModel.class);
        System.out.println(newSubmission.getLinks().getBucket());

    }


}
