package com.nadozirny.camunda;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.camunda.bpm.engine.CaseService;
import org.camunda.bpm.engine.delegate.Expression;
import org.camunda.bpm.engine.delegate.DelegateTask;
import org.camunda.bpm.engine.delegate.TaskListener;
import org.camunda.bpm.engine.runtime.CaseExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;


public class CMMNHttpConnector implements TaskListener {
  
  private static final Logger log = LoggerFactory.getLogger(CMMNHttpConnector.class);

  private Expression url;
  private Expression method;
  private Expression payload;
  private Expression headers;
  private Expression status;
  private Expression response;

  @Override
  public void notify(DelegateTask delegateTask) {
    log.info("task listener {}, event {}", delegateTask.getTaskDefinitionKey(), delegateTask.getEventName());
    if (delegateTask.getEventName().equals(EVENTNAME_COMPLETE)) {
      log.info("case instance: {}, task: {}, execution: {}, parent: {}", 
          delegateTask.getCaseInstanceId(), 
          delegateTask.getName(), 
          delegateTask.getCaseExecutionId(), 
          delegateTask.getCaseInstanceId() );
		  String _url = (String)url.getValue(delegateTask);
		  String _method = ((String)method.getValue(delegateTask)).toLowerCase();

		  log.info("URL: {} ", _url );

		  CloseableHttpClient httpClient = HttpClients.createDefault();
     	  CloseableHttpResponse resp=null;
		  Map<String,String> _headers=new HashMap<String,String>();
		  if ((String)headers.getValue(delegateTask)!=""){
		    try{
		      TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {};
		      ObjectMapper mapper=new ObjectMapper();
		      JsonNode node=mapper.readTree((String)headers.getValue(delegateTask));
		      _headers=mapper.convertValue( node, typeRef );
        	 }catch(Exception e){
		    	  log.error("Illegal headers: {} ", headers.getValue(delegateTask) );
		    }
		 }
		 try{
		 if (_method.equals("post")){
			 HttpPost post = new HttpPost( _url );
			 _headers.forEach( (k,v) -> post.addHeader(k, v) );
	         post.setEntity(new StringEntity((String)payload.getValue(delegateTask),"UTF-8"));
	         try{
    	        httpClient = HttpClients.createDefault();
        	    resp = httpClient.execute(post);
             }finally{
            	httpClient.close();
	        }
		}else
		if (_method.equals("get")){
			 HttpGet post = new HttpGet( _url );
			 _headers.forEach( (k,v) -> post.addHeader(k, v) );
	         try{
    	        httpClient = HttpClients.createDefault();
        	    resp = httpClient.execute(post);
             }finally{
            	httpClient.close();
	        }

		}else{
		  log.error("Illegal method: {} ", method.getValue(delegateTask) );

		}

		 log.debug("Http response: " + resp.getStatusLine().getStatusCode() );
		if ((String)status.getValue(delegateTask) != ""){
				delegateTask.getExecution().setVariable((String)status.getValue(delegateTask), resp.getStatusLine().getStatusCode() );
		 }
		if ((String)response.getValue(delegateTask) != ""){
				delegateTask.getExecution().setVariable((String)response.getValue(delegateTask), resp.toString() );
		 }


		}catch(Exception e){
		
 		}

    }
  }
}