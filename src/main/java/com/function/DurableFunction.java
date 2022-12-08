package com.function;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.OrchestrationRunner;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;
import java.util.*;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import com.azure.messaging.servicebus.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


public class DurableFunction
{
    class SymbolHelper
    {
        public String blobName;
        public String symbol;
        public String value;
    }
    class SymbolDeltaHelper
    {
        public String symbol;
        public String delta;
        public String curValue;
        public String origOrder;
        public String procTimeStamp;
    }

    @FunctionName("StartOrchestration")
    @StorageAccount("demolcsappdevsymbolsa1_STORAGE")
    public void run(
        @BlobTrigger(name = "content", path = "symboleventsin/{name}", dataType = "binary") byte[] content,
        @BindingName("name") String name,
        @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
        final ExecutionContext context)     
    {
        context.getLogger().info("blob triggered durable function started.");
        
        DurableTaskClient client = durableContext.getClient();

        String contentStr = new String(content);

        String strArr[] = new String[2];
        strArr[0] = name;
        strArr[1] = contentStr;
        String instanceId = 
            client.scheduleNewOrchestrationInstance("ProcessSymbols", strArr);       
    }

    @FunctionName("ProcessSymbols")
    public String processSymbolsOrchestrator(
            @DurableOrchestrationTrigger(name = "runtimeState") String runtimeState,
            final ExecutionContext context) {
        return OrchestrationRunner.loadAndRun(runtimeState, ctx -> {

            //Map<String, String> data = ctx.getInput(Map.class);
            String data[] = ctx.getInput(String[].class);
            String name = data[0];

            Map<String, List<LinkedTreeMap<String, String>>> symbolHelperMap = new Gson().fromJson(data[1], Map.class);
            List<LinkedTreeMap<String, String>> helperList = symbolHelperMap.get("symbols");
            for (LinkedTreeMap<String, String> sMap : helperList)
            {                
                SymbolHelper sHelper = new SymbolHelper();
                sHelper.blobName = name;
                sHelper.symbol = sMap.get("symbol");
                sHelper.value = sMap.get("value");
                
                context.getLogger().info("Found symbol: " + sMap.get("symbol") + ":" + sMap.get("value"));

                ctx.callActivity("ProcessSymbol", sHelper).await();
            }

        // delete blob
        String bPayload   = "{ \"blobname\": \"" +  name + "\"}";
        String urlString = System.getenv("DELETEBLOB_URL"); 
        _sendHTTPPostReq(bPayload, urlString);

            return "";
        });
    }

    @FunctionName("ProcessSymbol")
    public void processSymbolAction(
        @DurableActivityTrigger(name = "input") SymbolHelper aSHelper,
        final ExecutionContext context) 
    {                
        context.getLogger().info("Processing symbol: " + aSHelper.symbol + ":" + aSHelper.value);

        SymbolDeltaHelper deltaHelper = new SymbolDeltaHelper();
        deltaHelper.symbol        = aSHelper.symbol;
        deltaHelper.curValue      = aSHelper.value;
        deltaHelper.origOrder     = aSHelper.blobName;
        deltaHelper.procTimeStamp = String.valueOf(System.currentTimeMillis());        

        // persist to redis
        _persistToRedis(context, deltaHelper);

        // put payload onto service bus to be cached in cosmos
        String connectionString = System.getenv("SB_CON_STR");
        String queueName        = System.getenv("SB_QNAME");        

        String sbPayload = new Gson().toJson(deltaHelper);
        context.getLogger().info("SB Payload:\n" + sbPayload);
        _sendMessageToBus(connectionString, queueName, sbPayload);

    }

    private void _persistToRedis(ExecutionContext aContext, SymbolDeltaHelper aSHelper)
    {
        boolean useSsl = true;
        String cacheHostname = System.getenv("REDISCACHEHOSTNAME");
        String cachekey      = System.getenv("REDISCACHEKEY");

        // Connect to the Azure Cache for Redis over the TLS/SSL port using the key.
        Jedis jedis = new Jedis(cacheHostname, 6380, DefaultJedisClientConfig.builder()
            .password(cachekey)
            .ssl(useSsl)
            .build()
        );
        
        aContext.getLogger().info("Redis ping: " + jedis.ping());

        if (!"".equals(aSHelper.curValue) && "0".equals(aSHelper.curValue))
        {
            jedis.flushDB();
            jedis.close();

            aSHelper.delta = "0";

            return;
        }

        String cacheStr = jedis.get(aSHelper.symbol);
        if (cacheStr != null && !"".equals(cacheStr))
        {
            aContext.getLogger().info("Cached hit: " + aSHelper.symbol + " -> " + cacheStr);

            Integer cacheVal = (Integer) Integer.parseInt(cacheStr);
            Integer newVal   = (Integer) Integer.parseInt(aSHelper.curValue);
            
            Integer deltaVal = newVal - cacheVal;
            aSHelper.delta = String.valueOf(deltaVal);
        }
        
        jedis.set(aSHelper.symbol, aSHelper.curValue);
        jedis.close();

        aContext.getLogger().info("Cached to Redis: " + aSHelper.symbol + " -> " + aSHelper.curValue);
    }

    private void _sendMessageToBus(String aConnectionString, String aQueueName, String aPayload)
    {
        // create a Service Bus Sender client for the queue 
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
            .connectionString(aConnectionString)
            .sender()
            .queueName(aQueueName)
            .buildClient();

        // send one message to the queue
        senderClient.sendMessage(new ServiceBusMessage(aPayload));
        
        System.out.println("Sent a single message to the queue: " + aQueueName);
    }

    private void _sendHTTPPostReq(String aPayload, String aUrlString)
    {
        try 
        {
            URL url = new URL(aUrlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    
            //Request headers
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Cache-Control", "no-cache");
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Ocp-Apim-Subscription-Key", System.getenv("APIM_API_KEY"));            
    
            // Request body
            connection.setDoOutput(true);
            connection.getOutputStream().write(aPayload.getBytes());
        
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) 
            {
                content.append(inputLine);
            }
            
            in.close();
            System.out.println("server response: " + content);
    
            connection.disconnect();
        } 
        catch (Exception ex) 
        {
            System.out.print("exception:" + ex.getMessage());
        }
    }    
}