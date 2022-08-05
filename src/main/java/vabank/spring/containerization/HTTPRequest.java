package vabank.spring;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;


public class HTTPRequest
{
    public String reqLine;
    public String body;
    public ArrayList<String> headers;
    public HashMap<String,String> reqParams;

    //response
    public String code;
    public String codeStr;
    public String content;

    public String rc;

    public HashMap<String,String> getReqParams()
    {
        if(reqParams!=null)return reqParams;

        reqParams=new HashMap<String, String>();

        String paramLine=null;
        if(reqLine.startsWith("GET"))
        {
            paramLine=reqLine.substring(reqLine.indexOf('?')+1,reqLine.lastIndexOf(' '));
        }
        else if(reqLine.startsWith("POST") && body!=null)paramLine=body;

        String paired[]=paramLine.split("&");
        if(paired.length>0)
        {
            for (int i = 0; i < paired.length; i++) {

                String keyValue[]=paired[i].split("=",2);
                if(keyValue.length==2)reqParams.put(keyValue[0], keyValue[1]);
            }
        }

        return reqParams;
    }


}
