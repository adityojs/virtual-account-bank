import java.util.ArrayList;

public class query
{
    private String query,queryConfig,spName;
    private SPParameter[] params;


    public query(String spConfig)throws Exception
    {
        this.queryConfig=spConfig;


        //ambil data parameter
        ArrayList<String> listParameter=new ArrayList<String>();

        int index=-1;
        int pointer=0;
        while((index=spConfig.indexOf(':',pointer))!=-1)
        {
            listParameter.add(spConfig.substring(index+1, spConfig.indexOf('\'', index)));
            pointer=index+1;
        }



       
        spName=spConfig.substring(0, spConfig.indexOf("("));

        StringBuilder querySb=new StringBuilder();
        querySb.append("{").append(spName).append('(');
        for(int i=0;i<listParameter.size();i++)
        {
            querySb.append("?");
            if(i<listParameter.size()-1)querySb.append(",");
        }
        querySb.append(")}");

        query=querySb.toString();



        //array param
        params=new SPParameter[listParameter.size()];

        for(int i=0;i<params.length;i++)
        {
            String dataTemp[]=listParameter.get(i).split("_", 2);
            params[i]=new SPParameter(Integer.parseInt(dataTemp[0]), Integer.parseInt(dataTemp[1]));
        }

        listParameter=null;
    }


    public SPParameter[] getParams(){return params;}
    public String getQuery(){return query;}
    public String getQueryConfig(){return queryConfig;}
    public String getSpName(){return spName;}
}
