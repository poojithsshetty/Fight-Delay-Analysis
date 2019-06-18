package Join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    String joinType=null;
     Text emptyText = new Text("");
     Text temp = new Text();
     ArrayList<Text> r = new ArrayList<Text>();
     ArrayList<Text> m = new ArrayList<Text>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType= context.getConfiguration().get("type");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        r.clear();
        m.clear();

        for(Text val: values){
            temp=val;
            if(val.toString().charAt(0)=='R') r.add(new Text(temp.toString().substring(1)));
            else if(val.toString().charAt(0)=='M') m.add(new Text(temp.toString().substring(1)));
        }
        executeJoin(context);
    }

    public void executeJoin(Context context) throws IOException, InterruptedException {

        if(joinType.equalsIgnoreCase("inner")){
            if(!r.isEmpty() && !m.isEmpty()){
                for(Text ra: r){
                    for(Text mo: m){
                        context.write(ra,mo);
                    }
                }
            }
        }

        else if(joinType.equalsIgnoreCase("leftouter")){
            for(Text ra: r){
                if(!m.isEmpty()){
                    for(Text mo : m){
                        context.write(ra,mo);
                    }
                }
                else context.write(ra,emptyText);
            }
        }

        else if(joinType.equalsIgnoreCase("rightouter")){
            for(Text mo: m){
                if(!r.isEmpty()){
                    for(Text ra: r){
                        context.write(ra,mo);
                    }
                }
                else context.write(emptyText,mo);
            }
        }

        else if(joinType.equalsIgnoreCase("fullouter")){
            if(!r.isEmpty()){
                for(Text ra:r){
                    if(!m.isEmpty()){
                        for(Text mo: m){
                            context.write(ra,mo);
                        }
                    }else context.write(ra,emptyText);
                }
            }
            else {
                for(Text mo: m){
                    context.write(emptyText,mo);
                }
            }
        }

        else if(joinType.equalsIgnoreCase("antijoin")){
            if(r.isEmpty()^m.isEmpty()){
                for(Text ra: r){
                    context.write(ra,emptyText);
                }
                for(Text mo: m){
                    context.write(emptyText,mo);
                }
            }
        }
    }
}
