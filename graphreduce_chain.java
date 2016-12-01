import java.io.*;  
import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.io.File;
import java.net.*;  
import java.io.FileNotFoundException;
import java.util.Scanner;
import util.AdjacencyListGraph;
import util.Compressed;
import util.Graph;
import util.PriorityQueue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class graphreduce {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ArrayList<String> mylist= new ArrayList<String>();
        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
               String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    mylist.add(tokenizer.nextToken());
                    if(mylist.size()==2){
                        System.out.println(mylist);
                        word.set(mylist.toString());
                        context.write(word,one);
                        mylist = new ArrayList<String>();
                    }
                }
                
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public static HashMap<String,Compressed> compressed_path_map;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
            try{ 
               // System.setProperty("http.agent", "Chrome");
                URL url = new URL("http://textuploader.com/d5zr7/raw");
                 URLConnection yc = url.openConnection();
                  yc.setRequestProperty("User-Agent", "Mozilla/5.0");
                  Scanner in = new Scanner(new InputStreamReader(
                                    yc.getInputStream()));

                //Scanner in = new Scanner(url.openStream());
               

                // read the first line with the dimensions of the grid
                int width = in.nextInt();
                int height = in.nextInt();
                int n = in.nextInt();

                // THIS WILL MAKE A ARRAY (okay, a list of lists since Java won't allow
                // arrays of generics) OF GRAPHS FOR THE INDIVIDUAL CELLS --
                // g.get(r).get(c) IS THE GRAPH FOR THE CELL IN ROW r COLUMN c

                // make an empty graph for each cell
                Set<String> g_verts = new HashSet<String>();
                List<List<Graph<String>>> g = new ArrayList<List<Graph<String>>>();
                for (int r = 0; r < height; r++)
                {
                    List<Graph<String>> row = new ArrayList<Graph<String>>();
                    for (int c = 0; c < width; c++)
                        {
                        // make the list of vertices in this cell starting
                        // with the corners...
                        List<String> verts = new ArrayList<String>();
                        verts.add("g" + r + "." + c); // upper left
                        verts.add("g" + (r + 1) + "." + c); // lower left
                        verts.add("g" + r + "." + (c + 1)); // upper right
                        verts.add("g" + (r + 1) + "." + (c + 1)); // lower right

                        //////////////////////////// getting all the G's, will be needed for corner graph intitlization.
                        g_verts.add("g" + r + "." + c); // upper left
                        g_verts.add("g" + (r + 1) + "." + c); // lower left
                        g_verts.add("g" + r + "." + (c + 1)); // upper right
                        g_verts.add("g" + (r + 1) + "." + (c + 1)); // lower right
                        ///////////////////////////
                        //...then the interior vertices
                        for (int k = 0; k < n; k++)
                            {
                            verts.add("v" + r + "." + c + "." + k);
                            }

                        // add that graph!
                        row.add(new AdjacencyListGraph<String>(verts));
                        }
                    g.add(row);
                }
                String from;
                while (!(from = in.next()).equals("queries"))
                {
                    String to = in.next();
                    int w = in.nextInt();
                    
                    // the to vertex is always in the interior of the cell
                    assert to.charAt(0) == 'v';

                    // figure out from the to vertex which cell we're in
                    StringTokenizer tok = new StringTokenizer(to.substring(1), ".");    
                    int r = Integer.parseInt(tok.nextToken());
                    int c = Integer.parseInt(tok.nextToken());
                    
                    // add the edge to the correct cell
                    g.get(r).get(c).addEdge(from, to, w);
                }
                // MAKE YOUR CORNER GRAPH HERE (Ability to label edges with paths
                // they represent given to us via the hashmap of edge to compressed)
                Graph corner_graph= makeCornerGraph(g,height,width,g_verts);

                ////////////////////////////////////////////////////////////////
                // determine what cells we're in
                System.out.println(key);
                String keyAsString =key.toString();
                String[] temp = keyAsString.replace('[',' ').replace(']',' ').trim().split(","); 
                from = (String)temp[0].trim();
                String to = (String)temp[1].trim();
                StringTokenizer tok = new StringTokenizer(from.substring(1), ".");
                int fromR = Integer.parseInt(tok.nextToken());
                int fromC = Integer.parseInt(tok.nextToken());

                tok = new StringTokenizer(to.substring(1), ".");
                int toR = Integer.parseInt(tok.nextToken());
                int toC = Integer.parseInt(tok.nextToken());
                
                String[] fromCorners = {"g" + fromR + "." + fromC,
                            "g" + (fromR + 1) + "." + fromC,
                            "g" + fromR + "." + (fromC + 1),
                            "g" + (fromR + 1) + "." + (fromC + 1)};
                String[] toCorners = {"g" + toR + "." + toC,
                              "g" + (toR + 1) + "." + toC,
                              "g" + toR + "." + (toC + 1),
                              "g" + (toR + 1) + "." + (toC + 1)};

                ///////////////figure out shortest path t corners for the from and to vertices
                Graph from_grid = g.get(fromR).get(fromC);
                
                //add from and to vertices;
                corner_graph.addVertex(from);
                corner_graph.addVertex(to);
                for(int i =0 ; i<fromCorners.length ; i++)
                {
                    Compressed shortest_path = Dijkstra(from_grid,from,fromCorners[i]);
                    compressed_path_map.put(from+fromCorners[i],shortest_path);
                    corner_graph.addEdge(from,fromCorners[i] ,shortest_path.weight);
                }

                Graph to_grid = g.get(toR).get(toC);
                for(int i =0 ; i<toCorners.length ; i++)
                {
                    Compressed shortest_path = Dijkstra(to_grid,to,toCorners[i]);
                    compressed_path_map.put(to+toCorners[i],shortest_path);
                    corner_graph.addEdge(to,toCorners[i],shortest_path.weight);
                }
                

                // RUN DIJKSTRA'S ON THE CORNER GRAPH

                Compressed final_shortest = Dijkstra(corner_graph,from,to);
                System.out.print(final_shortest.weight);
                ArrayList final_path  = final_shortest.path;

                // RECONSTRUCT COMPLETE PATH FROM THE PATH OF CORNERS

                printShortestPath(final_path,context,final_shortest.weight);
                ///remove vertices from corner graph
                corner_graph.removeVertex(from);
                corner_graph.removeVertex(to);
            }
            catch(IOException ex) {
                 System.out.println("Ughhhhh");
                  ex.printStackTrace();
            }    
        }

        //Print out the shortest path, unroling out the g vertices
        public static void printShortestPath(ArrayList path,Context context,int weight)
         throws IOException, InterruptedException{
            ArrayList shortest_path = new ArrayList();
            for(int j = 0; j< path.size()-1 ; j++)
            {
                String first_key = (String)path.get(j);
                String second_key = (String)path.get(j+1);
                ArrayList final_compressed_list;
                Compressed final_compressed =  compressed_path_map.get(first_key+second_key);
                if(final_compressed == null)
                {
                    final_compressed = compressed_path_map.get(second_key+first_key);
                    Collections.reverse(final_compressed.path);
                }
                final_compressed_list = final_compressed.path;
                for(int k = 0; k< final_compressed_list.size() ;k++)
                {
                    if(shortest_path.size()>0)
                    {
                        if((k==0))
                        {
                            if(shortest_path.get(shortest_path.size()-1).equals(final_compressed_list.get(k)))
                            {
                                //ignore that value cos it repeats;
                            }
                            if(!(shortest_path.get(shortest_path.size()-1).equals(final_compressed_list.get(k))))
                            {
                                //correct ordering of the values;
                                Collections.reverse(final_compressed_list);
                            }
                        }
                        else
                        {
                            shortest_path.add(final_compressed_list.get(k));
                        }
                    }
                    else{
                        shortest_path.add(final_compressed_list.get(k));
                    }
                }
            }
            Text word = new Text(" "+shortest_path.toString());
            IntWritable one = new IntWritable(weight);
            context.write(word,one);
            System.out.println(" "+shortest_path.toString());

        }




        public static Graph makeCornerGraph(List<List<Graph<String>>> g, int height, int width, Set<String> g_verts)
        {
            //compressed_path_map = new HashMap<String, HashMap>();
            Compressed path_copy;
            compressed_path_map = new HashMap<String, Compressed>();
            HashMap<String, Compressed> dest_compression;
            Graph corner_graph = new AdjacencyListGraph<String>(g_verts); //new graph
            for(int i=0;i<g.size();i++)
            {
                for(int j=0; j<g.get(i).size();j++)
                {
                    String from = "g"+i+"."+j;
                    
                    //1st edge(1st horizontal,can repeat)
                    
                    String to = "g"+(i+1) +"."+j;
                    Compressed path = Dijkstra(g.get(i).get(j),from, to);
                    if(corner_graph.hasEdge(from, to))
                    {
                        int prev_weight = corner_graph.weight(from, to);
                        if(prev_weight>path.weight)
                        {
                            
                            corner_graph.changeWeight(from, to,path.weight);
                            compressed_path_map.put(from+to,path);
                            path_copy = path;
                            Collections.reverse(path_copy.path);
                            compressed_path_map.put(to+from,path_copy);
                        }
                        else{
                            //System.out.println("Previous weight, " + prev_weight +" was smaller than "+ path.weight);
                        }
                    }
                    else
                    {
                        
                        corner_graph.addEdge(from,to,path.weight); //add to new corner graph
                        compressed_path_map.put(from+to,path);
                        path_copy = path;
                        Collections.reverse(path_copy.path);
                        compressed_path_map.put(to+from,path_copy);
                    }
                    
                    //2nd edge(main diagonal, cant be repeated)
                    
                    to = "g"+(i+1) +"."+(j+1);
                    Compressed path2 = Dijkstra(g.get(i).get(j),"g"+i+"."+j, to);
                    compressed_path_map.put(from+to,path2);
                    corner_graph.addEdge(from,to,path2.weight);
                    path_copy = path2;
                    Collections.reverse(path_copy.path);
                    compressed_path_map.put(to+from,path_copy);
                    
                    //third edge(right , vertical can repeat)
                    
                    to =  "g"+ i +"."+(j+1);
                    Compressed path3 = Dijkstra(g.get(i).get(j),from,to);
                    if(corner_graph.hasEdge(from, to))
                    {
                        int prev_weight = corner_graph.weight(from,to);
                        if(prev_weight>path3.weight)
                        {
                            corner_graph.changeWeight(from, to,path3.weight);
                            compressed_path_map.put(from+to,path3);
                            corner_graph.addEdge(from,to,path3.weight);
                            path_copy = path3;
                            Collections.reverse(path_copy.path);
                            compressed_path_map.put(to+from,path_copy);
                        }
                        else{
                            //System.out.println("Previous weight, " + prev_weight +" was smaller than "+ path3.weight);

                        }
                    }
                    else{
                        compressed_path_map.put(from+to,path3);
                        corner_graph.addEdge(from,to,path3.weight);
                        path_copy = path3;
                        Collections.reverse(path_copy.path);
                        compressed_path_map.put(to+from,path_copy);

                    }

                    //4th edge 2nd diagonal(cant be repeated)
                    
                    from = "g"+i+"."+(j+1);
                    to = "g"+ (i+1)+"."+j;
                    Compressed path4 = Dijkstra(g.get(i).get(j),from, to);
                    compressed_path_map.put(from+to,path4);
                    corner_graph.addEdge(from,to,path4.weight);
                    path_copy = path4;
                    Collections.reverse(path_copy.path);
                    compressed_path_map.put(to+from,path_copy);

                    //5th edge(2nd horizontal,can be repeated)

                    from = "g"+i+"."+(j+1);
                    to = "g"+ (i+1)+"."+(j+1);
                    Compressed path5 = Dijkstra(g.get(i).get(j),from, to);
                    if(corner_graph.hasEdge(from, to))
                    {
                        
                        int prev_weight = corner_graph.weight(from,to);
                        if(prev_weight>path5.weight)
                        {
                            corner_graph.changeWeight(from, to,path5.weight);
                            compressed_path_map.put(from+to,path5);
                            corner_graph.addEdge(from,to,path5.weight);
                            path_copy = path5;
                            Collections.reverse(path_copy.path);
                            compressed_path_map.put(to+from,path_copy);
                        }
                        else{
                            //System.out.println("Previous weight, " + prev_weight +" was smaller than "+ path5.weight);

                        }
                    }
                    else
                    {
                        compressed_path_map.put(from+to,path5);
                        corner_graph.addEdge(from,to,path5.weight);
                        path_copy = path5;
                        Collections.reverse(path_copy.path);
                        compressed_path_map.put(to+from,path_copy);
                    }

                    //6th edge(can be repeated, 2nd vertical)
                    
                    from = "g"+(i+1)+"."+(j);
                    to = "g"+ (i+1)+"."+(j+1);
                    Compressed path6 = Dijkstra(g.get(i).get(j),from, to);
                    if(corner_graph.hasEdge(from, to))
                    {
                        int prev_weight = corner_graph.weight(from,to);
                        if(prev_weight>path6.weight)
                        {
                            
                            corner_graph.changeWeight(from, to,path6.weight);
                            compressed_path_map.put(from+to,path6);
                            corner_graph.addEdge(from,to,path6.weight);
                            path_copy = path6;
                            Collections.reverse(path_copy.path);
                            compressed_path_map.put(to+from,path_copy);
                        }
                        else{
                            //System.out.println("Previous weight, " + prev_weight +" was smaller than "+ path6.weight);
                        }
                    }
                    else
                    {
                        corner_graph.addEdge(from,to,path6.weight);
                        compressed_path_map.put(from+to,path6);
                        corner_graph.addEdge(from,to,path6.weight);
                        path_copy = path6;
                        Collections.reverse(path_copy.path);
                        compressed_path_map.put(to+from,path_copy);
                    }
                }
            }
            return corner_graph;
        }

        public static Compressed Dijkstra(Graph g,String source, String destination)
        {
            Compressed compressed=null;
            Set<String> black_vertice = new HashSet<String>();
            PriorityQueue queue = new PriorityQueue();
            int shortest=0;
            Set<String> verts = g.getVerts();
            Iterator iter = verts.iterator();
            //initialize every vertex's priority to infinity. Except source's.
            while (iter.hasNext()) {
                String v = ((String)iter.next());
                if (!v.equals(source))
                {
                    queue.addItem(v, Integer.MAX_VALUE); 
                }
                else
                {
                    queue.addItem(v, 0);//Source is first in queue
                }
            }
            //map vertives to prev vertices in their shortest paths
            HashMap<String, String> prevHashMap= new HashMap<String, String>();
            while(queue.getSize()!=0)
            {
                int lowest = (int)queue.peekPriority(); //gives us the source distance;
                String check = (String)queue.peekTop(); //returns source but keeps it on queue
                if(check.equals(destination))
                    //means we have gotten to our destination
                {
                    shortest = (int)queue.getPriority(check);
                    ArrayList<String> prevList = new ArrayList<String>();
                    while(prevHashMap.containsKey(destination)){
                        prevList.add(destination);
                        destination = prevHashMap.get(destination);
                    }
                    ArrayList<String> path = new ArrayList<String>();
                    //because source has no previous it wont be in the map and we have to add it ourselves
                    path.add(destination); 
                    Collections.reverse(prevList);
                    for(int i=0; i<prevList.size();i++){
                        path.add(prevList.get(i));
                    }
                    compressed = new Compressed(path,shortest);
                    break;
                    
                }
                else
                    //Keep going
                {
                    HashMap<String, Integer> neighbours = g.vertsAdjTo(check);
                    Set<String> keys = neighbours.keySet();
                    Iterator iterate = keys.iterator();
                    while (iterate.hasNext()){
                        String temp = ((String)iterate.next());
                        //only iterate use neighbours that havent been visited and completed(Non-black vertices)
                        if(!black_vertice.contains(temp))
                        {
                            int alternate = g.weight(check,temp) +lowest;
                            if(alternate<(int)queue.getPriority(temp))
                            {
                                queue.changePriority(temp,alternate);
                                prevHashMap.put(temp,check);
                            }
                        }
                    }
                }
                black_vertice.add((String)queue.removeItem());  
            }
            return compressed;
        }
    }
    public static class Map2 extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final  IntWritable count = new IntWritable(1); //dummy intwritable
        private Text word = new Text();
        private ArrayList<String> mylist= new ArrayList<String>();
        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
                String line = value.toString();
                System.out.println(line);
                line = line.trim();
                System.out.println("The current weight examined is: "+ line.charAt(line.length()-1));
                word.set(Character.toString(line.charAt(line.length()-1)));
                context.write(word,count);
        }
    }
    public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {

            public void reduce(Text key, Iterable<IntWritable> values, Context context) 
              throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                context.write(key, new IntWritable(sum));
            }
         }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "graphreduce");
        job.setJar("graphreduce.jar");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(new Path(args[1]));

        job.waitForCompletion(true);
        //calculate # of times each weight is found
        while (!job.isComplete())
        {
            System.out.println("Waiting for job");
        }
        if(job.isSuccessful()){
            Job job2 = new Job(conf, "graphreduce_chained");
            job2.setJar("graphreduce.jar");

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            String second_input_path = args[1]+"/part-r-00000";
            FileInputFormat.addInputPath(job2, new Path(second_input_path));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            job2.waitForCompletion(true);
            }
            else{    
            }
    }
}
