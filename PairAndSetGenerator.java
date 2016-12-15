 /** Author: Mohammed Ibrahim
About: Given two integers, m , n (m=length of prefix, n= length of suffix) and a list of vertices in a graph
		Outpue a list of start and end vertices and a set of vertices (of size m+n-2) to exclude when finding path

**/

/*  Todo: for every prefix/suffix possible, i.e(see if paths are actually existent and then count 
    how many work). i.s [1,3] [2,4,5,6]. [2,4,1] actually existent [3,5,6] existent? if yes, then count it. Do this for every 
    possible prefix-suffix using the excluded vertex.
*/
import java.util.*;
import java.util.Arrays;
import java.io.*;

public class PairAndSetGenerator
{
	//Test purposes only
    public static void main(String[] args) 
	{
		// ArrayList<Integer> verts = new ArrayList<Integer>();
		// int prefix_length = 2;
		// int suffix_length = 2;
		// Graph<Integer> graph = constructGraph(verts);

		// int[] vertices = new int[verts.size()]; //11,12,13,14,15,16,17,18,19,20;
  //       //verts.toArray(vertices);
  //       for(int j=0; j<verts.size();j++)
  //       {
  //           vertices[j] = verts.get(j);
  //       }
		// int number_of_verts = verts.size();
		// generatePairAndSet(vertices,prefix_length+suffix_length);
        startGeneration(4, 1, 2);
	}
    public static void startGeneration(int prefSufLength, int source, int dest)
    {
        ArrayList<Integer> verts = new ArrayList<Integer>();
        Graph<Integer> graph = constructGraph(verts);

        int[] vertices = new int[verts.size()]; //11,12,13,14,15,16,17,18,19,20;
        for(int j=0; j<verts.size();j++)
        {
            vertices[j] = verts.get(j);
        }
        int number_of_verts = verts.size();
        //System.out.println(number_of_verts);
        generatePairAndSet(vertices,prefSufLength, source, dest);
    }

    public static Graph constructGraph(ArrayList<Integer> verts)
    {
        File original = new File("original.txt");
        Graph<Integer> originalGraph = new Graph<Integer>();
        try
        {
            Scanner in = new Scanner(original);
            
            //ArraysList<Integer> verts= new ArrayList<Integer>(); 
            while(in.hasNextLine())
            {
                String line  = in.nextLine();
                String[] lineArray = line.split(" ");
                verts.add(Integer.parseInt(lineArray[0]));
                for(int i = 1; i<lineArray.length; i++)
                {
                    originalGraph.addEdge(Integer.parseInt(lineArray[0]), Integer.parseInt(lineArray[i]));
                }
            }
        }
        catch(FileNotFoundException e)
        {
            e.printStackTrace();
        }

        return originalGraph;
    }
	public static void generatePairAndSet(int[] verts, int setLength, int source, int dest)
	{
		generateCombination(verts,verts.length,2,setLength, source, dest);
	}

	public static void combinationUtil(int[] arr, int[] data, int start,int end, int index, int r,int setLength, int source, int dest)
    {
        if (index == r)
        {
            //Sice its a pair, generate all sets of length pref+suff-2
            if((data[0]==source) && (data[1]==dest))
            {
                generateAndPrintSet(arr,data,setLength-r);
            }
            else if((data[1]==source) && (data[0]==dest))
            {
                int[] tempdata = new int[data.length];
                int f =0;
                for (int l=data.length-1; l>=0; l--)
                {
                    tempdata[f] = data[l];
                    f++;
                }
                generateAndPrintSet(arr, tempdata, setLength-r);
            }
            return;
        }
        for (int i=start; i<=end && end-i+1 >= r-index; i++)
        {
            data[index] = arr[i];
            combinationUtil(arr, data, i+1, end, index+1, r,setLength, source, dest);
        }
    }
 
    public static void generateCombination(int[] arr, int n, int r, int setLength, int source, int dest)
    {
        int[] data=new int[r];
        combinationUtil(arr, data, 0, n-1, 0, r,setLength, source,dest);
    }

    public static void generateAndPrintSet(int[] arr,int[] data,int actual_setLength)
    {
    	
    	//Set<Integer> setA = new HashSet(Arrays.asList(temp_arr));
		Set<Integer> setB = new HashSet<Integer>();
		for (int k = 0; k<data.length;k++)
		{
			setB.add(data[k]);
		}

    	int[] temp_arr = new int[arr.length-data.length];
    	int l = 0;
    	for (int m = 0; m<arr.length;m++)
    	{
    		if(!setB.contains(arr[m]))
    		{
    			temp_arr[l] = arr[m];
    			l++;
    		}
    	}
    	arr = temp_arr;
        int[] new_data=new int[actual_setLength];
        int n = arr.length;
        combinationUtility(arr, new_data, 0, n-1, 0, actual_setLength,data);
    }
    public static void combinationUtility(int[] arr, int[] new_data, int start,int end, int index, int setLength,int[] data)
    {
        
        if (index == setLength)
        {
           	printSourceDestAndSet(new_data,data);
            return;
        }

        for (int i=start; i<=end && end-i+1 >= setLength-index; i++)
        {
            new_data[index] = arr[i];
            combinationUtility(arr, new_data, i+1, end, index+1, setLength,data);
        }
    }
    public static void printSourceDestAndSet(int[] setToExclude, int[] source_dest)
    {
    	System.out.println(Arrays.toString(source_dest) +" : "+ Arrays.toString(setToExclude));
		//int[] dest_source = new int[source_dest.length];
		//dest_source[0] = source_dest[1];
		//dest_source[1] = source_dest[0];
    	//System.out.println(Arrays.toString(dest_source) +" : "+ Arrays.toString(setToExclude));
    }

}