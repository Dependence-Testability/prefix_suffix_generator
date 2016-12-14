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

public class PairAndSetGenerator
{
	public static void main(String[] args)
	{
		//ArrayList<Integer> verts = new ArrayList<Integer>();
		int prefix_length = 3;
		int suffix_length = 3;
		
		int[] verts = {1, 2, 3, 4, 5,6,7,8,9,10};//11,12,13,14,15,16,17,18,19,20};
		int number_of_verts = verts.length;
		generatePairAndSet(verts,prefix_length+suffix_length);
	}

	public static void generatePairAndSet(int[] verts, int setLength)
	{
		generateCombination(verts,verts.length,2,setLength);
	}

	public static void combinationUtil(int[] arr, int[] data, int start,int end, int index, int r,int setLength)
    {
        if (index == r)
        {
            //Sice its a pair, generate all sets of length pref+suff-2
            generateAndPrintSet(arr,data,setLength-r);
            return;
        }
        for (int i=start; i<=end && end-i+1 >= r-index; i++)
        {
            data[index] = arr[i];
            combinationUtil(arr, data, i+1, end, index+1, r,setLength);
        }
    }
 
    public static void generateCombination(int[] arr, int n, int r, int setLength)
    {
        int[] data=new int[r];
        combinationUtil(arr, data, 0, n-1, 0, r,setLength);
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
		int[] dest_source = new int[source_dest.length];
		dest_source[0] = source_dest[1];
		dest_source[1] = source_dest[0];
    	System.out.println(Arrays.toString(dest_source) +" : "+ Arrays.toString(setToExclude));
    }

}