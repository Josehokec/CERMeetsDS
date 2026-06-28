package plan;

import java.util.ArrayList;
import java.util.List;

public class Permutations {
    public List<List<String>> permute(String[] strings) {
        List<List<String>> st = new ArrayList<>();
        List<String> a = new ArrayList<>();
        if(strings.length == 0) return st;

        boolean[] visited = new boolean[strings.length];
        searchStrings(strings, st, a, visited);
        return st;
    }

    public void searchStrings(String[] strings, List<List<String>> st, List<String> a, boolean[] visited){
        if(a.size() == strings.length){
            st.add(new ArrayList<>(a));
            return;
        }

        for(int i = 0; i < strings.length; i++){
            if(!visited[i]){
                a.add(strings[i]);
                visited[i] = true;
                searchStrings(strings, st, a, visited);
                a.remove(a.size() - 1);
                visited[i] = false;
            }
        }
    }

    public static void main(String[] args){
        long startTime = System.nanoTime();
        //"V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9"
        String[] strings = {"V1", "V2", "V3", "V4", "V5", "V6", "V7"};
        List<List<String>> ans1 = new Permutations().permute(strings);
        //System.out.println("answer:" + ans1);
        System.out.println("size:" + ans1.size());
        long endTime = System.nanoTime();
        System.out.println("cost: " + (endTime - startTime + 0.0)/1_000_000 + "ms.");
    }

}

/*
public List<List<Integer>> permute(int[] numbers) {
        List<List<Integer>> st = new ArrayList<>();
        List<Integer> a = new ArrayList<>();
        if(numbers.length == 0) return st;

        boolean[] visited = new boolean[numbers.length];
        searchNumbers(numbers, st, a, visited);
        return st;
    }

    public void searchNumbers(int[] numbers, List<List<Integer>> st, List<Integer> a, boolean[] visited){
        if(a.size() == numbers.length){
            st.add(new ArrayList<>(a));
            return;
        }

        for(int i = 0; i < numbers.length; i++){
            if(!visited[i]){
                a.add(numbers[i]);
                visited[i] = true;
                searchNumbers(numbers, st, a, visited);
                a.remove(a.size() - 1);
                visited[i] = false;
            }
        }
    }

    {
        int[] numbers = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        long startTime2 = System.nanoTime();
        List<List<Integer>> ans2 = new Permutations().permute(numbers);
        System.out.println("size:" + ans2.size());
        long endTime2 = System.nanoTime();
        System.out.println("cost: " + (endTime2 - startTime2 + 0.0)/1_000_000 + "ms.");
    }
 */
