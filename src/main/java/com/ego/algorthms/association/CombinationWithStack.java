package com.ego.algorthms.association;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

public class CombinationWithStack {

    private Stack<String> stack = new Stack<>();
    public List<List<String>> result = new ArrayList<>();

    /**
     *
     * @param elements  元素
     * @param n         要选多少个元素
     * @param length    当前有多少个元素
     * @param index     当前选到的下标
     *
     * 1    2   3     //开始下标到2
     * 1    2   4     //然后从3开始
     */
    public void findCombinations(List<String> elements, int n, int length, int index) {
        if (length == n) {
            this.result.add(new ArrayList<>(this.stack));
            return;
        }

        for (int i = index; i < elements.size(); i++) {
            if (!this.stack.contains(elements.get(i))) {
                this.stack.add(elements.get(i));
                findCombinations(elements, n, length + 1, i);
                this.stack.pop();
            }
        }
    }

    public static void main(String[] args) {
        List<String> list = Arrays.asList("a", "b", "c", "d");
        CombinationWithStack com = new CombinationWithStack();
        com.findCombinations(list, 3, 0, 0);
        System.out.println(com.result);
    }
}
