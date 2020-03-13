package com.ego.algorthms.association;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * 生成频繁项集的组合最关键的方法
 */
public class Combination {

    /**
     * Will return combinations of all sizes...
     * If elements = { a, b, c }, then findCollections(elements)
     * will return all unique combinations of elements as:
     * <p>
     * { [], [a], [b], [c], [a, b], [a, c], [b, c], [a, b, c] }
     *
     * @param <T>
     * @param elements a collection of type T elements
     * @return unique combinations of elements
     */
    public static List<List<String>> findSortedCombinations(List<String> elements) {
        List<List<String>> result = new ArrayList<>();
        for (int i = 1; i <= elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }
        return result;
    }

    /**
     * Will return unique combinations of size=n.
     * If elements = { a, b, c }, then findCollections(elements, 2) will return:
     * <p>
     * { [a, b], [a, c], [b, c] }
     *
     * @param <T>
     * @param elements a collection of type T elements
     * @param n        size of combinations
     * @return unique combinations of elements of size = n
     */
    public static List<List<String>> findSortedCombinations(List<String> elements, int n) {
        List<List<String>> result = new ArrayList<>();
        if (n <= 0) {
            result.add(new ArrayList<>());
            return result;
        }

        List<List<String>> combinations = findSortedCombinations(elements, n - 1);
        for (List<String> combination : combinations) {
            for (String element : elements) {
                if (combination.contains(element)) {
                    continue;
                }

                List<String> list = new ArrayList<>(combination);
                // 下面多余，有些重复了
                // list.addAll(combination);
                // if (list.contains(element)) {
                //     continue;
                // }
                list.add(element);
                Collections.sort(list);

                if (result.contains(list)) {
                    continue;
                }
                result.add(list);
            }
        }

        return result;
    }


    public static void main(String[] args) {
        List<String> list = Arrays.asList("a", "b", "c", "d");
        System.out.println("list=" + list);
        List<List<String>> comb = findSortedCombinations(list, 3);
        System.out.println(comb.size());
        System.out.println(comb);

        List<List<String>> comb1 = findSortedCombinations(list);
        // 组合项个数：2^n-1
        System.out.println(comb1.size());
        System.out.println(comb1);
    }
}
