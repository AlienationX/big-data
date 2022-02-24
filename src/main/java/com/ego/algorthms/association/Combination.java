package com.ego.algorthms.association;

import java.time.LocalDateTime;
import java.time.Duration;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.ego.algorthms.association.CombinationWithStack;


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

    public static List<List<String>> findSortedCombinationsOptimize(List<String> elements, int n) {
        List<List<String>> result = new ArrayList<>();
        if (n <= 0) {
            result.add(new ArrayList<>());
            return result;
        } else if (n == 1) {
            for (String element : elements) {
                result.add(Collections.singletonList(element));
            }
            return result;
        }

        List<List<String>> combinations = findSortedCombinationsOptimize(elements, n - 1);
        int elementsSize = elements.size();
        for (List<String> combination : combinations) {
            String lastValue = combination.get(n - 2);
            int lastValueIndex = elements.indexOf(lastValue);
            // int lastValueIndex = binSearchSortedList(elements, lastValue);  // 二分查找，小数据量没效果
            for (int j = lastValueIndex + 1; j < elementsSize; j++) {
                List<String> list = new ArrayList<>(combination);
                list.add(elements.get(j));
                Collections.sort(list);
                result.add(list);
            }
        }
        return result;
    }

    public static List<List<String>> findSortedCombinationsStack(List<String> elements, int n) {
        List<List<String>> result = new ArrayList<>();
        if (n <= 0) {
            result.add(new ArrayList<>());
            return result;
        } else if (n == 1) {
            for (String element : elements) {
                result.add(Collections.singletonList(element));
            }
            return result;
        }

        List<List<String>> combinations = findSortedCombinationsStack(elements, n - 1);
        int elementsSize = elements.size();
        for (List<String> combination : combinations) {
            String lastValue = combination.get(n - 2);
            int lastValueIndex = elements.indexOf(lastValue);
            // int lastValueIndex = binSearchSortedList(elements, lastValue);  // 二分查找，小数据量没效果
            for (int j = lastValueIndex + 1; j < elementsSize; j++) {
                List<String> list = new ArrayList<>(combination);
                list.add(elements.get(j));
                Collections.sort(list);
                result.add(list);
            }
        }
        return result;
    }

    public static List<List<String>> findSortedCombinationsMix(List<String> elements, int n) {
        List<List<String>> result = new ArrayList<>();
        if (n <= 0) {
            result.add(new ArrayList<>());
            return result;
        } else if (n == 1) {
            for (String element : elements) {
                result.add(Collections.singletonList(element));
            }
            return result;
        }

        // 参考python的apriori算法，n项集的前n-1项集相同才相加
        List<List<String>> combinations = findSortedCombinationsMix(elements, n - 1);
        int combinationsNum = combinations.size();
        for (int i = 0; i < combinationsNum; i++) {
            List<String> l1 = new ArrayList<>(combinations.get(i));
            List<String> l1Part = l1.subList(0, n - 2);
            Collections.sort(l1Part);
            for (int j = i + 1; j < combinationsNum; j++) {
                List<String> l2 = new ArrayList<>(combinations.get(j));
                List<String> l2Part = l2.subList(0, n - 2);
                Collections.sort(l2Part);
                if (l1Part.equals(l2Part)) {
                    // 两个list求并集，并去重复
                    List<String> combination = new ArrayList<>(l1);
                    combination.removeAll(l2); // 去除交集
                    combination.addAll(l2);  // 添加新的list
                    Collections.sort(combination);
                    result.add(combination);
                }
            }
        }

        return result;
    }

    public static List<List<String>> subsets(List<String> list) {
        List<List<String>> res = new ArrayList<>();
        res.add(new ArrayList<>());
        for (String val : list) {
            int all = res.size();
            for (int j = 0; j < all; j++) {
                List<String> tmp = new ArrayList<>(res.get(j));
                tmp.add(val);
                res.add(tmp);
            }
        }
        return res;
    }

    public static List<List<String>> subsets(List<String> list, int n) {
        List<List<String>> result = new ArrayList<>();
        if (n <= 0) {
            result.add(new ArrayList<>());
            return result;
        }

        List<List<String>> combinations = subsets(list);
        for (List<String> combination : combinations) {
            if (combination.size() == n) {
                result.add(combination);
            }
        }
        return result;
    }

    public static List<List<String>> subsetsBin(List<String> list, int n) {
        // 不推荐。2的n次方-1，n=67就会超出存储的数字最大值变成0，最后返回结果为空数组
        List<List<String>> result = new ArrayList<>();
        int listLength = list.size();
        long x = 1L;
        for (int i = 0; i < list.size(); i++) {
            x *= 2;  // 不行，数字太大存储不下，67次方就超出限定数字的最大值变成0
        }
        for (long i = 0; i < x - 1; i++) {
            String s = Long.toBinaryString(i);
            s = String.format("%" + listLength + "s", s).replace(" ", "0");
            int cnt = 0;
            List<String> combination = new ArrayList<>();
            for (int j = 0; j < s.length(); j++) {
                if (Integer.parseInt(String.valueOf(s.charAt(j))) == 1) {
                    combination.add(list.get(j));
                    cnt += 1;
                }
            }
            if (cnt == n) {
                result.add(combination);
            }
        }
        return result;
    }

    public static List<List<String>> leetCode(List<String> list) {
        // 不推荐。同二进制的方法，也会超出存储的数字最大值变成0，最后返回结果为空数组
        List<String> t = new ArrayList<>();
        List<List<String>> res = new ArrayList<>();

        int n = list.size();
        for (int mask = 0; mask < (1 << n); ++mask) {
            t.clear();
            for (int i = 0; i < n; ++i) {
                if ((mask & (1 << i)) != 0) {
                    t.add(list.get(i));
                }
            }
            res.add(new ArrayList<>(t));
        }
        return res;
    }

    public static List<List<String>> leetCode(List<String> list, int n) {
        List<List<String>> result = new ArrayList<>();
        if (n <= 0) {
            result.add(new ArrayList<>());
            return result;
        }

        List<List<String>> combinations = leetCode(list);
        for (List<String> combination : combinations) {
            if (combination.size() == n) {
                result.add(combination);
            }
        }
        return result;
    }

    public static int binSearchSortedList(List<String> list, String value) {
        int start = 0;
        int end = list.size() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            if (value.equals(list.get(mid))) {
                return mid;
            } else if (value.compareTo(list.get(mid)) > 0) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }
        return -1;
    }

    public static void printSpentTime(LocalDateTime startDT, LocalDateTime endDT) {
        Duration dr = Duration.between(startDT, endDT);
        System.out.println("Spent time millis: " + dr.toMillis());
        System.out.println("Spent time seconds: " + dr.toMillis() / 1000);
    }

    public static void test() {
        List<String> list = Arrays.asList("a", "b", "c", "d");
        System.out.println("list=" + list);
        List<List<String>> comb = findSortedCombinations(list, 3);
        System.out.println(comb.size());
        System.out.println(comb);

        List<List<String>> comb1 = findSortedCombinations(list);
        // 组合项个数：2^n-1
        System.out.println(comb1.size());
        System.out.println(comb1);

        // 性能测试
        list = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            list.add(String.valueOf(i));
        }
        Collections.sort(list);

        LocalDateTime startDT;
        LocalDateTime endDT;

        System.out.println("------------------ " + "findSortedCombinations");
        startDT = LocalDateTime.now();
        // System.out.println(findSortedCombinations(list, 2).size());
        endDT = LocalDateTime.now();
        printSpentTime(startDT, endDT);

        System.out.println("------------------ " + "findSortedCombinationsOptimize");
        System.out.println(findSortedCombinationsOptimize(Arrays.asList("a", "b", "c", "d", "e"), 3));
        startDT = LocalDateTime.now();
        // System.out.println(findSortedCombinationsOptimize(list, 3).size());
        endDT = LocalDateTime.now();
        printSpentTime(startDT, endDT);

        System.out.println("------------------ " + "CombinationWithStack");
        CombinationWithStack comDemo = new CombinationWithStack();
        comDemo.findCombinations(Arrays.asList("a", "b", "c", "d", "e"), 3, 0, 0);
        System.out.println(comDemo.result);
        startDT = LocalDateTime.now();
        CombinationWithStack com = new CombinationWithStack();
        com.findCombinations(list, 3, 0, 0);
        System.out.println(com.result.size());
        endDT = LocalDateTime.now();
        printSpentTime(startDT, endDT);

        System.out.println("------------------ " + "subsets");
        System.out.println(subsets(Arrays.asList("a", "b", "c", "d"), 3));
        startDT = LocalDateTime.now();
        // System.out.println(subsets(list, 2).size());  // 寻找所有组合后再过滤，很多组合都没用，太太慢
        endDT = LocalDateTime.now();
        printSpentTime(startDT, endDT);

        System.out.println("------------------ " + "subsetsBin");
        System.out.println(subsetsBin(Arrays.asList("a", "b", "c", "d"), 3));
        startDT = LocalDateTime.now();
        // System.out.println(subsetsBin(list, 2).size());  // 寻找所有组合后再过滤，很多组合都没用，太太慢
        endDT = LocalDateTime.now();
        printSpentTime(startDT, endDT);

        System.out.println("------------------ " + "leetCode");
        System.out.println(leetCode(Arrays.asList("a", "b", "c", "d"), 3));
        startDT = LocalDateTime.now();
        // System.out.println(leetCode(list, 2).size());  // 寻找所有组合后再过滤，很多组合都没用，太太慢
        endDT = LocalDateTime.now();
        printSpentTime(startDT, endDT);

        System.out.println("------------------ " + "findSortedCombinationsMix");
        System.out.println(findSortedCombinationsMix(Arrays.asList("a", "b", "c", "d", "e"), 3));
        startDT = LocalDateTime.now();
        // System.out.println(findSortedCombinationsMix(list, 3).size());
        endDT = LocalDateTime.now();
        printSpentTime(startDT, endDT);

        System.out.println(binSearchSortedList(Arrays.asList("a", "b", "c"), "c"));
    }

    public static void main(String[] args) {
        test();
    }
}
