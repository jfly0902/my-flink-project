package com.yskj.flink.task;

/**
 *
 * 使用堆排序，维护 TOP3
 *
 * @Author: xiang.jin
 * @Date: 2020/5/20 10:16
 */
public class HeapSort {

    public static void main(String[] args) {
        int a[] = {40,55,49,73,12,27,98,81,64,36,78};

        int[] top3ByHeap = getTop3ByHeap(a);

        for (int i : top3ByHeap) {
            System.out.println(i);
        }

    }

    private static int[] getTop3ByHeap(int[] arr) {
        //  创建堆
        for (int i = (arr.length -1) / 2; i >= 0; i--) {
            // 调整堆的数据
            adjustHeap(arr, i, arr.length);
        }

        //调整堆结构+交换堆顶元素与末尾元素
        for (int i = arr.length - 1; i > 0; i--) {
            //将堆顶元素与末尾元素进行交换
            int temp = arr[i];
            arr[i] = arr[0];
            arr[0] = temp;

            //重新对堆进行调整
            adjustHeap(arr, 0, i);
        }

        return arr;
    }

    private static void adjustHeap(int[] arr, int parent, int length) {

        // 拿到 parent节点的数据
        int tmp = arr[parent];

        // 拿到父节点的左边的节点
        int leftChild = 2 * parent +1;

        // 存在着左节点
        while (leftChild < length) {

            // 右节点
            int rightChild = leftChild + 1;

            // 右节点存在，并且右节点的值 大于 左节点的值，左右节点互换，选取 右节点的值
            if (rightChild < length && arr[leftChild] < arr[rightChild]) {
                leftChild++;
            }

            // 父节点的值大于 子节点的值，跳出
            if (tmp > arr[leftChild]) {
                break;
            }

            // 将父节点 和 子节点的值互换
            arr[parent] = arr[leftChild];

            // 选取左节点的位置作为父节点
            parent = leftChild;
            leftChild = 2 * leftChild + 1;

        }

        arr[parent] = tmp;

    }


}
