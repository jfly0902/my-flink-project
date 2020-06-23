package myflink;

import cn.hutool.dfa.WordTree;

import java.util.HashSet;
import java.util.List;

/**
 * @Author: xiang.jin
 * @Date: 2020/5/8 16:36
 */
public class HutoolTest {

    public static void main(String[] args) {
        WordTree wordTree = new WordTree();
        wordTree.addWords("大傻逼","傻逼","裸奔","你妈的");

        String str = "裸，你这个傻逼，真的是一个大#傻￥逼，&你*妈*真*的*是爱你的。你这个裸露的人";

        System.out.println(wordTree.match(str));
        System.out.println("==============");

        List<String> list = wordTree.matchAll(str, -1, true, true);

        for (String match :
                list) {
            System.out.println(match);
        }
    }


}
