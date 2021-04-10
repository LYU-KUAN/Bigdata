package com.daniel.mr;

/**
 * @Author Brian
 * @Description 启动类
 * 进行 Transition Matrix * PR Matrix 的迭代，并将结果写到硬盘上
 **/
public class Driver {
    public static void main(String[] args) throws Exception {
        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();
        // transition.txt
        String transitionMatrix = args[0]; // dir where transition.txt resides
        // pr.txt(也是第二个mr的输出)
        String prMatrix = args[1];
        // subPR(第一个mr的输出)
        String subPageRank = args[2];
        // 迭代次数
        int count = Integer.parseInt(args[3]);
        for (int i = 0; i < count; i++) {
            // 将上面的三个值传递给第一个mr，并记录每一次迭代的下标
            String[] args1 = {transitionMatrix, prMatrix + i, subPageRank + i};
            multiplication.main(args1);
            // 将第一个mr的输出即subPR传递给第二个mr,并且每一次会输出一个pr i文件夹(pr0,pr1,pr2......),最终结果是最后一次迭代的结果，即prN
            String[] args2 = {subPageRank + i, prMatrix + (i + 1)};
            sum.main(args2);
        }

    }

}
