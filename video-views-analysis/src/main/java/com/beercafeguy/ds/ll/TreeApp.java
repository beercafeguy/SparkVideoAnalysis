package com.beercafeguy.ds.ll;

public class TreeApp {

    public static void main(String[] args) {
        BinaryTree binaryTree=new BinaryTree();

        TreeNode n1=new TreeNode(10);
        TreeNode n2=new TreeNode(20);
        TreeNode n3=new TreeNode(30);
        TreeNode n4=new TreeNode(40);
        TreeNode n5=new TreeNode(50);
        TreeNode n6=new TreeNode(60);
        TreeNode n7=new TreeNode(70);

        binaryTree.root=n1;
        binaryTree.root.left=n2;
        binaryTree.root.right=n3;
        binaryTree.root.left.left=n4;
        binaryTree.root.left.right=n5;
        binaryTree.root.right.left=n6;
        binaryTree.root.right.right=n7;

        binaryTree.inOrderPrint(n1);
        System.out.println();
        binaryTree.invert(n1);
        binaryTree.inOrderPrint(n1);


    }
}


class BinaryTree{
    TreeNode root;

    public BinaryTree(){
        root=null;
    }

    public void invert(TreeNode passedRoot){
        if (passedRoot ==null){
            return;
        } else {
            TreeNode tmp=passedRoot.left;
            passedRoot.left=passedRoot.right;
            passedRoot.right=tmp;
            invert(passedRoot.left);
            invert(passedRoot.right);
        }
    }

    public void inOrderPrint(TreeNode passedRoot){
        if(passedRoot!=null){
            inOrderPrint(passedRoot.left);
            System.out.println(passedRoot.data+" ");
            inOrderPrint(passedRoot.right);
        }

    }
}