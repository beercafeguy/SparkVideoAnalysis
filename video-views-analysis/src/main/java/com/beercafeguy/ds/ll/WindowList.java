package com.beercafeguy.ds.ll;

import java.util.LinkedList;

public class WindowList {

    public static void main(String[] args) {

        LinkedList<Node> ll=new LinkedList<Node>();


        Node node1=new Node(1,19);
        Node node2=new Node(2,6);
        Node node3=new Node(8,10);
        Node node4=new Node(9,13);
        Node node5=new Node(12,14);
        Node node6=new Node(15,18);


        ll.addFirst(node1);
        ll.add(node2);
        ll.add(node3);
        ll.add(node4);
        ll.add(node5);
        ll.add(node6);


        //head
        //System.out.println(ll.getFirst());

        for (int i=0;i<ll.size()-1;i++){
            Node current=ll.get(i);
            Node next=ll.get(i+1);
            if(current.end > next.start){
                ll.remove(i+1);
                if(current.end < next.end) {
                    current.end = next.end;
                }
                i--;
            }
        }

        for(Node item: ll){
            System.out.println(item);
        }

    }

}
