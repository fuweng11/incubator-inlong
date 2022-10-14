package org.apache.inlong.agent.plugin.dbsync.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class RoundQueue<E> {

    private int capacity;
    private Object[] elementData;
    private int head = -1;
    private int tail = -1;


    public RoundQueue(int capacity){
        this.capacity = capacity;
        elementData = new Object[capacity];
    }


    public int size() {
        if (tail != -1) {
            int offset = head - tail;
            if (offset >= 0) {
                return offset + 1;
            } else {
                return capacity + offset + 1;
            }
        }

        return 0;
    }

    public boolean isEmpty() {
        return head == tail && tail == -1;
    }

    public void clear() {
        if(head != -1){
            int i = head;
            do{
                elementData[i] = null;
            } while(i != tail && ((i = ((i+1) % capacity)) > -2));
        }
    }

    public boolean add(E e) {

        if(e == null){
            throw new NullPointerException();
        }

        if(head == -1){
            elementData[0] = e;
            head = 0;
            tail = 0;
        } else {
            head = (head+1) % capacity;
            elementData[head] = e;
            if(head == tail){
                tail = (tail + 1) % capacity;
            }
        }

        return true;
    }

    public boolean offer(E e) {
        return add(e);
    }

    @SuppressWarnings("unchecked")
    public E remove() {

        if(head == -1){
            return null;
        }

        Object o = elementData[tail];
        elementData[tail] = null;

        if(head == tail){
            head = -1;
            tail = -1;
        } else {
            tail = (tail + 1) % capacity;
        }

        return (E)o;
    }

    @SuppressWarnings("unchecked")
    public E poll() {
        if(head == -1){
            return null;
        }

        Object o = elementData[head];
        elementData[head] = null;

        if(head == tail){
            head = -1;
            tail = -1;
        } else {
            head = (capacity + head -1) % capacity;
        }


        return (E)o;
    }

    @SuppressWarnings("unchecked")
    public E element() {
        if(head == -1){
            throw new NoSuchElementException("RoundQueue is Empty");
        }
        return (E)elementData[head];
    }

    @SuppressWarnings("unchecked")
    public E peek() {
        if(head == -1)
            return null;

        return (E)elementData[head];
    }

    public Iterator<E> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<E> {
        int cursor = tail;       // index of next element to return

        public boolean hasNext() {
            return cursor != -1;
        }

        @SuppressWarnings("unchecked")
        public E next() {
            if(cursor == -1){
                return null;
            }

            Object o = elementData[cursor];

            if(head == cursor){
                cursor = -1;
            } else {
                cursor = (cursor + 1) % capacity;
            }

            return (E)o;
        }

        public void remove() {
            throw new IllegalAccessError("RoundQueue iter not has this function!");
        }
    }
    
    public static void main(String[] args){
        RoundQueue<Long> rq = new RoundQueue<Long>(5);
        rq.add(0l);
        rq.add(1l);
        rq.add(2l);
        rq.add(3l);
        rq.add(4l);
        rq.add(5l);

        Iterator<Long> it = rq.iterator();

        while(it.hasNext()){
            System.out.println("Test -->> " + it.next());
        }

    }

}
