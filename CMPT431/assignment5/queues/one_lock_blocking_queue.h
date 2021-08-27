#include "../common/allocator.h"
#include <mutex>


template <class T>
class Node
{
  public:
    T value;
    Node<T>* next;
};

extern std::atomic<bool> no_more_enqueues;

template <class T>
class OneLockBlockingQueue
{
    Node<T>* head;
    Node<T>* tail;
    std::mutex lock_;
    std::atomic<bool> wakeup_dq;  // signal for enqueuers to wake up waiting dequeuers
    CustomAllocator my_allocator_;
public:
    OneLockBlockingQueue() : my_allocator_()
    {
        std::cout << "Using OneLockBlockingQueue\n";
    }

    void initQueue(long t_my_allocator_size){
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Initialize the queue head or tail here
        Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
        newNode->next = NULL;
        head = tail = newNode;
        wakeup_dq.store(false);
        //my_allocator_.freeNode(newNode);
    }

    void enqueue(T value)
    {
	    // add your enqueue code here
      Node<T>* node = (Node<T>* )my_allocator_.newNode();
      node->value = value;
      node->next = NULL;
      lock_.lock();
      tail->next = node;
      tail = node;
      wakeup_dq.store(true);
      lock_.unlock();
    }

    bool dequeue(T *value)
    {
      bool ret_value = false;
      lock_.lock();
      Node<T>* node = head;
      Node<T>* new_head = head->next;

      while(new_head == NULL)
      {
      // Queue is empty Wait until enqueuer wakes me up OR no more enqueues are coming
        // wakeup_dq.store(false);
        lock_.unlock();
        while (no_more_enqueues == false && wakeup_dq.load()==false);
        lock_.lock();

        if(head != tail)
        {
          node = head;
          new_head = head->next;
          if (new_head == tail)
          {
            // std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"<< std::endl;
            wakeup_dq.store(false);
          }
        }
        if(new_head == NULL && no_more_enqueues == true){
          lock_.unlock();
          return ret_value;
        }

      }

      *value = new_head->value;
      head = new_head;
      ret_value = true;
      lock_.unlock();
      my_allocator_.freeNode(node);
      return ret_value;

    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};
