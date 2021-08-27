#include "../common/allocator.h"
#include <mutex>

template <class T>
class Node
{
  public:
    T value;
    Node<T>* next;
};

template <class T>
class TwoLockQueue
{
    CustomAllocator my_allocator_;
    std::mutex head_lock, tail_lock;
    Node<T>* head;
    Node<T>* tail;

public:
    TwoLockQueue() : my_allocator_()
    {
        std::cout << "Using TwoLockQueue\n";
    }

    void initQueue(long t_my_allocator_size){
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Initialize the queue head or tail here
        Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
        newNode->next = NULL;
        head = tail = newNode;
        my_allocator_.freeNode(newNode);
    }

    void enqueue(T value)
    {
      Node<T>* node = (Node<T>* )my_allocator_.newNode();
      node->value = value;
      node->next = NULL;
      tail_lock.lock();
      tail->next = node;
      tail = node;
      tail_lock.unlock();
    }

    bool dequeue(T *value)
    {
      bool ret_value = false;
      head_lock.lock();
      Node<T>* node = head;
      Node<T>* new_head = head->next;
      if(new_head == NULL){
          // Queue is empty
          head_lock.unlock();
          return ret_value;
      }
      *value = new_head->value;
      head = new_head;
      ret_value = true;
      head_lock.unlock();
      my_allocator_.freeNode(node);
      return ret_value;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};
