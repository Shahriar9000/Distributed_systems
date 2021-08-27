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
class OneLockQueue
{
    CustomAllocator my_allocator_;
    std::mutex lock_;
    Node<T>* head;
    Node<T>* tail;
public:
    OneLockQueue() : my_allocator_()
    {
        std::cout << "Using OneLockQueue\n";
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
      lock_.lock();
      tail->next = node;
      tail = node;
      lock_.unlock();

    }

    bool dequeue(T *value)
    {
      bool ret_value = false;
      lock_.lock();
      Node<T>* node = head;
      Node<T>* new_head = head->next;
      if(new_head == NULL){
          // Queue is empty
          lock_.unlock();
          return ret_value;
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
