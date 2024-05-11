#ifndef QUEUE_H
#define QUEUE_H

typedef void* Item;
typedef struct node * PNode;
typedef struct node {
    Item data;
    PNode next;
} Node;

typedef struct {
    PNode front;
    PNode rear;
    int size;
} Queue;

/*构造一个空队列*/
Queue* InitQueue();

/*销毁一个队列*/
void DestroyQueue(Queue *pqueue);

/*清空一个队列*/
void ClearQueue(Queue *pqueue);

/*判断队列是否为空*/
int IsEmpty(Queue *pqueue);

/*返回队列大小*/
int GetSize(Queue *pqueue);

/*返回队头元素*/
PNode GetFront(Queue *pqueue,Item *pitem);

/*返回队尾元素*/
PNode GetRear(Queue *pqueue,Item *pitem);

/*将新元素入队*/
PNode EnQueue(Queue *pqueue,Item item);

/*队头元素出队*/
PNode DeQueue(Queue *pqueue,Item *pitem);

/*遍历队列并对各数据项调用visit函数*/
void QueueTraverse(Queue *pqueue,void (*visit)());

#endif