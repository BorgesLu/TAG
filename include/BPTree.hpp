#pragma once

#include <iostream>
#include <vector>
#include <tuple>
#include <algorithm>
#include <fstream>
#include <string>
#include <list>
//#include <filesystem>
#include <bitset>
using namespace std;

using offset_size_t = uint64_t;
using block_size_t = uint64_t;
using page_size_t = uint8_t;
using data_offset_size_t = uint16_t;

struct KeyValueOffset
{
    static constexpr offset_size_t INVALID = 0xFFFFFFFFFFFFFFFF;

    union
    {
        offset_size_t offset;
        struct
        {
            // KevValueOffest is 8 Byte， 64 bit;
            // 45 3 16 是所占的比特位
            block_size_t block_number : 45;
            page_size_t page_number : 3;
            data_offset_size_t data_offset : 16;
        };
    };

    KeyValueOffset() : offset{INVALID} {}

    static KeyValueOffset NONE() { return KeyValueOffset{INVALID}; }

    explicit KeyValueOffset(const offset_size_t offset) : offset(offset) {}

    KeyValueOffset(const block_size_t block_number, const page_size_t page_number, const data_offset_size_t slot)
        : block_number{block_number}, page_number{page_number}, data_offset{slot} {}

    static KeyValueOffset Tombstone()
    {
        return KeyValueOffset{};
    }

    inline std::tuple<block_size_t, page_size_t, data_offset_size_t> get_offsets() const
    {
        return {block_number, page_number, data_offset};
    }

    inline bool is_tombstone() const
    {
        return offset == INVALID;
    }

    inline bool operator==(const KeyValueOffset &rhs) const { return offset == rhs.offset; }
    inline bool operator!=(const KeyValueOffset &rhs) const { return offset != rhs.offset; }
};

using IndexV = KeyValueOffset;

class Node
{
    /*
        Generally size of the this node should be equal to the block size. Which will limit the number of disk access and increase the accesssing time.
        Intermediate nodes only hold the Tree pointers which is of considerably small size(so they can hold more Tree pointers) and only Leaf nodes hold
        the data pointer directly to the disc.

        IMPORTANT := All the data has to be present in the leaf node
    */
public:
    bool isLeaf;
    vector<uint64_t> keys;
    // Node* ptr2parent; //Pointer to go to parent node CANNOT USE check https://stackoverflow.com/questions/57831014/why-we-are-not-saving-the-parent-pointer-in-b-tree-for-easy-upward-traversal-in
    Node *ptr2next; // Pointer to connect next node for leaf nodes
    union ptr
    {                            // to make memory efficient Node
        vector<Node *> ptr2Tree; // Array of pointers to Children sub-trees for intermediate Nodes
        // vector<FILE*> dataPtr;   // Data-Pointer for the leaf node
        vector<IndexV> dataPtr; // Data-Pointer for the leaf node

        ptr();  // To remove the error !?
        ~ptr(); // To remove the error !?
    } ptr2TreeOrData;

    friend class BPTree; // to access private members of the Node and hold the encapsulation concept
public:
    Node();
};

Node::ptr::ptr()
{
}

Node::ptr::~ptr()
{
}

Node::Node()
{
    this->isLeaf = false;
    this->ptr2next = NULL;
}

class BPTree
{
    /*
        ::For Root Node :=
            The root node has, at least two tree pointers
        ::For Internal Nodes:=
            1. ceil(maxIntChildLimit/2)     <=  #of children <= maxIntChildLimit
            2. ceil(maxIntChildLimit/2)-1  <=  #of keys     <= maxIntChildLimit -1
        ::For Leaf Nodes :=
            1. ceil(maxLeafNodeLimit/2)   <=  #of keys     <= maxLeafNodeLimit -1
    */
private:
    uint32_t maxIntChildLimit;                                    // Limiting  #of children for internal Nodes!
    uint32_t maxLeafNodeLimit;                                    // Limiting #of nodes for leaf Nodes!!!
    Node *root;                                                   // Pointer to the B+ Tree root
    void insertInternal(uint64_t x, Node **cursor, Node **child); // Insert x from child in cursor(parent)
    Node **findParent(Node *cursor, Node *child);
    Node *firstLeftNode(Node *cursor);

public:
    BPTree();
    BPTree(int degreeInternal, int degreeLeaf);
    Node *getRoot();
    //  uint32_t getMaxIntChildLimit();
    //  uint32_t getMaxLeafNodeLimit();
    void setRoot(Node *);
    // void display(Node* cursor);
    // void seqDisplay(Node* cursor);
    IndexV Get(const uint64_t& key); //
    //void Insert(uint64_t key, IndexV dataPtr);
    void Insert(uint64_t key, IndexV dataPtr);
    // void insert(uint64_t key, FILE* filePtr);
    // void removeKey(uint32_t key);
    // void removeInternal(uint32_t x, Node* cursor, Node* child);
    //*********  micro-benchmark    ********
     //void getNeighbors(uint64_t key, vector<uint64_t> &adjList); //for bfs,return the adj-list of srcVID
     void getNeighbors(uint64_t VID, vector<IndexV> &adjList); //return the adj-list address of srcVID
     void getNeighborsFromIndex(uint64_t VID, int& countFromIndex);
     //void getNeighborsFromIndex(uint64_t key,uint64_t countFromIndex);
    // void getNeighborsFromData(uint32_t key);
    // void bfsFromIndex(uint32_t key);
    // void bfsFromData(uint32_t key);
};

BPTree::BPTree()
{
    this->maxIntChildLimit = 31;
    this->maxLeafNodeLimit = 31; // node size is 31 * key size(8B) + 33 * pointer size(8B) = 512B
    this->root = NULL;
}

BPTree::BPTree(int degreeInternal, int degreeLeaf)
{
    this->maxIntChildLimit = degreeInternal;
    this->maxLeafNodeLimit = degreeLeaf;
    this->root = NULL;
}

//
static Node* forParent = NULL;

Node **BPTree::findParent(Node *cursor, Node *child)
{
    /*
        Finds parent using depth first traversal and ignores leaf nodes as they cannot be parents
        also ignores second last level because we will never find parent of a leaf node during insertion using this function
    */
    std::cout<<"inside findParent\n";
  //  Node *parent = NULL;

    if (cursor->isLeaf || cursor->ptr2TreeOrData.ptr2Tree[0]->isLeaf)
        return NULL;

    for (int i = 0; i < cursor->ptr2TreeOrData.ptr2Tree.size(); i++)
    {
        if (cursor->ptr2TreeOrData.ptr2Tree[i] == child)
        {
           // parent = cursor;
           forParent = cursor;
        }
        else
        {
            // Commenting To Remove vector out of bound Error:
            // new (&cursor->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
            Node *tmpCursor = cursor->ptr2TreeOrData.ptr2Tree[i];
            findParent(tmpCursor, child);
        }
    }

    //return &parent;
    return &forParent;
}

Node *BPTree::firstLeftNode(Node *cursor)
{
    if (cursor->isLeaf)
        return cursor;
    for (int i = 0; i < cursor->ptr2TreeOrData.ptr2Tree.size(); i++)
        if (cursor->ptr2TreeOrData.ptr2Tree[i] != NULL)
            return firstLeftNode(cursor->ptr2TreeOrData.ptr2Tree[i]);

    return NULL;
}

void BPTree::insertInternal(uint64_t x, Node **cursor, Node **child)
{ // in Internal Nodes
std::cout<<"inside\n\n";
    if ((*cursor)->keys.size() < maxIntChildLimit - 1)
    {
        /*
            If cursor is not full find the position for the new key.
        */
        std::cout<<"L215 if\n";
        int i = std::upper_bound((*cursor)->keys.begin(), (*cursor)->keys.end(), x) - (*cursor)->keys.begin();
        (*cursor)->keys.push_back(x);
        //new (&(*cursor)->ptr2TreeOrData.ptr2Tree) std::vector<Node*>;
        //// now, root->ptr2TreeOrData.ptr2Tree is the active member of the union
        (*cursor)->ptr2TreeOrData.ptr2Tree.push_back(*child);

        if (i != (*cursor)->keys.size() - 1)
        { // if there are more than one element
            // Different loops because size is different for both (i.e. diff of one)

            for (int j = (*cursor)->keys.size() - 1; j > i; j--)
            { // shifting the position for keys and datapointer
                (*cursor)->keys[j] = (*cursor)->keys[j - 1];
            }

            for (int j = (*cursor)->ptr2TreeOrData.ptr2Tree.size() - 1; j > (i + 1); j--)
            {
                (*cursor)->ptr2TreeOrData.ptr2Tree[j] = (*cursor)->ptr2TreeOrData.ptr2Tree[j - 1];
            }

            (*cursor)->keys[i] = x;
            (*cursor)->ptr2TreeOrData.ptr2Tree[i + 1] = *child;
        }
        //cout << "Inserted key in the internal node :)" << endl;
    }
    else
    { // splitting
        cout << "Inserted Node in internal node successful" << endl;
        cout << "Overflow in internal:( HAIYAA! splitting internal nodes" << endl;

        vector<uint64_t> virtualKeyNode((*cursor)->keys);
        vector<Node *> virtualTreePtrNode((*cursor)->ptr2TreeOrData.ptr2Tree);

        int i = std::upper_bound((*cursor)->keys.begin(), (*cursor)->keys.end(), x) - (*cursor)->keys.begin(); // finding the position for x
        virtualKeyNode.push_back(x);                                                                           // to create space
        virtualTreePtrNode.push_back(*child);                                                                  // to create space
        std::cout<<"L250 create space\n";

        if (i != virtualKeyNode.size() - 1)
        {
            for (int j = virtualKeyNode.size() - 1; j > i; j--)
            { // shifting the position for keys and datapointer
                virtualKeyNode[j] = virtualKeyNode[j - 1];
            }

            for (int j = virtualTreePtrNode.size() - 1; j > (i + 1); j--)
            {
                virtualTreePtrNode[j] = virtualTreePtrNode[j - 1];
            }

            virtualKeyNode[i] = x;
            virtualTreePtrNode[i + 1] = *child;
        }

        uint64_t partitionKey;                                      // exclude middle element while splitting
        partitionKey = virtualKeyNode[(virtualKeyNode.size() / 2)]; // right biased
        int partitionIdx = (virtualKeyNode.size() / 2);

        // resizing and copying the keys & TreePtr to OldNode
        (*cursor)->keys.resize(partitionIdx);
        (*cursor)->ptr2TreeOrData.ptr2Tree.resize(partitionIdx + 1);
        (*cursor)->ptr2TreeOrData.ptr2Tree.reserve(partitionIdx + 1);
        for (int i = 0; i < partitionIdx; i++)
        {
            (*cursor)->keys[i] = virtualKeyNode[i];
        }

        for (int i = 0; i < partitionIdx + 1; i++)
        {
            (*cursor)->ptr2TreeOrData.ptr2Tree[i] = virtualTreePtrNode[i];
        }

        Node *newInternalNode = new Node;
        new (&newInternalNode->ptr2TreeOrData.ptr2Tree) std::vector<Node *>;
        // Pushing new keys & TreePtr to NewNode

        for (int i = partitionIdx + 1; i < virtualKeyNode.size(); i++)
        {
            newInternalNode->keys.push_back(virtualKeyNode[i]);
        }

        for (int i = partitionIdx + 1; i < virtualTreePtrNode.size(); i++)
        { // because only key is excluded not the pointer
            newInternalNode->ptr2TreeOrData.ptr2Tree.push_back(virtualTreePtrNode[i]);
        }
std::cout<<"L299 root\n";
        if ((*cursor) == root)
        {
            /*
                If cursor is a root we create a new Node
            */
            Node *newRoot = new Node;
            newRoot->keys.push_back(partitionKey);
            new (&newRoot->ptr2TreeOrData.ptr2Tree) std::vector<Node *>;
            newRoot->ptr2TreeOrData.ptr2Tree.push_back(*cursor);
            //// now, newRoot->ptr2TreeOrData.ptr2Tree is the active member of the union
            newRoot->ptr2TreeOrData.ptr2Tree.push_back(newInternalNode);

            root = newRoot;
            cout << "Created new ROOT!" << endl;
        }
        else
        {
            /*
                ::Recursion::
            */
            std::cout<<"L320 recur\n\n";
            insertInternal(partitionKey, findParent(root, *cursor), &newInternalNode);
            std::cout<<"after inser\n";

        }
    }
}

Node *BPTree::getRoot()
{
    return this->root;
}

void BPTree::setRoot(Node *ptr)
{
    this->root = ptr;
}

void BPTree::Insert(uint64_t key, IndexV dataPtr)
{
    if (root == NULL)
    {
        root = new Node;
        root->isLeaf = true;
        root->keys.push_back(key);
        new (&root->ptr2TreeOrData.dataPtr) std::vector<IndexV>;
        //// now, root->ptr2TreeOrData.dataPtr is the active member of the union
        root->ptr2TreeOrData.dataPtr.push_back(dataPtr);
        return;
    }
    else
    {
        Node *cursor = root;
        Node *parent = NULL;
        while (cursor->isLeaf == false)
        {
            parent = cursor;
            int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
            cursor = cursor->ptr2TreeOrData.ptr2Tree[idx];
        }

        // now cursor is the leaf node in which we'll insert the new key
        if (cursor->keys.size() < maxLeafNodeLimit)
        {
            /*
                If current leaf Node is not FULL, find the correct position for the new key and insert!
            */
            int i = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
            //插入
            cursor->keys.push_back(key);
            cursor->ptr2TreeOrData.dataPtr.push_back(dataPtr);

            //排序  此处的开销？
            if (i != cursor->keys.size() - 1)
            {
                for (int j = cursor->keys.size() - 1; j > i; j--)
                { // shifting the position for keys and datapointer
                    cursor->keys[j] = cursor->keys[j - 1];
                    cursor->ptr2TreeOrData.dataPtr[j] = cursor->ptr2TreeOrData.dataPtr[j - 1];
                }

                // since earlier step was just to inc. the size of vectors and making space, now we are simplying inserting
                cursor->keys[i] = key;
                cursor->ptr2TreeOrData.dataPtr[i] = dataPtr;
            }
           // cout << "Inserted successfully: " << key << endl;
        }
        else
        {

            // Splitting the node

            vector<uint64_t> virtualNode(cursor->keys);
            vector<IndexV> virtualDataNode(cursor->ptr2TreeOrData.dataPtr);

            // finding the probable place to insert the key
            int i = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();

            virtualNode.push_back(key);         // to create space
            virtualDataNode.push_back(dataPtr); // to create space

            Node *newLeaf = new Node;
            newLeaf->isLeaf = true;
            new (&newLeaf->ptr2TreeOrData.dataPtr) std::vector<IndexV>;

            // now, newLeaf->ptr2TreeOrData.ptr2Tree is the active member of the union
            // swapping the next ptr
            Node *temp = cursor->ptr2next;
            cursor->ptr2next = newLeaf;
            newLeaf->ptr2next = temp;

            // resizing and copying the keys & dataPtr to OldNode
            cursor->keys.resize((maxLeafNodeLimit) / 2 + 1); // check +1 or not while partitioning
            cursor->ptr2TreeOrData.dataPtr.reserve((maxLeafNodeLimit) / 2 + 1);
            for (int i = 0; i <= (maxLeafNodeLimit) / 2; i++)
            {
                cursor->keys[i] = virtualNode[i];
                cursor->ptr2TreeOrData.dataPtr[i] = virtualDataNode[i];
            }

            // Pushing new keys & dataPtr to NewNode
            for (int i = (maxLeafNodeLimit) / 2 + 1; i < virtualNode.size(); i++)
            {
                newLeaf->keys.push_back(virtualNode[i]);
                newLeaf->ptr2TreeOrData.dataPtr.push_back(virtualDataNode[i]);
            }

            if (cursor == root)
            {
                /*
                    If cursor is root node we create new node
                */

                Node *newRoot = new Node;
                newRoot->keys.push_back(newLeaf->keys[0]);
                new (&newRoot->ptr2TreeOrData.ptr2Tree) std::vector<Node *>;
                newRoot->ptr2TreeOrData.ptr2Tree.push_back(cursor);
                newRoot->ptr2TreeOrData.ptr2Tree.push_back(newLeaf);
                root = newRoot;
                cout << "Created new Root!" << endl;
            }
            else
            {
                // Insert new key in the parent
                insertInternal(newLeaf->keys[0], &parent, &newLeaf);
            }
        }
    }
}

IndexV BPTree::Get(const uint64_t& key)
{
    if (root == NULL)
    {
        std::cout << "NO Tuples Inserted yet" << endl;
        // std::throw "NO Tuples Inserted yet" ;
        // return;
    }
    else
    {
        Node *cursor = root;
        while (cursor->isLeaf == false)
        {
            /*
                upper_bound returns an iterator pointing to the first element in the range
                [first,last) which has a value greater than �val�.(Because we are storing the
                same value in the right node;(STL is doing Binary search at back end)
            */
            int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
            cursor = cursor->ptr2TreeOrData.ptr2Tree[idx]; // upper_bound takes care of all the edge cases
        }

        int idx = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin(); // Binary search
        try
        {
            /* code */
            if (idx == cursor->keys.size() || cursor->keys[idx] != key)
            {
                cout << "HUH!! Key NOT FOUND" << endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << 'Key NOT FOUND\n';
        }
        IndexV offset;
        offset = cursor->ptr2TreeOrData.dataPtr[idx];
        return offset;
    }
}

void BPTree::getNeighborsFromIndex(uint64_t VID,int& countFromIndex)
{
    //range query
    //构造起始和终止的 Key
    uint64_t endVID = VID + 1;
    uint64_t endKey = endVID << 32;
    uint64_t key = VID << 32;

     if (root == NULL)
    {
        cout << "NO Tuples Inserted yet" << endl;
        return;
    }
    else
    {
        Node *cursor = root;
        while (cursor->isLeaf == false)
        {

            int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
            cursor = cursor->ptr2TreeOrData.ptr2Tree[idx]; // upper_bound takes care of all the edge cases
        }

        int idx = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin(); // Binary search

        if (idx == cursor->keys.size() || cursor->keys[idx] != key)
        {
           // cout << "HUH!! Key NOT FOUND" << endl;
            return;
        }
        // print all neighbors
        idx++;
        if (idx == cursor->keys.size())
        {
            cursor = cursor->ptr2next;
            idx = 0;
        }
        while (cursor->keys[idx] < endKey)
        {

            //  uint32_t desVID = cursor->keys[idx] & 0x00001111;
           // uint64_t desVID = cursor->keys[idx] - key;
           countFromIndex ++;
           // std::cout << "  desVID is:\t" << desVID;
            idx++;
            if (idx == cursor->keys.size())
            {
                cursor = cursor->ptr2next;
                idx = 0;
            }
        }
    }

}





//return address of neighbors
 void BPTree::getNeighbors(uint64_t VID, vector<IndexV> &adjList){
      //range query
    //构造起始和终止的 Key
    uint64_t endVID = VID + 1;
    uint64_t endKey = endVID << 32;
    uint64_t key = VID << 32;

 if (root == NULL)
    {
        cout << "NO Tuples Inserted yet" << endl;
        return;
    }
    else
    {
        Node *cursor = root;
        while (cursor->isLeaf == false)
        {

            int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
            cursor = cursor->ptr2TreeOrData.ptr2Tree[idx]; // upper_bound takes care of all the edge cases
        }

        int idx = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin(); // Binary search

        if (idx == cursor->keys.size() || cursor->keys[idx] != key)
        {
           // cout << "HUH!! Key NOT FOUND" << endl;
            return;
        }
        // print all neighbors
        idx++;
        if (idx == cursor->keys.size())
        {
            cursor = cursor->ptr2next;
            idx = 0;
        }
        while (cursor->keys[idx] < endKey)
        {

            //  uint32_t desVID = cursor->keys[idx] & 0x00001111;
          //  uint32_t desVID = cursor->keys[idx] - key;
           
            idx++;
            if (idx == cursor->keys.size())
            {
                cursor = cursor->ptr2next;
                idx = 0;
            }
            IndexV offset = cursor->ptr2TreeOrData.dataPtr[idx];
            adjList.push_back(offset);
        }
    }
   

}