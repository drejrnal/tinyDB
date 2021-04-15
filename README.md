# CMU15445数据库系统设计

## Buffer pool management
### Static hashing
- 预先知道存储元素的个数；分配一张空间足够的表，存储元素；若空间不够，则需要扩展空间，并重新分配表中所有元素
- 可以通过链式哈希，存储冲突的元素，虽然避免了所有元素的重新洗牌，但容易造成链表无限扩张的问题。
### Dynamic hashing
- 同链式哈希不同之处在于，动态哈希在buckets满的情况下，split the bucket instead of grow the linked list infinitely.  虽然在split bucket的过程中，会涉及元素的再分配，但再分配的数量相对static hash较少，它的变化是localized. 
- 采用可扩展哈希数据结构建立页表，key是page_id(uint32_t) ，value是 class Page *

## B+ 树数据结构设计及并发控制
- B+ tree leaf page内 key与value个数相同；而B+ tree internal node 内， key比value个数少1。
- 并发B+ tree 数据结构中，当执行插入/删除操作时，首先获取根节点的写锁，在向下遍历的过程中，每到达一个子节点，检查该子节点是否“安全”(“安全”即指：插入操作中，该子节点未满；删除操作中，该子节点至少有一半以上元素)；一旦认定安全，则会释放所有（注意是所有）父节点的写锁，否则，父节点继续持有写锁。
- 并发程序中，在删除操作遇到一个bug是：判断该bucket是否为空同在该bucket执行删除操作应该是一个原子操作，类似于原语compare and swap

### 两阶段锁协议
- 两阶段锁协议不能保证避免死锁，在growing phase阶段，还可进行锁的升级；在shrinking phase阶段，可进行锁的降级
