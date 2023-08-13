在测试中学习，本仓库用etcd raft搭建了一个最简单的集群，不支持snapshot和持久化，mock了网络调用

对象定义：

- process: 程序的入口
- application: 代表业务逻辑，比如一个kv
- storage: mock出来的存储，代码copy自raft.MemoryStorage
- net: mock出来的网络层，也就是transport

