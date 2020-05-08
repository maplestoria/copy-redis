# copy-redis

[![CircleCI](https://circleci.com/gh/maplestoria/copy-redis.svg?style=svg)](https://circleci.com/gh/maplestoria/copy-redis)

Redis数据同步工具，支持以多种模式进行Redis之间的数据近实时单向同步.

```bash
$ copy-redis -h
Usage: copy-redis [options]

Options:
    -s, --source 源Redis的URI, 格式:"redis://[:password@]host:port[/db]"
                        此Redis内的数据将复制到目的Redis中
    -t, --target 目的Redis的URI, URI格式同上

    -d, --discard-rdb   是否跳过整个RDB不进行复制. 默认为false, 复制完整的RDB
    -a, --aof           是否需要处理AOF. 默认为false, 当RDB复制完后程序将终止
        --sharding      是否sharding模式
        --cluster       是否cluster模式
    -l, --log 日志输出文件
                        默认输出至stdout
    -p, --batch-size 2500
                        发送至Redis的每一批命令的最大数量, 若<=0则不限制数量
    -i, --flush-interval 100
                        发送命令的最短间隔时间(毫秒)
    -h, --help          输出帮助信息
    -v, --version
```

## 下载地址

[点击前往下载](https://github.com/maplestoria/copy-redis/releases/latest)

## 使用

### 普通模式

假设现有两个Redis实例, 互相独立, 且均未设置密码(安全警告:heavy_exclamation_mark:):

- 127.0.0.1:6379
- 127.0.0.1:6479

现在使用`copy-redis`将`6379`端口Redis的数据同步至`6479`端口的Redis, 在shell中执行如下命令即可:

```bash
$ copy-redis -s redis://127.0.0.1:6379 -t redis://127.0.0.1:6479
```

### Sharding模式

为了应对Redis数据持续增长带来的压力, 常见的解决方案便是**分片**. 将一份数据分成多份, 分别存放在不同的Redis中, 使用时再按照相应的规则, 从对应的Redis中获取即可.

在`copy-redis`中, 分片使用的方案以及实际key的分布都与[jedis](https://github.com/xetorthio/jedis)在**默认配置**下的情况相同.

> 即采用`MurmurHash`将各个Shard按照**权重1**以`SHARD-{shard index}-NODE-{node index}`的hash值为key, 生成160个node.

> 获取某个key的数据时, 根据key的hash值取所有的node中大于等于key hash值的第一个node. 若未找到满足条件的node, 则取所有node中的第一个node

> 但是, [jedis](https://github.com/xetorthio/jedis)支持MD5, 设置各个Shard的名称和权重, 以及key pattern, 这几项配置在`copy-redis`中**均不支持**. 故上条所述, 与[jedis](https://github.com/xetorthio/jedis)在**默认配置**下的实际数据分布相同.

若要使用Sharding模式, 只需指定多个target地址, 并指定`--sharding`参数即可:

```bash
$ copy-redis -s redis://127.0.0.1:6379 \
             -t redis://127.0.0.1:6479 -t redis://127.0.0.1:6579 \ 
             --sharding
```

### Cluster模式

Cluster模式的使用与Sharding模式类似, 指定一个或多个Redis Cluster的节点地址, 并指定`--cluster`参数即可:

```bash
$ copy-redis -s redis://127.0.0.1:6379 -t redis://127.0.0.1:6479 --cluster
```

### Note

- 只有Cluster模式目前不支持pipeline, 所以写入效率较低

- 暂不支持Redis 6的新特性

- 以下命令/功能在Sharding/Cluster模式下不支持:
    - BITOP
    - EVAL
    - EVALSHA
    - MULTI & EXEC
    - PFMERGE
    - SDIFFSTORE
    - SINTERSTORE
    - SUNIONSTORE
    - ZUNIONSTORE
    - ZINTERSTORE
    - Pub/Sub
    
- 程序在正常退出时, 会在工作目录下创建`.copy-redis`文件夹, 里面存放了replication相关的id和offset.
 在程序启动时, 会从`.copy-redis`文件夹中获取之前保存的信息, 若成功获取到之前保存的数据, 则以此去请求`partial replication`,
 这样可以继续之前的replication进度, 以免触发`full replication`. 