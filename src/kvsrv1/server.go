package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KeyValue struct {
	Key     string
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	mu   sync.Mutex
	data map[string]*KeyValue
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	return &KVServer{
		data: make(map[string]*KeyValue), // 初始化空map
	}
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
//
//	Get 处理获取键值的RPC请求：
//
// - 如果键存在，返回对应的值和版本号
// - 如果键不存在，返回ErrNoKey错误
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if item, exists := kv.data[args.Key]; exists {
		reply.Value = item.Value
		reply.Version = item.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.

// Put 处理更新键值的RPC请求：

// - 当客户端版本号与服务器端键的版本号匹配时更新值
// - 版本号不匹配时返回ErrVersion错误
// - 对于不存在的键：
//   - 如果客户端版本号为0则创建新键值对
//   - 否则返回ErrNoKey错误
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	items, exists := kv.data[args.Key]
	if !exists {
		if args.Version == 0 {
			kv.data[args.Key] = &KeyValue{
				Key:     args.Key,
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
			return
		}
		reply.Err = rpc.ErrNoKey
		return
	} else {
		if args.Version != items.Version {
			reply.Err = rpc.ErrVersion
			return
		}
		kv.data[args.Key].Value = args.Value
		kv.data[args.Key].Version = args.Version + 1
		reply.Err = rpc.OK
		return
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
