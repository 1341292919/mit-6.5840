package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"time"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.

// Get 获取指定键的当前值和版本号。
// 如果键不存在则返回 ErrNoKey。
// 遇到其他错误时会无限重试。
//
// RPC调用示例：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// 注意：
// - args 和 reply 的类型必须与RPC处理函数的参数类型匹配
// - reply 必须作为指针传递

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

		// RPC成功且键存在
		if ok && reply.Err == rpc.OK {
			return reply.Value, reply.Version, rpc.OK
		}

		// RPC成功但键不存在
		if ok && reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}

		// 情况3：RPC
		if !ok {
			DPrintf("Get RPC failed, retrying...")
			time.Sleep(100 * time.Millisecond) // 避免忙等待
			continue
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.

// Put 仅在请求中的版本号与服务器端键的版本号匹配时更新键值。
// 版本号不匹配时服务器应返回 ErrVersion。
//
// 错误处理规则：
// - 如果首次RPC就收到 ErrVersion，Put 应返回 ErrVersion（确定未执行）
// - 如果重试RPC收到 ErrVersion，则必须返回 ErrMaybe（因为可能已执行但响应丢失）
//
// RPC调用示例：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// 注意：
// - args 和 reply 的类型必须与RPC处理函数的参数类型匹配
// - reply 必须作为指针传递
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	isFirst := true
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			case rpc.ErrVersion:
				if isFirst {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		} else {
			DPrintf("Put RPC failed, retrying...")
			time.Sleep(100 * time.Millisecond)
			isFirst = false // 后续尝试不再是首次
			continue
		}
	}
}
