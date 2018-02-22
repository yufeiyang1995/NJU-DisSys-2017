package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"time"
)

func main() {
	ipPort := flag.String("ipPort", "127.0.0.1:8080", "http listen ip&port")
	flag.Parse()
	fmt.Println("ip_port:", *ipPort)
	client, err := rpc.DialHTTP("tcp", *ipPort)
	if err != nil {
		fmt.Println("链接rpc服务器失败:", err)
	}
	var reply int64

	var user string
	var password string
	fmt.Println("输入用户名：")
	fmt.Scanln(&user)
	fmt.Println("输入密码：")
	fmt.Scanln(&password)

	user = user + "," + password
	//fmt.Println(user)
	var oldTime = time.Now().UnixNano()
	err = client.Call("Watcher.GetInfo", user, &reply)
	if err != nil {
		fmt.Println("调用远程服务失败", err)
	}
	if reply > 0 {
		var newTime = time.Now().UnixNano()
		var timeDiff = newTime - oldTime
		reply = reply + timeDiff/2
		//fmt.Println("diff：", oldTime, ",", newTime, ",", timeDiff, ",", reply)
		var replyTime = time.Unix(reply/1e9, 0).String()
		fmt.Println("远程服务返回时间：", replyTime)
	} else {
		fmt.Println("权限验证失败")
	}
}
