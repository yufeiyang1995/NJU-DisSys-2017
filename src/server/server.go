package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

type Watcher int

var user map[string]string

func (w *Watcher) GetInfo(arg string, result *int64) error {
	fmt.Println("user:", arg)
	var s = strings.Split(arg, ",")
	var userName = s[0]
	var password = s[1]
	if user[userName] == password {
		*result = time.Now().UnixNano()
		fmt.Println("rpc:", *result)
	} else {
		*result = -1
	}
	return nil
}

func verifyAuthorize() {

}

func printTime() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		time := <-ticker.C
		fmt.Println(time.String())
	}
}

func listenPort(port string) {
	watcher := new(Watcher)
	rpc.Register(watcher)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("监听失败，端口可能已经被占用")
	}
	fmt.Println("正在监听", port, "端口")
	http.Serve(l, nil)
}

func main() {

	port := flag.String("port", ":8080", "http listen port")
	flag.Parse()
	fmt.Println("port:", *port)

	user = make(map[string]string)
	user["yyf"] = "111"
	user["jrh"] = "222"
	user["gzf"] = "333"
	go printTime()
	listenPort(*port)
}
