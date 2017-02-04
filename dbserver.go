package main

import (
	"fmt"
	"net"
	"net/http"
	//"os"

	"golang.org/x/net/netutil"

	"./tinydatabase"
)

func main() {
	db, err := tinydatabase.LoadDatabaseList("webdb", "json")
	if err != nil {
		db, err = tinydatabase.NewDatabaseList("webdb", "json")
		if err != nil {
			fmt.Printf("ERROR:%s", err)
			return
		}
	}

	webIf := tinydatabase.WebIF{}
	webIf.Prefix = "/v1/"
	webIf.Databases = db
	tinydatabase.AddHandler(webIf)

	port := ":8000" //os.Args[1] //":80"
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("ERROR:%s", err)
		return
	}
	limit_listener := netutil.LimitListener(listener, 1)
	http_config := &http.Server{
		Addr: port,
	}

	defer limit_listener.Close()
	err = http_config.Serve(limit_listener)
	if err != nil {
		fmt.Printf("ERROR:%s", err)
		return
	}
}
