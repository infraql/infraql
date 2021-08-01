package srv

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"infraql/internal/iql/driver"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/handler"

	lrucache "vitess.io/vitess/go/cache"
)

func handleConnection(c net.Conn, runtimeCtx dto.RuntimeCtx, lruCache *lrucache.LRUCache) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')

		if err != nil {
			fmt.Println(err)
			return
		}

		temp := strings.TrimSpace(string(netData))
		if temp == "STOP" {
			break
		}
		sqlEng, err := entryutil.BuildSQLEngine(runtimeCtx)
		if err != nil {
			fmt.Println(err)
			return
		}
		handlerContext, _ := handler.GetHandlerCtx(netData, runtimeCtx, lruCache, sqlEng)
		handlerContext.Outfile = c
		handlerContext.OutErrFile = c
		if handlerContext.RuntimeContext.DryRunFlag {
			driver.ProcessDryRun(handlerContext)
			continue
		}
		driver.ProcessQuery(handlerContext)
	}
	c.Close()
}

func Serve(portNo int, runtimeCtx dto.RuntimeCtx, lruCache *lrucache.LRUCache) {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide a port number!")
		// return
	}

	portStr := strconv.Itoa(portNo)

	l, err := net.Listen("tcp4", ":"+portStr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()
	rand.Seed(time.Now().Unix())

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c, runtimeCtx, lruCache)
	}
}
