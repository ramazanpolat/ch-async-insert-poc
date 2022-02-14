package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const ddl = `
CREATE TABLE example (
	  Col1 UInt64
	, Col2 String
	, Col3 Array(UInt8)
	, Col4 DateTime
) Engine=MergeTree ORDER BY Col1
`

func main() {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"192.168.135.151:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "qwe123",
			},
			//Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)
	if err != nil {
		log.Fatal(err)
	}
	/*
		if err := conn.Exec(ctx, ddl); err != nil {
			log.Fatal(err)
		}*/

	var startTime = time.Now().UnixNano()

	for i := 0; i < 100000; i++ {
		var insert = fmt.Sprintf(`INSERT INTO example SELECT 
			%d, '%s', [1, 2, 3, 4, 5, 6, 7, 8, 9], now()`, i, "Golang SQL database driver")
		//fmt.Println(insert)
		err := conn.AsyncInsert(ctx, insert, false)
		//err := conn.Exec(ctx, insert, false)
		if err != nil {
			log.Fatal(err)
		}
	}

	var endTime = time.Now().UnixNano()

	fmt.Println("It took", endTime-startTime, "ns")
}
