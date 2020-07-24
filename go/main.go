package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

type Channel struct {
	Name       string   `json:"name"`
	ProductIDs []string `json:"product_ids"`
}

type WsRequest struct {
	Type     string     `json:"type"`
	Channels []*Channel `json:"channels"`
}

type Answer struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Bid       string `json:"best_bid"`
	Ask       string `json:"best_ask"`
}

func main() {

	productIds := [3]string{
		"ETH-BTC",
		"BTC-USD",
		"BTC-EUR",
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(productIds))

	for i := 0; i < len(productIds); i++ {

		request, err := json.Marshal(&WsRequest{"subscribe", []*Channel{
			{"ticker", []string{productIds[i]}},
		}})

		if err != nil {
			log.Fatal(err)
		}

		go getData(wg, request)
	}

	wg.Wait()
}

func getData(wg *sync.WaitGroup, request []byte) {

	defer wg.Done()

	dialer := websocket.Dialer{
		Subprotocols:    []string{},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	conn, _, err := dialer.Dial("wss://ws-feed.pro.coinbase.com", nil)
	if err != nil {
		log.Fatal(err)
	}

	conn.WriteMessage(websocket.TextMessage, request)

	for {

		_, ms, err := conn.ReadMessage()
		if err != nil {
			log.Fatal(err)
		}

		setIntoTicks(ms)
	}

}

func setIntoTicks(ms []byte) {

	var answer Answer
	json.Unmarshal(ms, &answer)

	db, err := sql.Open("mysql", "docker:docker@tcp(db:3306)/test_db")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if answer.Type == "ticker" {

		bid, err := strconv.ParseFloat(answer.Bid, 64)
		if err != nil {
			log.Fatal(err)
		}

		ask, err := strconv.ParseFloat(answer.Ask, 64)
		if err != nil {
			log.Fatal(err)
		}

		result, err := db.Exec("insert into ticks (timestamp, symbol, bid, ask) values (?, ?, ?, ?)",
			makeTimestamp(), answer.ProductID, bid, ask)
		if err != nil {
			log.Fatal(err)
		}

		rowsAffected, err := result.RowsAffected()
		if rowsAffected == 1 {
			fmt.Println("Я сделяль запись для инстумента " + answer.ProductID)
		}
	}
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
