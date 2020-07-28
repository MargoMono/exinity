package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

const (
	websocketURL = "wss://ws-feed.pro.coinbase.com"
	dbConnection = "docker:docker@tcp(db:3306)/test_db"
)

type Service struct {
	ws       *websocket.Conn
	db       *sql.DB
	wsAnswer chan *Answer
}

func NewConnection(websocketURL string, dbConnection string) (*Service, error) {

	dialer := websocket.Dialer{
		Subprotocols:    []string{},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	ws, _, err := dialer.Dial(websocketURL, nil)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	db, err := sql.Open("mysql", dbConnection)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &Service{ws, db, make(chan *Answer)}, nil
}

type Channel struct {
	Name       string   `json:"name"`
	ProductIDs []string `json:"product_ids"`
}

type WsRequest struct {
	Type     string     `json:"type"`
	Channels []*Channel `json:"channels"`
}

func (service *Service) subscribe(currencyPairs []string) error {

	request, err := json.Marshal(&WsRequest{"subscribe", []*Channel{
		{"ticker", currencyPairs},
	}})

	if err != nil {
		log.Fatal(err)
	}

	return service.ws.WriteMessage(websocket.TextMessage, request)
}

type Answer struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Bid       string `json:"best_bid"`
	Ask       string `json:"best_ask"`
}

func (service *Service) setIntoTicks() {
	for {
		select {
		case data := <-service.wsAnswer:
			if data.Type == "ticker" {

				bid, err := strconv.ParseFloat(data.Bid, 64)

				if err != nil {
					log.Fatal(err)
				}

				ask, err := strconv.ParseFloat(data.Ask, 64)

				if err != nil {
					log.Fatal(err)
				}

				result, err := service.db.Exec("insert into ticks (timestamp, symbol, bid, ask) values (?, ?, ?, ?)",
					makeTimestamp(), data.ProductID, bid, ask)

				if err != nil {
					log.Fatal(err)
				}

				rowsAffected, err := result.RowsAffected()

				if rowsAffected == 1 {
					fmt.Println("Я сделяль запись для инстумента " + data.ProductID)
				}
			}
		}
	}
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func main() {
	currencPairs := []string{
		"ETH-BTC",
		"BTC-USD",
		"BTC-EUR",
	}

	service, err := NewConnection(websocketURL, dbConnection)

	if err != nil {
		log.Fatal(err)
	}

	service.subscribe(currencPairs)
	go service.setIntoTicks()

	defer func() {
		service.ws.Close()
		service.db.Close()
	}()

	for {
		_, ms, err := service.ws.ReadMessage()

		if err != nil {
			log.Fatal(err)
		}

		var answer Answer

		if err := json.Unmarshal(ms, &answer); err != nil {
			log.Fatal(err)
		}

		service.wsAnswer <- &answer
	}
}
