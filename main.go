package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCPDatastore structure
type GCPDatastore struct {
	DBClient *datastore.Client // 8 bytes
}

// ScheduledEntity is the basic ScheduledEntity model
type ScheduledEntity struct {
	Timetorun int `datastore:"timetorun"`
}

var client *datastore.Client

var mutex = &sync.Mutex{}
var results int
var counter int

func main() {
	killMain := make(chan bool)
	jobs := make(chan []*datastore.Key)

	ctx := context.Background()
	client, _ = datastore.NewClient(ctx, "project", option.WithCredentialsFile("./serviceacc.json"))

	go QueueKeys(jobs, "SheduledBeta")

	for i := 0; i < 10; i++ {
		go worker(i, jobs)
	}

	ticker := time.NewTicker(5 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("Completed: ", results)
				fmt.Printf("Counter: %v \n", counter)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	<-killMain
}

// QueueKeys queues keys
func QueueKeys(jobs chan<- []*datastore.Key, kind string) {
	ctx := context.Background()

	qry := datastore.NewQuery(kind)
	it := client.Run(ctx, qry)

	var keyStore []*datastore.Key
	for {
		var entity ScheduledEntity
		key, err := it.Next(&entity)
		if err == iterator.Done {
			jobs <- keyStore
			fmt.Println("QUEUE DONE")
			break
		} else if err != nil {
			log.Fatal(err)
		}

		keyStore = append(keyStore, key)
		counter++
		if len(keyStore) >= 500 {
			jobs <- keyStore
			keyStore = []*datastore.Key{}
		}
	}
}

func worker(id int, jobs <-chan []*datastore.Key) {
	ctx := context.Background()

	for chunk := range jobs {
		// Delete stuff here
		err := client.DeleteMulti(ctx, chunk)
		if err != nil {
			fmt.Println(err)
		}
		results += len(chunk)
		fmt.Printf("Worker ID [ %v ] completed jobs => %v\n", id, len(chunk))
	}
}
