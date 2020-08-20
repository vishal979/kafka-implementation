package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
	"github.com/pkg/profile"
)

type producer struct {
	p     *kafka.Producer
	topic string
	body  []byte
}

var kafkaAlive bool
var prodChan chan producer
var prodChanFile chan producer

func main() {
	defer profile.Start().Stop()
	cpus := runtime.NumCPU()
	if cpus == 1 {
		cpus = 1
	} else {
		cpus = cpus - 1
	}
	runtime.GOMAXPROCS(cpus)
	f, _ := os.OpenFile("server.logs", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	log.SetOutput(f)
	var p producer

	var err error
	p.p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "172.16.10.202", "debug": "broker"})
	if err != nil {
		panic(err)
	}
	prodChan = make(chan producer)
	prodChanFile = make(chan producer)

	defer p.p.Close()

	go p.deliveryNotifications()
	go p.healthChecker()

	r := mux.NewRouter()
	r.HandleFunc("/{topic}", p.producerHandler).Methods("POST")
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	log.Println("starting server")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := http.ListenAndServe(":8080", r); err != nil {
			log.Println("server crashed ", err)
			wg.Done()
		}
	}()
	fmt.Println("server running successfully")
	wg.Wait()
	p.p.Flush(10)
}

//listen to delivery notifications
func (p producer) deliveryNotifications() {
	for e := range p.p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				f, err := os.OpenFile(fmt.Sprintf("%v.logs", p.topic), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
				defer f.Close()
				if err != nil {
					log.Println("unable to write the log ")
					log.Println(string(p.body))
					return
				}
				f.Write(p.body)
				f.Write([]byte("\n"))
			}
		}
	}
}

func (p producer) healthChecker() {
	for {
		var err error
		var topictest string = "kafkatest"
		_, err = p.p.GetMetadata(&topictest, false, 50)
		if err != nil {
			kafkaAlive = false
		}
		kafkaAlive = true
	}
}

func writeFile(prodChanFile chan producer) {
	p := <-prodChanFile
	f, err := os.OpenFile(fmt.Sprintf("%v.logs", p.topic), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()
	if err != nil {
		log.Println("unable to write the log ")
		log.Println(string(p.body))
		return
	}
	f.Write(p.body)
	f.Write([]byte("\n"))
	return
}

func (p *producer) producerHandler(w http.ResponseWriter, r *http.Request) {
	p.topic = mux.Vars(r)["topic"]
	var err error
	p.body, err = ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("error while reading message body ", err)
	}

	if !kafkaAlive {
		f, err := os.OpenFile(fmt.Sprintf("%v.logs", p.topic), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		defer f.Close()
		if err != nil {
			log.Println("unable to write the log ")
			log.Println(string(p.body))
			return
		}
		f.Write(p.body)
		return
	}

	err = p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          p.body,
	}, nil)
	if err != nil {
		log.Println("error producing")
	}
	return
}
