package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/bruhugo/protobuf_sstable"
)

func main() {
	args := os.Args
	if len(args) != 2 {
		panic("you should use this program like: ./<COMMAND NAME> <PORT>")
	}

	db, err := protobuf_sstable.NewDatabase()
	if err != nil {
		panic(err)
	}
	log.Print("database started")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /{key}", PostHandler(db))
	mux.HandleFunc("GET /{key}", GetHandler(db))

	middleware := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("PATH: %s", r.URL.Path)
		mux.ServeHTTP(w, r)
	})

	server := http.Server{
		Addr:         fmt.Sprintf(":%s", args[1]),
		Handler:      middleware,
		IdleTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}

	log.Print("server ready")
	err = server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func PostHandler(db *protobuf_sstable.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 1024)
		size, err := r.Body.Read(b)
		if err != nil && err != io.EOF {
			log.Printf("error: %s", err)
			w.WriteHeader(500)
			return
		}
		defer r.Body.Close()
		b = b[:size]

		key := r.PathValue("key")
		log.Printf("POST key: %s value: %s", key, string(b))
		if key == "" {
			w.WriteHeader(400)
			return
		}

		db.Append(key, string(b))
	}
}

func GetHandler(db *protobuf_sstable.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			w.WriteHeader(400)
			return
		}

		value, ok := db.Get(key)
		if !ok {
			w.WriteHeader(404)
			return
		}

		log.Printf("GET key: %s value: %s", key, value)

		_, err := w.Write([]byte(value))
		if err != nil {
			log.Printf("error: %s", err)
			w.WriteHeader(500)
			return
		}
	}
}
