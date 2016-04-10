package main

import (
	"io"
	"net/http"
	"os"
	"runtime"

	"github.com/pmalek/nlog"

	"github.com/gorilla/mux"
)

var log *nlog.Logger

func init() {
	formatter := nlog.NewTextFormatter(true, true)
	formatter.TimestampFormat = "2006-04-02 15:04:05.000000 -0700"
	log = nlog.NewLogger(nlog.InfoLevel, formatter)

	file, err := os.OpenFile("log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file", nlog.Data{"err": err})
	}
	multi := io.MultiWriter(file, os.Stdout)
	log.SetOut(multi)

	/*
	 *log.Formatter = &prefixed.TextFormatter{
	 *  TimestampFormat: "2006 Jan 2 15:04:05.000000 -0700",
	 *}
	 */

	/*
	 *log.Formatter = &logrus.JSONFormatter{
	 *  TimestampFormat: time.RFC3339Nano,
	 *}
	 */
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	h := NewHandler()
	r := mux.NewRouter()
	r.HandleFunc("/get", h.Get).Methods(http.MethodGet)
	r.HandleFunc("/set", h.Set).Methods(http.MethodPost)
	r.HandleFunc("/remove", h.Remove).Methods(http.MethodDelete)
	r.HandleFunc("/list", h.List).Methods(http.MethodGet)

	log.Infof("Starting server...")
	http.ListenAndServe(":3000", r)
}
