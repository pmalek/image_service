package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"github.com/pmalek/image_service/notifier"
	"github.com/pmalek/image_service/task"
	"github.com/pmalek/nlog"
)

var (
	masterLocation       string
	storageLocation      string
	keyValueStoreAddress string
	log                  *nlog.Logger
)

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
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Error: Too few arguments.")
		return
	}

	//defer profile.Start().Stop()

	keyValueStoreAddress = os.Args[1]

	if ok := getSlavesAddressesFromDatabase(); ok == false {
		log.Errorf("Couldn't get master and/or storage address from key value store")
		return
	}

	notif := new(notifier.Notifier)
	rpc.Register(notif)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatalf("listen error:", err)
	}
	go http.Serve(l, nil)

	f := func() {
		neg_task := task.Task{-1, -1}

		myTask, err := getNewTask(masterLocation)
		if err != nil || myTask == neg_task {
			return
		}
		myImage, err := getImageFromStorage(storageLocation, myTask)
		if err != nil {
			return
		}

		myImage = doWorkOnImage(myImage)

		err = sendImageToStorage(storageLocation, myTask, myImage)
		if err != nil {
			return
		}

		err = registerFinishedTask(masterLocation, myTask)
		if err != nil {
			return
		}
	}

	log.Infof("Launching workers main loop...")
	for { // worker main loop
		select {
		case id := <-notifier.Todo:
			log.Infof("Task to be done... %v", id)
			go f()
		case <-time.After(1000 * time.Millisecond):
			log.Debugf("Hmmm")
		}
	}
}

func getNewTask(masterAddress string) (task.Task, error) {
	response, err := http.Post("http://"+masterAddress+"/getNewTask", "text/plain", nil)
	if err != nil {
		log.Error("", nlog.Data{"err": err})
		return task.Task{-1, -1}, errors.New("Error getting new task")
	} else if response.StatusCode == http.StatusNoContent {
		log.Infof("No task to take...")
		return task.Task{-1, -1}, nil
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Error("", nlog.Data{"err": err})
		return task.Task{-1, -1}, err
	}

	log.Info("", nlog.Data{"data": string(data)})
	log.Info("", nlog.Data{"response.StatusCode": response.StatusCode})

	myTask := task.Task{}
	err = json.Unmarshal(data, &myTask)
	if err != nil {
		log.Error("", nlog.Data{"err": err})
		return task.Task{-1, -1}, err
	}

	return myTask, nil
}

func getImageFromStorage(storageAddress string, myTask task.Task) (image.Image, error) {
	response, err := http.Get("http://" + storageAddress + "/getImage?state=working&id=" + strconv.Itoa(myTask.Id))
	if err != nil {
		log.Error("", nlog.Data{"err": err})
		return nil, err
	} else if response.StatusCode == http.StatusNoContent {
		log.Errorf("http.StatusNoContent")
		return nil, errors.New("http.StatusNoContent")
	} else if response.StatusCode != http.StatusOK {
		log.Error("", nlog.Data{"response.StatusCode": response.StatusCode})
		return nil, nil
	}

	myImage, err := png.Decode(response.Body)
	if err != nil {
		log.Error("Problem decoding the image", nlog.Data{"err": err})
		return nil, err
	}

	return myImage, nil
}

func doWorkOnImage(myImage image.Image) image.Image {
	if myImage == nil {
		log.Errorf("nil Image")
		return nil
	}

	myCanvas := image.NewRGBA(myImage.Bounds())

	for i := 0; i < myCanvas.Rect.Max.X; i++ {
		for j := 0; j < myCanvas.Rect.Max.Y; j++ {
			r, g, b, _ := myImage.At(i, j).RGBA()
			myColor := new(color.RGBA)
			myColor.R = uint8(g)
			myColor.G = uint8(r)
			myColor.B = uint8(b)
			myColor.A = uint8(255)
			myCanvas.Set(i, j, myColor)
		}
	}

	return myCanvas.SubImage(myImage.Bounds())
}

func sendImageToStorage(storageAddress string, myTask task.Task, myImage image.Image) error {
	if myImage == nil {
		log.Errorf("nil Image")
		return nil
	}

	data := []byte{}
	buffer := bytes.NewBuffer(data)
	err := png.Encode(buffer, myImage)
	if err != nil {
		return err
	}
	response, err := http.Post("http://"+storageAddress+"/sendImage?state=finished&id="+strconv.Itoa(myTask.Id), "image/png", buffer)
	if err != nil || response.StatusCode != http.StatusOK {
		return err
	}

	return nil
}

func registerFinishedTask(masterAddress string, myTask task.Task) error {
	response, err := http.Post("http://"+masterAddress+"/registerTaskFinished?id="+strconv.Itoa(myTask.Id), "test/plain", nil)
	if err != nil || response.StatusCode != http.StatusOK {
		return err
	}

	return nil
}

// Helpers...

func getSlavesAddressesFromDatabase() bool {
	masterLocation, _ = retrieveFromKeyValueStore("masterAddress")
	storageLocation, _ = retrieveFromKeyValueStore("storageAddress")
	// TODO check errors from retrieveFromKeyValueStore ?

	return (len(masterLocation) != 0) && (len(storageLocation) != 0)
}

func retrieveFromKeyValueStore(key string) (string, error) {
	response, err := http.Get("http://" + keyValueStoreAddress + "/get?key=" + key)
	if err != nil {
		err_str := "Couldn't get " + key + " from key value store"
		log.Error(err_str, nlog.Data{"err": err})
		return "", errors.New(err_str)
	} else if response.StatusCode != http.StatusOK {
		err_str := "Received HTTP status " + strconv.Itoa(response.StatusCode)
		log.Error(err_str, nlog.Data{"response.Body": response.Body})
		return "", errors.New(err_str)
	}

	if data, err := ioutil.ReadAll(response.Body); err != nil {
		err_str := "Couldn't read data from response.Body"
		log.Error(err_str, nlog.Data{"err": err, "data": data})
		return "", errors.New(err_str)
	} else {
		ret := string(data)
		if len(ret) == 0 {
			err_str := "Zero length return value"
			log.Errorf(err_str)
			return "", errors.New(err_str)
		}

		return ret, nil
	}
}
