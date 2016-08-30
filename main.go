package main

import (
	"net/http"
	"io/ioutil"
	"regexp"
	"strings"
	"io"
	"fmt"
	"archive/zip"
	"os"
	"path/filepath"
	"gopkg.in/redis.v4"
	"gopkg.in/cheggaaa/pb.v1"
	"sync/atomic"
	"sync"
	"strconv"
)

const DefaultLink = "http://bitly.com/nuvi-plz"
const DefaultParallelDownloads = 10
const DefaultParallelProcessing = 10

type  DownloadedFile struct {
	URL       string
	LocalPath string
}

var HTTPListingRE = regexp.MustCompile(`(?i)<tr><td>.*?href="([^"]*)`)
var client *redis.Client
var bar *pb.ProgressBar
var stat = struct {
	Total      uint64
	Skipped    uint64
	Downloaded uint64
	Unzipped   uint64
	XMLS       uint64
	Inserted   uint64
	Duplicates uint64
}{}
var processingWait sync.WaitGroup

func unzip(zipPath string) (string, error) {
	dest := filepath.Dir(zipPath) + "/" + filepath.Base(zipPath) + "_unzip"

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return "", err
		}
	}

	return dest, nil
}

func downloadPool(filesToDownload<-chan string, filesToProcess chan <- DownloadedFile) {
	for fileURL := range filesToDownload {
		path, err := downloadAndGetPath(fileURL)
		if err != nil {
			processingWait.Done()
			fmt.Println(err)
		} else {
			atomic.AddUint64(&stat.Downloaded, 1)
			filesToProcess <- DownloadedFile{fileURL, path}
		}
	}
}

func processingPool(filesToProcess<-chan DownloadedFile) {
	for filePath := range filesToProcess {
		err := processFile(filePath)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func downloadAndGetPath(fileURL string) (string, error) {
	response, err := http.Get(fileURL)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()
	file, err := ioutil.TempFile("", "")

	if err != nil {
		return "", err
	}

	_, err = io.Copy(file, response.Body)
	if err != nil {
		return "", err
	}
	file.Close()

	return file.Name(), nil
}

func listHTTPDirFiles(dirURL string) ([]string, error) {
	req, err := http.NewRequest("GET", dirURL, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	var body []byte
	body, err = ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	trs := HTTPListingRE.FindAllStringSubmatch(string(body), -1)

	var urls []string

	respLocation := resp.Request.URL.String()

	if !strings.HasSuffix(respLocation, "/") {
		respLocation = respLocation + "/"
	}

	if err != nil {
		return nil, err
	}
	for _, tr := range trs {
		if !strings.HasPrefix(tr[1], "/") && !strings.HasPrefix(tr[1], ".") {
			urls = append(urls, resp.Request.URL.String() + tr[1])
		}
	}
	return urls, nil
}

func processFile(file DownloadedFile) error {
	unzippedDir, err := unzip(file.LocalPath)
	if err != nil {
		return err
	}

	os.Remove(file.LocalPath)
	atomic.AddUint64(&stat.Unzipped, 1)

	files, _ := ioutil.ReadDir(unzippedDir)
	for _, fileInfo := range files {
		atomic.AddUint64(&stat.XMLS, 1)
		b, err := ioutil.ReadFile(unzippedDir + "/" + fileInfo.Name())
		if err != nil {
			return (err)
		}
		// hash := string(uint64ToBytes(xxhash.Checksum64(b)))
		// file name of file is MD5 of its content

		fparts := strings.Split(fileInfo.Name(), ".")
		hash := fparts[0]
		os.Remove(unzippedDir + "/" + fileInfo.Name())

		if client.HExists("NEWS_XML_KEY", hash).Val() {
			atomic.AddUint64(&stat.Duplicates, 1)
			continue
		}

		err = client.RPush("NEWS_XML", b).Err()
		if err != nil {
			return err
		}
		atomic.AddUint64(&stat.Inserted, 1)
		client.HSet("NEWS_XML_KEY", hash, "")
	}

	client.HSet("NEWS_ZIP_URL", file.URL, "")

	os.RemoveAll(unzippedDir)
	bar.Increment()
	processingWait.Done()

	return nil
}

func main() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	})

	_, err := client.Ping().Result()

	if err != nil {
		panic("REDIS ERROR: " + err.Error())
	}

	link := os.Getenv("NUVI_LIST_LINK")

	if link == "" {
		link = DefaultLink
	}

	parallelDownloads, _ := strconv.Atoi(os.Getenv("NUVI_POOL_DOWNLOADS"))

	if parallelDownloads == 0 {
		parallelDownloads = DefaultParallelDownloads
	}

	parallelProcessing, _ := strconv.Atoi(os.Getenv("NUVI_POOL_PROCESSING"))

	if parallelProcessing == 0 {
		parallelProcessing = DefaultParallelProcessing
	}

	urls, err := listHTTPDirFiles(link)

	if err != nil {
		fmt.Println(err)
	}
	filesToDownload := make(chan string, len(urls))
	filesToProcess := make(chan DownloadedFile, len(urls))
	bar = pb.New(len(urls)).SetMaxWidth(100)
	bar.Start()

	for i := 0; i < parallelDownloads; i++ {
		go downloadPool(filesToDownload, filesToProcess)
	}
	for i := 0; i < parallelProcessing; i++ {
		go processingPool(filesToProcess)
	}

	for _, url := range urls {
		stat.Total++
		if !client.HExists("NEWS_ZIP_URL", url).Val() {
			processingWait.Add(1)
			filesToDownload <- url
		} else {
			stat.Skipped++
			bar.Increment()
		}
	}

	close(filesToDownload)
	processingWait.Wait()

	bar.Finish()
	fmt.Printf("%+v\n", stat)
}