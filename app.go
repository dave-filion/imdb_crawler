package main

import (
	"log"
	"github.com/PuerkitoBio/goquery"
	"strings"
	"container/list"
	"encoding/csv"
	"os"
	url2 "net/url"
	"encoding/json"
	"io/ioutil"
	"github.com/jmoiron/sqlx"
	_"github.com/lib/pq"
	"time"
)

var MaxInserts = 100

func makeUrl(host string, path string) string {
	return host + path
}

type Film struct {
	Id          int       `db:"id"`
	FilmName        string    `db:"film_name"`
	YearReleased   string    `db:"year_released"`
	URL         string    `db:"url"`
	RunningTime string    `db:"running_time"`
	CreatedAt   time.Time `db:"created_at"`
}

func alreadyVisited(url string, visited *map[string]Film) bool {
	for k, _ := range *visited {
		if k == url {
			return true
		}
	}
	return false
}

type LastRunLinksData struct {
	Links []string `json:"links"`
}

func connectToDb() *sqlx.DB {
	db, err := sqlx.Connect("postgres", "dbname=immb sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func InsertFilm(film Film, db *sqlx.DB) {
	now := time.Now()
	film.CreatedAt = now
	log.Println("inserting: ", film)
	_, err := db.NamedExec(`INSERT INTO films (film_name, year_released, url, running_time, created_at) 
		VALUES (:film_name, :year_released, :url, :running_time, :created_at)`, &film)
	if err != nil {
		log.Fatal("error inserting film", err)
	}
}

func CreateFilmTable(db *sqlx.DB) {
	_, err := db.Exec(`CREATE TABLE films 
							(id serial, 
							film_name text, 
							year_released text, 
							url text, 
							running_time text, 
							created_at timestamp)`)
	if err != nil {
		log.Println("Table already exists, continuing")
	}
}

func 

func main() {
	db := connectToDb()
	CreateFilmTable(db)

	log.Println("Starting crawler")
	host := "http://www.imdb.com"

	defaultUrl := "http://www.imdb.com/title/tt0979435"

	// open last links json file
	data := LastRunLinksData{}
	fp1, err := os.OpenFile("last_run_links.json", os.O_CREATE | os.O_RDWR | os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("could not open last_run_links json file", err)
	}
	defer fp1.Close()
	bytes, err := ioutil.ReadAll(fp1)
	if err != nil {
		log.Fatal("error reading json file", err)
	}
	if len(bytes) > 0 {
		err := json.Unmarshal(bytes, &data)
		if err != nil {
			log.Fatal("error unmarshaling json", err)
		}
	}
	// append last links to list if they exist
	links := list.New()
	if len(data.Links) > 0 {
		for _, v := range data.Links {
			links.PushBack(v)
		}
	} else {
		links.PushBack(defaultUrl)
	}
	fp1.Close()

	log.Println(data.Links)

	// open csv to write
	fp, err := os.OpenFile("movie_output.csv", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("error opening file", err)
	}
	defer fp.Close()

	csvReader := csv.NewReader(fp)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("error reading save file", err)
	}

	visitedMap := map[string]Film{}
	if len(records) == 0 {
		log.Println("No existing records, starting new file")
	} else {
		log.Println("Loading existing records file")
		for _, rec := range records {
			url := rec[0]
			name := rec[1]
			year := rec[2]
			runTime := rec[3]
			film := Film{0, name, year, url, runTime, time.Now()}
			visitedMap[url] = film
		}
		log.Println(visitedMap)
	}

	// rewind fp
	fp.Seek(0, 0)

	csvWriter := csv.NewWriter(fp)

	n := 0
	for link := links.Front(); link != nil; link = link.Next() {
		url := link.Value.(string)

		// clean up URL string
		urlobj, err := url2.ParseRequestURI(url)
		if err != nil {
			log.Fatal("Could not parse url", err)
		}
		url = urlobj.Scheme + "://" + urlobj.Host + urlobj.Path // remove query part

		visited, film, newLinks := processUrl(url, &visitedMap)
		if !visited {
			log.Println("------------------", film.FilmName, "-------------------")

			// write film data in csv or database
			InsertFilm(film, db)
			// add new links to the end of links
			for _, newLink := range newLinks {
				if !alreadyVisited(newLink, &visitedMap) {
					links.PushBack(makeUrl(host, newLink))
				}
			}

			n += 1
			if n == MaxInserts {
				log.Println("Max inserts limit reached!")
				break
			}
		}
	}

	csvWriter.Flush()
	log.Println("DONE, writing json save file")

	fp1, err = os.OpenFile("last_run_links.json", os.O_RDWR | os.O_TRUNC | os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("error reopening and truncating file", err)
	}
	defer fp1.Close()

	toSaveLinks := []string{}
	for link := links.Front(); link != nil; link = link.Next() {
		toSaveLinks = append(toSaveLinks, link.Value.(string))
	}
	data = LastRunLinksData{
		toSaveLinks,
	}
	bytes, err = json.Marshal(&data)
	if err != nil {
		log.Fatal("error marshalling json", err)
	}
	fp1.Write(bytes)
}

func processUrl(url string, visited *map[string]Film) (v bool, film Film, links []string) {
	if alreadyVisited(url, visited) {
		return true, film, links
	}

	log.Println("Visiting:", url)

	doc, err := goquery.NewDocument(url)
	if err != nil {
		log.Fatal(err)
	}

	// Get film data
	doc.Find(".title_wrapper").Each(func(i int, selection *goquery.Selection) {
		nameWithYear := strings.TrimSpace(selection.Find("h1").Text())
		titleYear := strings.TrimSpace(selection.Find("#titleYear").Text())

		var runningTime string
		selection.Find(".subtext").Each(func(i int, selection *goquery.Selection) {
			runningTime = strings.TrimSpace(selection.Find("time").Text())
		})

		// Trim year off the end of title
		nameOnly := strings.TrimSpace(strings.TrimSuffix(nameWithYear, titleYear))
		film = Film{
			0,
			nameOnly,
			titleYear,
			url,
			runningTime,
			time.Now(),
		}
	})

	(*visited)[url] = film

	// find all related films
	doc.Find(".rec_item").Each(func(i int, selection *goquery.Selection) {
		node := selection.Find("a").Nodes[0]
		for _, attr := range node.Attr {
			if attr.Key == "href" {
				links = append(links, attr.Val)
			}
		}
	})

	return false, film, links
}
