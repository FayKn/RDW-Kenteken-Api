package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	defer timeTrack(time.Now(), "CSV processing")

	log.Println("Starting CSV processing")

	file, err := os.Open("rdw-full.csv")
	if err != nil {
		log.Fatal("Error opening file", err)
	}
	defer file.Close()

	db, err := connectToDB()
	if err != nil {
		log.Fatal("Error connecting to the database ", err)
	}

	reader := csv.NewReader(file)
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 100000) // Limit to 200 concurrent goroutines

	counter := 0 // Initialize counter

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error reading a record", err)
		}

		// skip the first line
		if record[0] == "kenteken" {
			continue
		}

		counter++ // Increment counter for each line processed
		if counter%1000000 == 0 {
			log.Printf("Processed %d lines", counter)
		}

		wg.Add(1)
		semaphore <- struct{}{} // Acquire a slot in the semaphore
		go processRecord(record, &wg, semaphore, db)
	}

	wg.Wait() // Wait for all goroutines to finish
	log.Println("File processed successfully")
}

func processRecord(record []string, wg *sync.WaitGroup, semaphore chan struct{}, db *sql.DB) {
	defer wg.Done() // Ensure this signals done at the end

	vervaldatumAPK := time.Time{}
	// set default value for vervaldatum_apk to 1-1-1970
	vervaldatumAPK = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	err := error(nil)
	if record[4] != "" {
		// parse the vervaldatum_apk to a time.Time object from 20271011 to 2027-10-11
		vervaldatumAPK, err = time.Parse("20060102", record[4])
		if err != nil {
			log.Fatal("Error parsing vervaldatum_apk", err)
		}
	}
	// Prepare the SQL statement
	_, err = db.Exec("INSERT INTO voertuigen (kenteken, voertuigsoort, merk, handelsbenaming, vervaldatum_apk) VALUES (?, ?, ?, ?, ?)", record[0], record[1], record[2], record[3], vervaldatumAPK)
	if err != nil {
		log.Println(record)
		log.Fatal("Error inserting record", err)
	}

	<-semaphore // Release a slot in the semaphore
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
