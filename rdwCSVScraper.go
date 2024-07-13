package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
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
	// Read and discard the first line (header)
	if _, err := reader.Read(); err != nil {
		log.Fatal("Error reading the header line", err)
	}

	const batchSize = 2000
	var batch [][]string
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 100)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error reading a record", err)
		}

		batch = append(batch, record)
		if len(batch) >= batchSize {
			wg.Add(1)
			semaphore <- struct{}{} // Acquire a slot in the semaphore
			go func(batch [][]string) {
				defer wg.Done()
				processRecords(batch, db)
				<-semaphore // Release a slot in the semaphore
			}(batch)
			batch = nil // Start a new batch
		}
	}

	// Process any remaining records that did not make a full batch
	if len(batch) > 0 {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(batch [][]string) {
			defer wg.Done()
			processRecords(batch, db)
			<-semaphore
		}(batch)
	}

	wg.Wait() // Wait for all goroutines to finish
	log.Println("File processed successfully")
}

func processRecords(records [][]string, db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Error starting transaction", err)
	}

	stmt, err := tx.Prepare(
		"INSERT INTO voertuigen (kenteken, voertuigsoort, merk, " +
			"handelsbenaming, vervaldatum_apk, datum_tenaamstelling, " +
			"bruto_bpm, " +
			"inrichting, aantal_zitplaatsen, eerste_kleur, tweede_kleur" +
			") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
	)

	if err != nil {
		log.Fatal("Error preparing statement", err)
	}

	defer stmt.Close() // Ensure the statement is closed

	for _, record := range records {
		_, err = stmt.Exec(
			record[0], record[1], record[2], record[3], // Kenteken, voertuigsoort, merk, handelsbenaming
			parseDateRdwFormat(record[4]), parseDateRdwFormat(record[5]), // Vervaldatum_apk, datum_tenaamstelling
			stringToDecimal(record[6]),                               // Bruto_bpm
			record[7], stringToInt(record[8]), record[9], record[10], // Inrichting, aantal_zitplaatsen, eerste_kleur, tweede_kleur
		)
		if err != nil {
			log.Println("Error inserting record ", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Fatal("Error committing transaction ", err)
	}

}
func partitionCSVIntoChunks(csvFile *csv.Reader, chunkSize int) [][][]string {
	chunks := make([][][]string, 0)

	// Read and discard the first line (header)
	_, err := csvFile.Read()
	if err != nil {
		log.Fatal("Error reading the header line", err)
	}

	for {
		chunk := make([][]string, 0)
		for i := 0; i < chunkSize; i++ {
			record, err := csvFile.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("Error reading a record", err)
			}
			chunk = append(chunk, record)
		}
		if len(chunk) > 0 {
			chunks = append(chunks, chunk)
		}
		if len(chunk) < chunkSize {
			break
		}
	}
	log.Println("CSV file partitioned into chunks")
	return chunks
}

func parseDateRdwFormat(dateString string) time.Time {
	date := time.Time{}
	err := error(nil)
	// set default value for vervaldatum_apk to 1-1-1970
	date = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	if dateString != "" {
		date, err = time.Parse("20060102", dateString)
		if err != nil {
			log.Fatal("Error parsing date", err)
		}
	}
	return date
}

func stringToDecimal(s string) float64 {
	if s == "" {
		return 0
	}

	f, err := strconv.ParseFloat(s, 32)
	if err != nil {
		log.Println(s)
		log.Println("Error converting string to float", err)
	}

	return f
}

func stringToInt(s string) int {
	if s == "" {
		return 0
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		log.Println(s)
		log.Println("Error converting string to int", err)
	}

	return i
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
