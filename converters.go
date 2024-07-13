package main

import (
	"log"
	"strconv"
	"time"
)

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

func parseISO8601Date(dateString string) time.Time {
	date := time.Time{}
	err := error(nil)
	// Set default value for date to 1-1-1970
	date = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	if dateString != "" {
		date, err = time.Parse("2006-01-02T15:04:05.000", dateString)
		if err != nil {
			log.Fatal("Error parsing ISO 8601 date", err)
		}
	}
	return date
}

func stringToDecimal(s string) float32 {
	if s == "" {
		return 0.00
	}

	f, err := strconv.ParseFloat(s, 32)
	if err != nil {
		log.Println("Error converting string to float", err)
	}

	return float32(f)
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

func stringToFloatToInt(s string) int {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Fatalf("Error converting string to float: %v", err)
	}
	return int(f)
}
