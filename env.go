package main

import (
	"github.com/joho/godotenv"
	"log"
	"os"
)

func getEnvVar(varName string) string {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	var envVar = os.Getenv(varName)

	return envVar
}
